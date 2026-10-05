// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gateway is where orders enter the system.
//
// The edge of a system is where the dual-write problem lives: an order has to
// be saved and announced, and doing those as two writes means a crash between
// them either loses the announcement or announces something that was never
// saved. Neither is recoverable by retrying, because the process that would
// retry is the one that died.
//
// So the gateway does one write. The event is inserted in the same transaction
// as the order, and a relay publishes it afterwards.
package gateway

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/contracts"
)

// Service is the gateway.
type Service struct {
	mq    *acemq.Conn
	db    *sql.DB
	store *patterns.SQLOutboxStore
	relay *patterns.OutboxRelay
}

// Start connects, applies the topology, creates the schema and starts the relay.
func Start(ctx context.Context, url string, db *sql.DB) (*Service, error) {
	mq, err := acemq.Connect(ctx, url)
	if err != nil {
		return nil, err
	}

	// Every service applies the whole topology. Applying it five times is safe
	// and means no service depends on another having started first.
	if err := contracts.Topology().Apply(ctx, mq); err != nil {
		mq.Close()
		return nil, err
	}

	// The relay's store reads through the pool, on its own schedule, because it
	// must not be inside anybody's request transaction.
	store := patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect)
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS orders (id VARCHAR(64) PRIMARY KEY, customer VARCHAR(128),
		 sku VARCHAR(64), quantity INT, total DECIMAL(10,2), status VARCHAR(32))`,
		store.Schema(),
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			mq.Close()
			return nil, fmt.Errorf("could not create the gateway schema: %w", err)
		}
	}

	relay := patterns.NewOutboxRelay(mq, store,
		patterns.RelayInterval(200*time.Millisecond), patterns.RelayBatch(20))
	// Not the caller's context: the relay outlives the request that started
	// the service, and stops on Close.
	relay.Start(context.Background())

	return &Service{mq: mq, db: db, store: store, relay: relay}, nil
}

// PlaceOrder takes an order and returns the id the customer is given.
//
// In a real gateway this is the body of an HTTP handler. The two writes look
// exactly like this.
func (s *Service) PlaceOrder(
	ctx context.Context, customer, sku string, quantity int, total float64,
) (orderID string, err error) {
	orderID = "ord-" + shortID()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	if _, err = tx.ExecContext(ctx,
		`INSERT INTO orders (id, customer, sku, quantity, total, status)
		 VALUES (?, ?, ?, ?, ?, 'PLACED')`,
		orderID, customer, sku, quantity, total); err != nil {
		return "", err
	}

	// The record is encoded now, inside the transaction, and the relay later
	// publishes exactly these bytes. The message id is the order id, and so is
	// the correlation id: that is what every service downstream copies forward.
	record, err := patterns.Record(s.mq, contracts.Exchange, contracts.OrderPlacedKey,
		contracts.OrderPlaced{OrderID: orderID, Customer: customer, SKU: sku, Quantity: quantity, Total: total},
		acemq.MessageType("OrderPlaced"),
		acemq.MessageID(orderID),
		acemq.CorrelationID(orderID))
	if err != nil {
		return "", err
	}

	// The outbox writes through the caller's transaction. That is the whole
	// trick: there is no second commit that can fail on its own.
	if err = patterns.NewSQLOutboxStore(tx, patterns.SQLiteDialect).Add(ctx, record); err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return orderID, nil
}

// StatusOf is what the gateway believes about an order, which is only what it
// was told.
func (s *Service) StatusOf(ctx context.Context, orderID string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM orders WHERE id = ?`, orderID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "UNKNOWN", nil
	}
	return status, err
}

// PendingInOutbox is how many records are still waiting to be published.
func (s *Service) PendingInOutbox(ctx context.Context) (int, error) {
	pending, err := s.store.Pending(ctx, 0)
	return len(pending), err
}

// Close stops the relay, then the connection.
func (s *Service) Close() error {
	return errors.Join(s.relay.Close(), s.mq.Close())
}

func shortID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
