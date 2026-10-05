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

// Package payments takes the money.
//
// This is the service where at-least-once delivery stops being a technicality.
// Every other service in this system can handle a message twice and produce the
// same outcome; this one cannot, because the second charge is real money
// belonging to a real customer.
//
// So it claims each order in a shared store before charging, and confirms
// afterwards. The store is shared rather than in-memory because there is more
// than one instance of this service in production, and an in-memory store makes
// each instance individually idempotent while the fleet is not.
package payments

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/contracts"
)

// AutomaticLimit is the amount over which a human has to look at it. Every
// payment system has one of these.
const AutomaticLimit = 1_000.00

// Service is payments.
type Service struct {
	mq       *acemq.Conn
	consumer *acemq.Consumer
	charged  *patterns.SQLIdempotencyStore
	captured *acemq.Publisher[contracts.PaymentCaptured]
	declined *acemq.Publisher[contracts.PaymentDeclined]

	captures, declines, duplicatesRefused atomic.Int64
}

// Start connects, applies the topology and starts consuming.
func Start(ctx context.Context, url string, db *sql.DB) (*Service, error) {
	mq, err := acemq.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	s := &Service{
		mq:       mq,
		captured: acemq.NewPublisher[contracts.PaymentCaptured](mq, contracts.Exchange, contracts.PaymentCapturedKey),
		declined: acemq.NewPublisher[contracts.PaymentDeclined](mq, contracts.Exchange, contracts.PaymentDeclinedKey),
	}
	if err := s.start(ctx, db); err != nil {
		mq.Close()
		return nil, err
	}
	return s, nil
}

func (s *Service) start(ctx context.Context, db *sql.DB) error {
	if err := contracts.Topology().Apply(ctx, s.mq); err != nil {
		return err
	}

	s.charged = patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect, "payments_handled")
	if _, err := db.ExecContext(ctx, s.charged.Schema()); err != nil {
		return fmt.Errorf("could not create the payments schema: %w", err)
	}
	// A generous claim timeout: it has to outlast the slowest charge, because a
	// claim that expires while the payment gateway is still thinking is a claim
	// another instance will take, and then the customer pays twice.
	s.charged.SetClaimTimeout(2 * time.Minute)

	var err error
	s.consumer, err = acemq.Consume(ctx, s.mq, contracts.PaymentsQueue, s.charge,
		acemq.Prefetch(20),
		acemq.RetryWith(acemq.ExponentialRetry(4, 200*time.Millisecond, 5*time.Second)))
	return err
}

func (s *Service) charge(ctx context.Context, m acemq.Message[contracts.OrderPlaced]) acemq.Ack {
	order := m.Payload

	// The claim is the whole safety net. A redelivery -- from a broker restart,
	// a consumer that died mid-handle, or a relay that published twice -- loses
	// here.
	claimed, err := s.charged.Claim(ctx, order.OrderID)
	if err != nil {
		// The store is what is broken, not the message.
		return acemq.Retry(err)
	}
	if !claimed {
		s.duplicatesRefused.Add(1)
		return acemq.Accept()
	}

	// The correlation id is what makes five services one story in a log
	// aggregator. Carrying it forward is not optional.
	correlation := acemq.CorrelationID(m.Envelope.CorrelationID)

	var outcome *atomic.Int64
	if order.Total > AutomaticLimit {
		err = s.declined.Send(ctx,
			contracts.PaymentDeclined{OrderID: order.OrderID, Customer: order.Customer, Reason: "over the automatic limit"},
			acemq.MessageType("PaymentDeclined"), correlation)
		outcome = &s.declines
	} else {
		err = s.captured.Send(ctx,
			contracts.PaymentCaptured{OrderID: order.OrderID, Customer: order.Customer,
				SKU: order.SKU, Quantity: order.Quantity, Amount: order.Total},
			acemq.MessageType("PaymentCaptured"), correlation)
		outcome = &s.captures
	}
	if err != nil {
		// Nothing was decided downstream, so the claim is given back and the
		// retry can run. Kept, it would make the retry look like a duplicate and
		// the order would stop here with nobody told.
		return acemq.Retry(errors.Join(err, s.charged.Release(ctx, order.OrderID)))
	}
	outcome.Add(1)

	// Confirmed only after the outcome is published. Confirming first would
	// mean a crash in between leaves the order marked as charged with nothing
	// downstream ever told -- an order that took the money and stopped.
	//
	// A failed confirm is not a reason to retry: the outcome is published, and
	// a retry would only publish it again. The claim stays, unconfirmed, and
	// refuses redeliveries until its timeout.
	_ = s.charged.Confirm(ctx, order.OrderID)
	return acemq.Accept()
}

// Captured is how many orders were charged.
func (s *Service) Captured() int64 { return s.captures.Load() }

// Declined is how many orders were refused.
func (s *Service) Declined() int64 { return s.declines.Load() }

// DuplicatesRefused is how many redeliveries were recognised and refused.
// Worth graphing.
func (s *Service) DuplicatesRefused() int64 { return s.duplicatesRefused.Load() }

// Close drains the consumer, then closes the connection.
func (s *Service) Close() error {
	return errors.Join(s.consumer.Close(), s.mq.Close())
}
