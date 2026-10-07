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

// Package policies owns applications and policies: the records everything else
// refers to.
//
// Two things here are worth the reading.
//
// The outbox is still necessary. This is a monolith with one database, so the
// usual argument for an outbox -- two services, two datastores -- does not
// apply. It applies anyway, because the two systems that must agree are this
// database and the broker, and no transaction spans both. A monolith removes the
// distributed transaction between modules; it does not remove the one between a
// module and its broker.
//
// Claims asks this module a question rather than reading its tables. In one
// process a function call would obviously work, which is exactly why the
// discipline matters: the moment claims calls in here, the two modules are one
// module, and no package structure will separate them again.
package policies

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/contracts"
)

// Module is policies.
type Module struct {
	mq       *acemq.Conn
	db       *sql.DB
	relay    *patterns.OutboxRelay
	accepted *acemq.Consumer
	lookups  *patterns.Responder
	issued   atomic.Int64
}

// Start creates the schema, starts the relay, and starts issuing and answering.
func Start(ctx context.Context, mq *acemq.Conn, db *sql.DB) (m *Module, err error) {
	m = &Module{mq: mq, db: db}
	defer func() {
		if err != nil {
			err = errors.Join(err, m.Close())
		}
	}()

	outbox := patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect)
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS applications (id VARCHAR(64) PRIMARY KEY, applicant VARCHAR(255),
		 product VARCHAR(64), sum_assured INT, age INT)`,
		`CREATE TABLE IF NOT EXISTS policies (id VARCHAR(64) PRIMARY KEY, application_id VARCHAR(64),
		 applicant VARCHAR(255), product VARCHAR(64), premium INT)`,
		outbox.Schema(),
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return nil, fmt.Errorf("could not create the policies schema: %w", err)
		}
	}

	// The relay reads through the pool, on its own schedule, and never inside
	// anybody's transaction. Not the caller's context: it outlives start-up and
	// stops on Close.
	m.relay = patterns.NewOutboxRelay(mq, outbox,
		patterns.RelayInterval(200*time.Millisecond), patterns.RelayBatch(20))
	m.relay.Start(context.Background())

	if m.accepted, err = acemq.Consume(ctx, mq, contracts.PoliciesQueue, m.issue); err != nil {
		return nil, err
	}
	if m.lookups, err = patterns.Serve(ctx, mq, contracts.PolicyLookupQueue, m.statusOf); err != nil {
		return nil, err
	}
	return m, nil
}

// Submit takes an application, and announces it, in one transaction.
func (m *Module) Submit(ctx context.Context, applicant, product string, sumAssured, age int) (string, error) {
	applicationID := "APP-" + shortID()
	err := m.inOneTransaction(ctx,
		func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO applications (id, applicant, product, sum_assured, age) VALUES (?, ?, ?, ?, ?)`,
				applicationID, applicant, product, sumAssured, age)
			return err
		},
		contracts.ApplicationSubmittedKey,
		contracts.ApplicationSubmitted{ApplicationID: applicationID, Applicant: applicant,
			Product: product, SumAssured: sumAssured, AgeOfApplicant: age},
		acemq.MessageType("ApplicationSubmitted"), acemq.CorrelationID(applicationID))
	if err != nil {
		return "", fmt.Errorf("could not submit the application for %s: %w", applicant, err)
	}
	return applicationID, nil
}

// issue is underwriting having said yes, so the policy exists.
func (m *Module) issue(ctx context.Context, msg acemq.Message[contracts.ApplicationAccepted]) acemq.Ack {
	accepted := msg.Payload
	policyID := "POL-" + shortID()
	err := m.inOneTransaction(ctx,
		func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO policies (id, application_id, applicant, product, premium) VALUES (?, ?, ?, ?, ?)`,
				policyID, accepted.ApplicationID, accepted.Applicant, accepted.Product, accepted.AnnualPremium)
			return err
		},
		contracts.PolicyIssuedKey,
		contracts.PolicyIssued{PolicyID: policyID, ApplicationID: accepted.ApplicationID,
			Applicant: accepted.Applicant, Product: accepted.Product, AnnualPremium: accepted.AnnualPremium},
		acemq.MessageType("PolicyIssued"), acemq.CorrelationID(accepted.ApplicationID))
	if err != nil {
		return acemq.Retry(fmt.Errorf("could not issue a policy for %s: %w", accepted.ApplicationID, err))
	}
	m.issued.Add(1)
	return acemq.Accept()
}

// inOneTransaction is the outbox: the row and the event it announces are
// written by one commit, or neither is.
func (m *Module) inOneTransaction(
	ctx context.Context, write func(*sql.Tx) error, routingKey string, event any, opts ...acemq.EnvelopeOption,
) (err error) {
	// Encoded now, and the relay later publishes exactly these bytes.
	record, err := patterns.Record(m.mq, contracts.Exchange, routingKey, event, opts...)
	if err != nil {
		return err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	if err = write(tx); err != nil {
		return err
	}
	// The outbox writes through the same transaction. There is no second commit
	// that can fail on its own.
	if err = patterns.NewSQLOutboxStore(tx, patterns.SQLiteDialect).Add(ctx, record); err != nil {
		return err
	}
	return tx.Commit()
}

// statusOf answers the question claims asks, without claims touching this
// module's tables.
func (m *Module) statusOf(ctx context.Context, msg acemq.Message[contracts.PolicyQuery]) (contracts.PolicyStatus, error) {
	return m.lookup(ctx, msg.Payload.PolicyID)
}

func (m *Module) lookup(ctx context.Context, policyID string) (contracts.PolicyStatus, error) {
	var premium int
	err := m.db.QueryRowContext(ctx, `SELECT premium FROM policies WHERE id = ?`, policyID).Scan(&premium)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return contracts.PolicyStatus{PolicyID: policyID}, nil
	case err != nil:
		return contracts.PolicyStatus{}, fmt.Errorf("could not look up %s: %w", policyID, err)
	}
	return contracts.PolicyStatus{PolicyID: policyID, InForce: true, AnnualPremium: premium}, nil
}

// Issued is how many policies have been issued.
func (m *Module) Issued() int64 { return m.issued.Load() }

// PremiumOf is the premium recorded for a policy, and whether it exists.
func (m *Module) PremiumOf(ctx context.Context, policyID string) (int, bool, error) {
	status, err := m.lookup(ctx, policyID)
	return status.AnnualPremium, status.InForce, err
}

// Close stops answering, then issuing, then the relay.
func (m *Module) Close() error {
	var errs []error
	if m.lookups != nil {
		errs = append(errs, m.lookups.Close())
	}
	if m.accepted != nil {
		errs = append(errs, m.accepted.Close())
	}
	if m.relay != nil {
		errs = append(errs, m.relay.Close())
	}
	return errors.Join(errs...)
}

func shortID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
