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

// Package billing takes the first premium: the one module where handling a
// message twice is real money.
//
// The same reasoning as payments in apps/01, and worth repeating, because the
// monolith makes it easy to assume the problem went away. It did not. A retry
// still redelivers, a redeploy mid-handler still leaves a message
// unacknowledged, and both still produce a second delivery of a message that
// already took money.
//
// The idempotency store is SQL rather than in-memory even though this is one
// process, because "one process" is a fact about today. The moment this module
// is lifted out -- which is the whole point of the arrangement -- an in-memory
// store becomes two stores that each think they are the only one.
package billing

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/contracts"
)

// Module is billing.
type Module struct {
	issued  *acemq.Consumer
	charged *acemq.Publisher[contracts.PremiumCharged]

	mu      sync.Mutex
	charges []string
}

// Start creates the idempotency table and starts charging.
func Start(ctx context.Context, mq *acemq.Conn, db *sql.DB) (*Module, error) {
	seen := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	if _, err := db.ExecContext(ctx, seen.Schema()); err != nil {
		return nil, fmt.Errorf("could not create the billing schema: %w", err)
	}

	m := &Module{charged: contracts.Event[contracts.PremiumCharged](mq, contracts.PremiumChargedKey)}
	var err error
	m.issued, err = acemq.Consume(ctx, mq, contracts.BillingQueue,
		// The store is handed to the consumer rather than used by hand: the claim
		// is taken before the handler and confirmed after it accepts, which is
		// the order that closes the window a manual "mark it afterwards" leaves
		// open.
		patterns.Idempotent(seen, m.charge),
		acemq.Prefetch(10))
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Module) charge(ctx context.Context, msg acemq.Message[contracts.PolicyIssued]) acemq.Ack {
	policy := msg.Payload
	if err := m.charged.Send(ctx,
		contracts.PremiumCharged{PolicyID: policy.PolicyID, Applicant: policy.Applicant, Amount: policy.AnnualPremium},
		acemq.MessageType("PremiumCharged"), acemq.CorrelationID(policy.ApplicationID)); err != nil {
		// The claim is released by the wrapper, so the retry runs for real.
		return acemq.Retry(err)
	}
	m.mu.Lock()
	m.charges = append(m.charges, policy.PolicyID)
	m.mu.Unlock()
	return acemq.Accept()
}

// Charges is one entry per charge actually taken. A duplicate here is the bug.
func (m *Module) Charges() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.charges...)
}

// Close stops charging.
func (m *Module) Close() error { return m.issued.Close() }
