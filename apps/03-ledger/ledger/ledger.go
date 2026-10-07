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

// Package ledger is the only thing allowed to append to the journal.
//
// One writer, deliberately. A ledger's invariant -- every transfer produces two
// entries that sum to zero -- cannot be enforced by two processes appending
// independently, and a stream will happily accept an unbalanced pair from each.
// Making the writer singular is what makes the invariant checkable at all.
//
// Balances are not stored here. This module decides whether a transfer is
// allowed and appends the entries; its own view of the balances is derived from
// the log, for that one decision, and rebuilt from it on every start.
package ledger

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/contracts"
)

// Retention is how long the journal keeps entries.
//
// An hour, because this is an example. A real ledger keeps them for as long as
// the law says, and this is the setting people get wrong: a stream that expires
// entries is a ledger that quietly loses the ability to rebuild the balances it
// claims are derived. If retention is shorter than "forever", the projection is
// the system of record after all, and nobody wrote that down.
var Retention = patterns.StreamRetention{MaxAge: time.Hour, MaxBytes: 50 << 20}

// Module is the ledger.
type Module struct {
	journal  *acemq.Publisher[contracts.EntryPosted]
	rejected *acemq.Publisher[contracts.TransferRejected]
	commands *acemq.Consumer
	balances *balances

	posted, refused atomic.Int64
}

// Start declares the journal, rebuilds the balances from it, and only then
// starts taking commands.
func Start(ctx context.Context, mq *acemq.Conn) (*Module, error) {
	if err := patterns.DeclareStream(ctx, mq, contracts.Journal, Retention); err != nil {
		return nil, err
	}
	m := &Module{
		// Published straight at the stream by name. A stream is addressed as a
		// queue, so the default exchange and the stream's name is the whole of
		// it.
		journal:  acemq.NewPublisher[contracts.EntryPosted](mq, "", contracts.Journal, acemq.Mandatory[contracts.EntryPosted]()),
		rejected: acemq.NewPublisher[contracts.TransferRejected](mq, contracts.Exchange, contracts.TransferRejectedKey, acemq.Mandatory[contracts.TransferRejected]()),
	}

	var err error
	if m.balances, err = rebuiltFrom(ctx, mq); err != nil {
		return nil, err
	}
	if m.commands, err = acemq.Consume(ctx, mq, contracts.Commands, m.apply); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Module) apply(ctx context.Context, msg acemq.Message[contracts.Transfer]) acemq.Ack {
	transfer := msg.Payload
	if transfer.AmountMinor <= 0 {
		return m.reject(ctx, transfer, "a transfer must be for a positive amount")
	}
	if available := m.balances.of(transfer.From); available < transfer.AmountMinor {
		// Refused, and the refusal recorded. A ledger that silently drops what it
		// will not do cannot explain itself later.
		return m.reject(ctx, transfer, fmt.Sprintf("insufficient funds: %s holds %d", transfer.From, available))
	}

	// Two entries, one transfer, summing to zero, appended one after the other
	// by the only writer there is. A real ledger appends them as one record so a
	// crash between them is impossible; that is this example's honest
	// limitation.
	for _, entry := range []contracts.EntryPosted{
		{EntryID: entryID(), TransferID: transfer.TransferID, Account: transfer.From,
			AmountMinor: -transfer.AmountMinor, Description: transfer.Description},
		{EntryID: entryID(), TransferID: transfer.TransferID, Account: transfer.To,
			AmountMinor: transfer.AmountMinor, Description: transfer.Description},
	} {
		if err := m.post(ctx, entry); err != nil {
			// Not retried: half a transfer may already be in the journal, and a
			// retry would append the first half again. Parked for a human, which
			// is what an unbalanced journal needs.
			return acemq.Park(err)
		}
	}
	return acemq.Accept()
}

func (m *Module) post(ctx context.Context, entry contracts.EntryPosted) error {
	if err := m.journal.Send(ctx, entry,
		acemq.MessageType("EntryPosted"), acemq.MessageID(entry.EntryID),
		acemq.CorrelationID(entry.TransferID)); err != nil {
		return err
	}
	m.balances.apply(entry)
	m.posted.Add(1)
	return nil
}

func (m *Module) reject(ctx context.Context, transfer contracts.Transfer, reason string) acemq.Ack {
	if err := m.rejected.Send(ctx,
		contracts.TransferRejected{TransferID: transfer.TransferID, From: transfer.From, To: transfer.To,
			AmountMinor: transfer.AmountMinor, Reason: reason},
		acemq.MessageType("TransferRejected"), acemq.CorrelationID(transfer.TransferID)); err != nil {
		return acemq.Retry(err)
	}
	m.refused.Add(1)
	return acemq.Accept()
}

// Fund opens an account with money in it, which every ledger needs a way to do.
func (m *Module) Fund(ctx context.Context, account string, amountMinor int64) error {
	return m.post(ctx, contracts.EntryPosted{EntryID: entryID(), TransferID: "OPENING-" + account,
		Account: account, AmountMinor: amountMinor, Description: "opening balance"})
}

// BalanceOf is this module's own view, derived from the log.
func (m *Module) BalanceOf(account string) int64 { return m.balances.of(account) }

// Balances is every balance this module holds.
func (m *Module) Balances() map[string]int64 { return m.balances.snapshot() }

// Replayed is how many entries the start-up rebuild read.
func (m *Module) Replayed() int64 {
	m.balances.mu.Lock()
	defer m.balances.mu.Unlock()
	return m.balances.replayed
}

// Posted is how many entries were appended.
func (m *Module) Posted() int64 { return m.posted.Load() }

// Rejected is how many transfers were refused.
func (m *Module) Rejected() int64 { return m.refused.Load() }

// Close stops taking commands. The balances are ordinary memory.
func (m *Module) Close() error { return m.commands.Close() }

func entryID() string { return "E-" + acemq.NewID() }
