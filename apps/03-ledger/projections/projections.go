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

// Package projections is a statement per account, built by reading the
// journal.
//
// It exists to make one claim checkable: a projection is disposable. It stores
// nothing the log does not contain, it is built by reading from offset zero, and
// throwing it away costs nothing but the time to read the log again.
//
// It also proves the log is genuinely shared. The ledger reads the same stream
// from the same offset for its own purposes, and neither reader affects the
// other -- no competing consumption, no "who got the message". That is the
// property a queue does not have, and the reason a ledger wants a stream.
//
// A fraud model, a tax report, a daily-balance chart: each is a new reader from
// offset zero, added without touching the writer and with full history from the
// day it starts.
package projections

import (
	"context"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/contracts"
)

// Statements is the projection.
type Statements struct {
	reader *acemq.Consumer

	mu         sync.Mutex
	statements map[string][]contracts.EntryPosted
}

// Start reads the journal, from the beginning of history or only from now.
func Start(ctx context.Context, mq *acemq.Conn, fromFirst bool) (*Statements, error) {
	s := &Statements{statements: map[string][]contracts.EntryPosted{}}
	offset := patterns.FromNext()
	if fromFirst {
		offset = patterns.FromFirst()
	}
	var err error
	s.reader, err = patterns.ReadStream(ctx, mq, contracts.Journal,
		func(_ context.Context, m acemq.Message[contracts.EntryPosted]) acemq.Ack {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.statements[m.Payload.Account] = append(s.statements[m.Payload.Account], m.Payload)
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: offset})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// StatementOf is the entries seen for an account, oldest first.
func (s *Statements) StatementOf(account string) []contracts.EntryPosted {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]contracts.EntryPosted(nil), s.statements[account]...)
}

// BalanceOf is the sum of an account's entries, which is what a balance is.
func (s *Statements) BalanceOf(account string) int64 {
	var total int64
	for _, entry := range s.StatementOf(account) {
		total += entry.AmountMinor
	}
	return total
}

// Entries is how many entries were read.
func (s *Statements) Entries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, entries := range s.statements {
		n += len(entries)
	}
	return n
}

// Close stops reading.
func (s *Statements) Close() error { return s.reader.Close() }
