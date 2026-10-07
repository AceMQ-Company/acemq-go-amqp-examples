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

package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/contracts"
)

const (
	// quietPeriod is how long without an entry counts as caught up.
	//
	// A crude way to find the end of a stream, and honest about it. The precise
	// way is to read the offset of the last entry before starting and stop
	// there; that is what a production rebuild does.
	quietPeriod = 400 * time.Millisecond

	rebuildLimit = 30 * time.Second
)

// balances is computed by reading the journal from the beginning.
//
// It holds no state that anybody wrote down. Delete it, restart the process, and
// it comes back identical, because it is a function of the log and nothing
// else. FromFirst is the whole trick: a queue cannot do this -- reading it
// consumes it -- and a stream is not emptied by being read.
//
// # Read to the end, then stop
//
// The reader is closed once it has caught up, and the writer maintains the
// balances itself from then on. That is a correctness requirement, not an
// optimisation. The first Java version kept following the stream and applied
// each entry as it was written, so every entry was counted twice -- once
// locally, once when it came back round. Keeping only the stream has the
// opposite problem: a transfer decided against a balance that does not yet
// include the transfer before it. For a single writer, maintaining its own
// total after the rebuild is both correct and immediate.
//
// A rebuild is O(history). At a billion entries the answer is a snapshot -- the
// balance at offset N, plus everything after N -- deliberately not here.
type balances struct {
	mu       sync.Mutex
	accounts map[string]int64
	replayed int64
}

func rebuiltFrom(ctx context.Context, mq *acemq.Conn) (*balances, error) {
	b := &balances{accounts: map[string]int64{}}
	var lastSeen atomic.Int64
	lastSeen.Store(time.Now().UnixNano())

	reader, err := patterns.ReadStream(ctx, mq, contracts.Journal,
		func(_ context.Context, m acemq.Message[contracts.EntryPosted]) acemq.Ack {
			b.apply(m.Payload)
			b.mu.Lock()
			b.replayed++
			b.mu.Unlock()
			lastSeen.Store(time.Now().UnixNano())
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: patterns.FromFirst()})
	if err != nil {
		return nil, fmt.Errorf("could not rebuild balances from %s: %w", contracts.Journal, err)
	}

	deadline := time.Now().Add(rebuildLimit)
	for time.Since(time.Unix(0, lastSeen.Load())) < quietPeriod {
		if time.Now().After(deadline) {
			return nil, errors.Join(fmt.Errorf("the journal did not stop producing entries within %s;"+
				" a rebuild cannot finish while somebody is still writing", rebuildLimit), reader.Close())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Stopped, not merely ignored. A reader left running is the double count.
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return b, nil
}

// apply is the writer posting as it appends, which is safe precisely because
// there is one writer.
func (b *balances) apply(entry contracts.EntryPosted) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.accounts[entry.Account] += entry.AmountMinor
}

func (b *balances) of(account string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.accounts[account]
}

// snapshot is every balance, for comparing two rebuilds.
func (b *balances) snapshot() map[string]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	copied := make(map[string]int64, len(b.accounts))
	for account, balance := range b.accounts {
		copied[account] = balance
	}
	return copied
}
