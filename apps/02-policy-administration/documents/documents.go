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

// Package documents is the claim-check pattern.
//
// A medical report scanned at 300 dpi is tens of megabytes. Putting it on a
// queue is possible and is a mistake: it fills the broker's memory, it is copied
// to every bound queue, and it makes a dead-letter queue impossible to inspect.
// What travels instead is a claim check -- the document goes to a store, and the
// message carries the key it was stored under.
//
// The store is a map, because the example must run without infrastructure. A
// real one is S3, Azure Blob Storage or a filesystem; the pattern is identical
// and two method bodies change.
//
// Retention is the part people forget. The store and the queue have different
// lifetimes: a message replayed a month later carries a key, and if the store
// expired it the replay produces a message nobody can read -- worse than a lost
// message, because it looks like a message.
package documents

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/contracts"
)

// Module is documents.
type Module struct {
	stored *acemq.Publisher[contracts.DocumentStored]

	mu    sync.Mutex
	store map[string][]byte
}

// New is documents. It consumes nothing, so there is nothing to start.
func New(mq *acemq.Conn) *Module {
	return &Module{
		stored: contracts.Event[contracts.DocumentStored](mq, contracts.DocumentStoredKey),
		store:  map[string][]byte{},
	}
}

// Store keeps a document and announces that it exists. The event is a few
// hundred bytes whatever the document weighs.
func (m *Module) Store(ctx context.Context, policyID, kind string, content []byte) (string, error) {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	key := fmt.Sprintf("doc/%s/%s/%s", policyID, kind, hex.EncodeToString(b))

	m.mu.Lock()
	m.store[key] = content
	m.mu.Unlock()

	err := m.stored.Send(ctx,
		contracts.DocumentStored{PolicyID: policyID, DocumentKey: key, Kind: kind, Bytes: len(content)},
		acemq.MessageType("DocumentStored"), acemq.CorrelationID(policyID))
	return key, err
}

// Fetch redeems a claim check, when the store still has it.
func (m *Module) Fetch(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	content, ok := m.store[key]
	return content, ok
}

// Held is how many documents are held.
func (m *Module) Held() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.store)
}
