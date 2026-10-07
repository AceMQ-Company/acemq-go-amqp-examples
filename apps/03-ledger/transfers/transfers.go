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

// Package transfers is where transfers are asked for, and where refusals are
// noticed.
//
// Deliberately thin. Everything a ledger is careful about happens in the
// writer; this exists to make the shape obvious -- a transfer is a command, sent
// to a queue, which may be refused, and a refusal is a normal outcome rather
// than an error.
//
// Events are named in the past tense and cannot be argued with; commands are
// requests and can be turned down. Systems that blur the two publish
// TransferMade before knowing whether it was, and then need a second event to
// take it back.
package transfers

import (
	"context"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/contracts"
)

// Gateway is transfers.
type Gateway struct {
	requested   *acemq.Publisher[contracts.Transfer]
	rejections  *acemq.Consumer
	mu          sync.Mutex
	refusedList []contracts.TransferRejected
}

// Start begins listening for refusals.
func Start(ctx context.Context, mq *acemq.Conn) (*Gateway, error) {
	g := &Gateway{requested: acemq.NewPublisher[contracts.Transfer](mq, contracts.Exchange,
		contracts.TransferRequestedKey, acemq.Mandatory[contracts.Transfer]())}
	var err error
	g.rejections, err = acemq.Consume(ctx, mq, contracts.Rejections,
		func(_ context.Context, m acemq.Message[contracts.TransferRejected]) acemq.Ack {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.refusedList = append(g.refusedList, m.Payload)
			return acemq.Accept()
		})
	if err != nil {
		return nil, err
	}
	return g, nil
}

// Request asks for money to move, and returns the transfer id, which correlates
// the command with both entries and any refusal.
func (g *Gateway) Request(ctx context.Context, from, to string, amountMinor int64, description string) (string, error) {
	transferID := "T-" + acemq.NewID()[:8]
	return transferID, g.requested.Send(ctx,
		contracts.Transfer{TransferID: transferID, From: from, To: to, AmountMinor: amountMinor, Description: description},
		acemq.MessageType("Transfer"), acemq.CorrelationID(transferID))
}

// Refused is the transfers the ledger refused, and why.
func (g *Gateway) Refused() []contracts.TransferRejected {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]contracts.TransferRejected(nil), g.refusedList...)
}

// Close stops listening.
func (g *Gateway) Close() error { return g.rejections.Close() }
