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

// Package contracts is the events a ledger is made of.
//
// In apps/01 and apps/02 the events describe what happened to the system of
// record. Here they are the system of record. There is no balances table that
// events update; a balance is what you get by adding up entries, and it can be
// deleted and recomputed without losing anything, because nothing was ever
// stored that the log does not contain.
//
// That has one consequence worth stating before the code: an entry is never
// changed and never deleted. Money moved wrongly is corrected by posting the
// opposite entry, exactly as a paper ledger does, and both entries stay.
//
// Every name and every JSON field is the Java example's, character for
// character.
package contracts

import acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

const (
	// Journal is the stream every entry is appended to.
	//
	// A stream rather than a queue, and the difference is the point. A queue is
	// emptied by being read; a stream is not. Ten readers can each read all of
	// history at their own pace, and a projection written next year can start
	// from offset zero.
	Journal = "ledger.journal"

	// Commands is where transfer commands arrive. An ordinary queue: a command
	// is handled once.
	Commands = "ledger.commands"

	// Rejections is where refusals are announced, for whoever wants to be told
	// rather than to read.
	Rejections = "ledger.rejections"

	// Exchange is where the ledger announces what it decided, for anything that
	// is not a projection.
	Exchange = "ledger"

	EntryPostedKey       = "ledger.entry.posted"
	TransferRejectedKey  = "ledger.transfer.rejected"
	TransferRequestedKey = "ledger.transfer.requested"
)

// EntryPosted is one side of one movement of money.
//
// Signed rather than a debit/credit flag: a sum over a column is then simply a
// sum. Whole minor units -- pennies, cents -- because a ledger in float64 is a
// ledger that disagrees with itself after enough additions.
type EntryPosted struct {
	EntryID     string `json:"entryId"`     // unique, and the idempotency key
	TransferID  string `json:"transferId"`  // the movement this is one half of
	Account     string `json:"account"`     // whose balance changes
	AmountMinor int64  `json:"amountMinor"` // positive credits, negative debits
	Description string `json:"description"` // what a statement will show
}

// TransferRejected is a transfer that was refused, with the reason kept beside
// the ones that were not.
type TransferRejected struct {
	TransferID  string `json:"transferId"`
	From        string `json:"from"`
	To          string `json:"to"`
	AmountMinor int64  `json:"amountMinor"`
	Reason      string `json:"reason"`
}

// Transfer is a request to move money between two accounts. Not an event: it
// may be refused.
type Transfer struct {
	TransferID  string `json:"transferId"`
	From        string `json:"from"`
	To          string `json:"to"`
	AmountMinor int64  `json:"amountMinor"`
	Description string `json:"description"`
}

// Topology is the ledger's queues. The journal is not here: it is a stream, and
// the ledger declares it with its retention.
//
// Classic queues, as in Java: a durable queue this library declares is quorum
// unless told otherwise, and a queue cannot be redeclared as another type.
func Topology() *acemq.Topology {
	classic := acemq.OfType(acemq.QueueClassic)
	return acemq.NewTopology().
		Exchange(Exchange, "topic").

		// Commands: an ordinary queue, because a transfer must be applied once.
		Queue(Commands, classic).
		Binding(Commands, Exchange, TransferRequestedKey).

		// Rejections are announced so somebody can act on them. The ledger keeps
		// its own count; this is for whoever wants to be told.
		Queue(Rejections, classic).
		Binding(Rejections, Exchange, TransferRejectedKey)
}
