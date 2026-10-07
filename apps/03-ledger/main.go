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

// The ledger, against a real broker with a real stream.
//
//	docker compose up -d
//	go run ./apps/03-ledger
//
// The claims that matter are the projections: one rebuilt from nothing that
// agrees with the writer, and one started later that still sees all of
// history. Those are the claims event sourcing makes, and they are either true
// or the architecture is a story.
//
// The journal is emptied once, at the start of the run, and never again. Every
// scenario starts a fresh ledger, which rebuilds its balances from everything
// the scenarios before it wrote -- so each one is also a restart, and the last
// one checks that a restart changed nothing. Any broken claim ends the run with
// a non-zero exit.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/contracts"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/ledger"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/projections"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/03-ledger/transfers"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Println("the ledger and every projection of it agree")
}

// written is every entry the run has appended, across all its ledgers.
var written int64

func run() error {
	if err := cleanSlate(); err != nil {
		return err
	}
	scenarios := []struct {
		name string
		run  func(context.Context, *system) error
	}{
		{"a transfer posts two entries that sum to zero", doubleEntry},
		{"a transfer that would overdraw is refused, and the refusal is recorded", insufficientFunds},
		{"a projection built from offset zero agrees with the writer", projectionAgreesWithTheWriter},
		{"a projection added later still gets all of history", aLaterProjectionSeesEverything},
		{"two readers of the same stream do not compete for entries", readersDoNotCompete},
		{"a projection from now sees only what is new", aProjectionFromNow},
		{"a ledger rebuilt from the journal agrees with the one that wrote it", aRestartChangesNothing},
	}
	for _, scenario := range scenarios {
		log.Printf("--- %s", scenario.name)
		if err := runScenario(scenario.run); err != nil {
			return fmt.Errorf("%s: %w", scenario.name, err)
		}
	}
	return nil
}

func runScenario(scenario func(context.Context, *system) error) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	s, err := start(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.stop()) }()
	return scenario(ctx, s)
}

func doubleEntry(ctx context.Context, s *system) error {
	if err := s.ledger.Fund(ctx, "alice", 10_000); err != nil {
		return err
	}
	if _, err := s.transfers.Request(ctx, "alice", "bob", 2_500, "rent"); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.ledger.BalanceOf("bob") == 2_500 }); err != nil {
		return err
	}
	if err := expect("alice", s.ledger.BalanceOf("alice"), 7_500); err != nil {
		return err
	}
	// The invariant: money is neither created nor destroyed by a transfer.
	return expect("alice and bob together", s.ledger.BalanceOf("alice")+s.ledger.BalanceOf("bob"), 10_000)
}

func insufficientFunds(ctx context.Context, s *system) error {
	if err := s.ledger.Fund(ctx, "carol", 1_000); err != nil {
		return err
	}
	if _, err := s.transfers.Request(ctx, "carol", "dave", 5_000, "optimistic"); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return len(s.transfers.Refused()) > 0 }); err != nil {
		return err
	}
	if reason := s.transfers.Refused()[0].Reason; !strings.Contains(reason, "insufficient funds") {
		return fmt.Errorf("refused because %q; expected insufficient funds", reason)
	}
	// Nothing was posted. A ledger that half-applies a refused transfer is worse
	// than one that refuses loudly.
	if err := expect("carol", s.ledger.BalanceOf("carol"), 1_000); err != nil {
		return err
	}
	return expect("dave", s.ledger.BalanceOf("dave"), 0)
}

func projectionAgreesWithTheWriter(ctx context.Context, s *system) error {
	if err := s.ledger.Fund(ctx, "erin", 20_000); err != nil {
		return err
	}
	for _, invoice := range []struct {
		amount      int64
		description string
	}{{3_000, "invoice 1"}, {4_000, "invoice 2"}} {
		if _, err := s.transfers.Request(ctx, "erin", "frank", invoice.amount, invoice.description); err != nil {
			return err
		}
	}
	if err := waitFor(ctx, func() bool { return s.ledger.BalanceOf("frank") == 7_000 }); err != nil {
		return err
	}

	// A reader that has never seen a message, starting from the beginning of
	// time. It stores nothing the log does not contain, and must reach the same
	// answer.
	return withProjection(ctx, s, true, func(statements *projections.Statements) error {
		if err := waitFor(ctx, func() bool { return statements.BalanceOf("frank") == 7_000 }); err != nil {
			return err
		}
		for _, account := range []string{"erin", "frank"} {
			if err := expect(account+" in the projection", statements.BalanceOf(account), s.ledger.BalanceOf(account)); err != nil {
				return err
			}
		}
		// And it has the detail the balance does not: the opening balance and
		// two debits against erin.
		if err := expect("entries against erin", len(statements.StatementOf("erin")), 3); err != nil {
			return err
		}
		return expectDescriptions(statements.StatementOf("frank"), "invoice 1", "invoice 2")
	})
}

func aLaterProjectionSeesEverything(ctx context.Context, s *system) error {
	if err := s.ledger.Fund(ctx, "grace", 5_000); err != nil {
		return err
	}
	if _, err := s.transfers.Request(ctx, "grace", "heidi", 1_000, "before the projection existed"); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.ledger.BalanceOf("heidi") == 1_000 }); err != nil {
		return err
	}

	// Started now, after the entries were written. On a queue there would be
	// nothing left to read -- the ledger's own reader consumed it.
	return withProjection(ctx, s, true, func(late *projections.Statements) error {
		if err := waitFor(ctx, func() bool { return late.BalanceOf("heidi") == 1_000 }); err != nil {
			return err
		}
		return expectDescriptions(late.StatementOf("heidi"), "before the projection existed")
	})
}

func readersDoNotCompete(ctx context.Context, s *system) error {
	if err := s.ledger.Fund(ctx, "ivan", 8_000); err != nil {
		return err
	}
	if _, err := s.transfers.Request(ctx, "ivan", "judy", 2_000, "shared"); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.ledger.BalanceOf("judy") == 2_000 }); err != nil {
		return err
	}
	return withProjection(ctx, s, true, func(first *projections.Statements) error {
		return withProjection(ctx, s, true, func(second *projections.Statements) error {
			if err := waitFor(ctx, func() bool {
				return first.BalanceOf("judy") == 2_000 && second.BalanceOf("judy") == 2_000
			}); err != nil {
				return err
			}
			// Both saw the same entry. On a queue exactly one of them would have.
			if err := expect("entries for judy in the first", len(first.StatementOf("judy")), 1); err != nil {
				return err
			}
			return expect("entries for judy in the second", len(second.StatementOf("judy")), 1)
		})
	})
}

// Not in the Java system test, which never starts a projection from now: the
// option exists there and goes unchecked.
func aProjectionFromNow(ctx context.Context, s *system) error {
	return withProjection(ctx, s, false, func(recent *projections.Statements) error {
		if err := s.ledger.Fund(ctx, "kim", 100); err != nil {
			return err
		}
		if err := waitFor(ctx, func() bool { return recent.BalanceOf("kim") == 100 }); err != nil {
			return err
		}
		// Settle long enough for anything older to have arrived, had it been
		// coming.
		time.Sleep(time.Second)
		return expect("entries read from now", recent.Entries(), 1)
	})
}

// Not in the Java system test either, though its README makes the claim: the
// balances are a function of the log, so throwing them away and reading the log
// again gives them back exactly. The writer is restarted, and it must hold the
// same balances it held before, have read every entry the run ever wrote, and
// account for every penny that was paid in.
func aRestartChangesNothing(ctx context.Context, s *system) error {
	before := s.ledger.Balances()
	if err := s.ledger.Close(); err != nil {
		return err
	}
	written += s.ledger.Posted()
	s.ledger = nil

	restarted, err := ledger.Start(ctx, s.mq)
	if err != nil {
		return err
	}
	s.ledger = restarted

	if err := expect("entries replayed", restarted.Replayed(), written); err != nil {
		return err
	}
	if after := restarted.Balances(); !maps.Equal(before, after) {
		return fmt.Errorf("balances before the restart were %v and after it %v", before, after)
	}
	var total int64
	for _, balance := range before {
		total += balance
	}
	// Every opening balance the run paid in, and not a penny more.
	if err := expect("money in the ledger", total, 10_000+1_000+20_000+5_000+8_000+100); err != nil {
		return err
	}
	log.Printf("%d entries replayed, %d accounts, %d in total", written, len(before), total)
	return nil
}

func withProjection(ctx context.Context, s *system, fromFirst bool, use func(*projections.Statements) error) (err error) {
	statements, err := projections.Start(ctx, s.mq, fromFirst)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, statements.Close()) }()
	return use(statements)
}

func expectDescriptions(entries []contracts.EntryPosted, want ...string) error {
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Description)
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("the statement reads %q; expected %q", got, want)
	}
	return nil
}

// system is the ledger and the transfer gateway on one connection.
type system struct {
	mq        *acemq.Conn
	ledger    *ledger.Module
	transfers *transfers.Gateway
}

func start(ctx context.Context) (s *system, err error) {
	s = &system{}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.stop())
		}
	}()
	if s.mq, err = acemq.Connect(ctx, brokerURL()); err != nil {
		return nil, err
	}
	if err := contracts.Topology().Apply(ctx, s.mq); err != nil {
		return nil, err
	}
	if s.ledger, err = ledger.Start(ctx, s.mq); err != nil {
		return nil, err
	}
	log.Printf("the ledger rebuilt its balances from %d entries", s.ledger.Replayed())
	if s.transfers, err = transfers.Start(ctx, s.mq); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *system) stop() error {
	var errs []error
	if s.transfers != nil {
		errs = append(errs, s.transfers.Close())
	}
	if s.ledger != nil {
		written += s.ledger.Posted()
		errs = append(errs, s.ledger.Close())
	}
	if s.mq != nil {
		errs = append(errs, s.mq.Close())
	}
	return errors.Join(errs...)
}

// cleanSlate deletes the journal and both queues, once, so a ledger an earlier
// run left behind cannot be added to this one's. Deleting a ledger's journal is
// the one thing a real ledger never does; this is an example's test fixture.
func cleanSlate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		return err
	}
	defer mq.Close()
	for _, queue := range []string{contracts.Journal, contracts.Commands, contracts.Rejections} {
		if err := mq.DeleteQueue(ctx, queue); err != nil {
			return err
		}
	}
	return nil
}

func expect[N int | int64](what string, got N, want N) error {
	if got != want {
		return fmt.Errorf("%s is %d; expected %d", what, got, want)
	}
	return nil
}

func waitFor(ctx context.Context, done func() bool) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !done() {
		select {
		case <-ctx.Done():
			return errors.New("the ledger did not reach the expected state in time")
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}

// brokerURL is ACEMQ_LEDGER_URL, then ACEMQ_URL, then the compose broker.
//
// A variable of its own because this app wants a virtual host of its own: it
// deletes its journal at the start of every run, and that should not be able to
// reach anything else.
func brokerURL() string {
	for _, name := range []string{"ACEMQ_LEDGER_URL", "ACEMQ_URL"} {
		if url := os.Getenv(name); url != "" {
			return url
		}
	}
	return "amqp://guest:guest@localhost:5672/"
}
