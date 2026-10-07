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

// The whole application: six modules, one connection, one database.
//
//	docker compose up -d
//	cd apps/02-policy-administration && go run .
//
// Which is what it is in production too -- that is the point of a monolith.
// The difference from apps/01 is not the number of processes but where the
// boundaries are: here they are Go packages and a broker, and this file is the
// only place that imports more than one of them. Each scenario gets a freshly
// started application and freshly emptied queues, so nothing one counted leaks
// into the next, and any broken claim ends the run with a non-zero exit.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
	_ "modernc.org/sqlite"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/billing"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/claims"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/contracts"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/documents"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/policies"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/underwriting"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Println("every application ended where it should")
}

func run() error {
	dir, err := os.MkdirTemp("", "policy-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	scenarios := []struct {
		name string
		run  func(context.Context, *application) error
	}{
		{"an ordinary application becomes a policy, and the premium is taken once", theHappyPath},
		{"an application above the automatic limit is referred, and never becomes a policy", referredAboveTheLimit},
		{"a claim is assessed against an answer from policies, not against a local copy", claimsAskRatherThanRead},
		{"a large document travels as a claim check, not as a message", documentsTravelByReference},
		{"three copies of one event charge once; a genuinely different event still charges", billingIsIdempotent},
		{"a lookup nobody answers is neither a yes nor a no", aLookupNobodyAnswers},
	}
	for i, scenario := range scenarios {
		log.Printf("--- %s", scenario.name)
		if err := runScenario(filepath.Join(dir, fmt.Sprint(i)), scenario.run); err != nil {
			return fmt.Errorf("%s: %w", scenario.name, err)
		}
	}
	return nil
}

func runScenario(dir string, scenario func(context.Context, *application) error) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	app, err := start(ctx, dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, app.stop()) }()
	return scenario(ctx, app)
}

func theHappyPath(ctx context.Context, a *application) error {
	if _, err := a.policies.Submit(ctx, "A. Applicant", "TERM-LIFE", 100_000, 40); err != nil {
		return err
	}
	// Submitted -> underwritten -> issued -> charged, with no module calling
	// another.
	if err := waitFor(ctx, func() bool { return len(a.billing.Charges()) == 1 }); err != nil {
		return err
	}
	if err := expect("accepted", a.underwriting.Accepted(), 1); err != nil {
		return err
	}
	if err := expect("issued", a.policies.Issued(), 1); err != nil {
		return err
	}
	if err := expect("charges", len(a.billing.Charges()), 1); err != nil {
		return err
	}

	// 100 base + 20 age loading, from the rating table in the pricing stage.
	policyID := a.billing.Charges()[0]
	premium, found, err := a.policies.PremiumOf(ctx, policyID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("policy %s was charged for and does not exist", policyID)
	}
	if err := expect("premium", premium, 120); err != nil {
		return err
	}
	// Submitted, accepted, issued, charged: every event once, and only once.
	return expectAudited(ctx, a, 4)
}

func referredAboveTheLimit(ctx context.Context, a *application) error {
	if _, err := a.policies.Submit(ctx, "B. Applicant", "TERM-LIFE", 750_000, 35); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return a.underwriting.Declined() == 1 }); err != nil {
		return err
	}
	// Nothing downstream ran. Billing charging for a referred application would
	// be the expensive version of this bug, which is why it is bound to
	// PolicyIssued and not to the application.
	if err := expect("issued", a.policies.Issued(), 0); err != nil {
		return err
	}
	if err := expect("charges", len(a.billing.Charges()), 0); err != nil {
		return err
	}
	// Submitted, declined.
	return expectAudited(ctx, a, 2)
}

func claimsAskRatherThanRead(ctx context.Context, a *application) error {
	policyID, err := aPolicy(ctx, a, "C. Applicant", 50_000, 30)
	if err != nil {
		return err
	}
	if _, err := a.claims.Submit(ctx, policyID, 5_000, "windscreen"); err != nil {
		return err
	}
	// A policy nobody issued. The lookup is what makes this answerable at all.
	if _, err := a.claims.Submit(ctx, "POL-does-not-exist", 5_000, "windscreen"); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return a.claims.Settled() == 1 && a.claims.Rejected() == 1 }); err != nil {
		return fmt.Errorf("settled %d and rejected %d: %w", a.claims.Settled(), a.claims.Rejected(), err)
	}
	// The four of the policy, then one settled and one rejected. Without the
	// audit queue's wildcard those last two would have reached nothing.
	return expectAudited(ctx, a, 6)
}

func documentsTravelByReference(ctx context.Context, a *application) error {
	policyID, err := aPolicy(ctx, a, "D. Applicant", 60_000, 45)
	if err != nil {
		return err
	}

	// Four megabytes, which is a small scan and a large message.
	scan := make([]byte, 4*1024*1024)
	key, err := a.documents.Store(ctx, policyID, "medical-report", scan)
	if err != nil {
		return err
	}
	content, found := a.documents.Fetch(key)
	if !found {
		return fmt.Errorf("the claim check %s cannot be redeemed", key)
	}
	if err := expect("bytes fetched", len(content), len(scan)); err != nil {
		return err
	}
	// What crossed the broker is the key and the size.
	if !strings.Contains(key, policyID) || !strings.Contains(key, "medical-report") {
		return fmt.Errorf("the key %q names neither the policy nor the kind", key)
	}
	return expectAudited(ctx, a, 5)
}

func billingIsIdempotent(ctx context.Context, a *application) error {
	policyID, err := aPolicy(ctx, a, "E. Applicant", 80_000, 50)
	if err != nil {
		return err
	}
	premium, _, err := a.policies.PremiumOf(ctx, policyID)
	if err != nil {
		return err
	}

	// Three copies carrying one message id: what a redelivery looks like from
	// the consumer's side, and what the idempotency store exists to absorb.
	messageID := "redelivery-" + acemq.NewID()
	issued := contracts.Event[contracts.PolicyIssued](a.mq, contracts.PolicyIssuedKey)
	for range 3 {
		if err := issued.Send(ctx,
			contracts.PolicyIssued{PolicyID: policyID, ApplicationID: "APP-x", Applicant: "E. Applicant",
				Product: "TERM-LIFE", AnnualPremium: premium},
			acemq.MessageType("PolicyIssued"), acemq.MessageID(messageID)); err != nil {
			return err
		}
	}

	// Two, not one -- and the difference is the whole point. The store
	// deduplicates by message id, so the three copies are one charge. They are
	// not the same message as the original issue, which had an id of its own,
	// so suppressing that too would mean the store had stopped telling "sent
	// twice" from "happened twice".
	if err := waitFor(ctx, func() bool { return len(a.billing.Charges()) == 2 }); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	if err := expect("charges", len(a.billing.Charges()), 2); err != nil {
		return err
	}
	// The four of the policy, the three copies, and the one charge they made.
	return expectAudited(ctx, a, 8)
}

// The other claim the Java system test leaves unchecked, and the one its
// README makes loudest: a lookup that did not answer is not a "no". Policies
// stops answering, and the claim must come back as an error naming the timeout,
// with nothing settled and nothing rejected.
func aLookupNobodyAnswers(ctx context.Context, a *application) error {
	policyID, err := aPolicy(ctx, a, "F. Applicant", 40_000, 33)
	if err != nil {
		return err
	}
	if err := a.policies.Close(); err != nil {
		return err
	}
	a.policies = nil

	started := time.Now()
	_, err = a.claims.Submit(ctx, policyID, 1_000, "nobody home")
	if !errors.Is(err, patterns.ErrRequestTimedOut) {
		return fmt.Errorf("a claim against a silent policies module returned %v; expected a timeout", err)
	}
	if took := time.Since(started); took < claims.LookupTimeout {
		return fmt.Errorf("the lookup gave up after %s, before its %s timeout", took, claims.LookupTimeout)
	}
	if err := expect("settled", a.claims.Settled(), 0); err != nil {
		return err
	}
	if err := expect("rejected", a.claims.Rejected(), 0); err != nil {
		return err
	}
	log.Printf("refused to guess: %v", err)
	// The four of the policy, and nothing for the claim.
	return expectAudited(ctx, a, 4)
}

// aPolicy puts an application through to a charged policy and returns its id.
func aPolicy(ctx context.Context, a *application, applicant string, sumAssured, age int) (string, error) {
	if _, err := a.policies.Submit(ctx, applicant, "TERM-LIFE", sumAssured, age); err != nil {
		return "", err
	}
	if err := waitFor(ctx, func() bool { return len(a.billing.Charges()) == 1 }); err != nil {
		return "", err
	}
	return a.billing.Charges()[0], nil
}

// expectAudited is the one claim the Java system test leaves unchecked: that
// the audit trail holds every event exactly once. It waits for the count, then
// waits again, because a duplicate arrives after the original and a check that
// stopped at the first match would never see one.
func expectAudited(ctx context.Context, a *application, want int64) error {
	count := func() int64 {
		n, err := a.mq.MessageCount(ctx, contracts.AuditQueue)
		if err != nil {
			return -1
		}
		return n
	}
	if err := waitFor(ctx, func() bool { return count() >= want }); err != nil {
		return fmt.Errorf("the audit trail holds %d events; expected %d: %w", count(), want, err)
	}
	time.Sleep(time.Second)
	if err := expect("audited events", count(), want); err != nil {
		return err
	}
	log.Printf("audited: %d events", want)
	return nil
}

// application is the six modules on one connection and one database. Audit
// has no code: it is a queue bound to policy.#, and that is all it needs to be.
type application struct {
	mq           *acemq.Conn
	db           *sql.DB
	policies     *policies.Module
	underwriting *underwriting.Module
	documents    *documents.Module
	billing      *billing.Module
	claims       *claims.Module
}

func start(ctx context.Context, dir string) (a *application, err error) {
	a = &application{}
	defer func() {
		if err != nil {
			err = errors.Join(err, a.stop())
		}
	}()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	// One connection for the whole application, because it is one application.
	// In apps/01 each service had its own; here sharing one is correct, and is
	// the only thing that genuinely differs at the transport level.
	if a.mq, err = acemq.Connect(ctx, brokerURL()); err != nil {
		return nil, err
	}
	if err := cleanSlate(ctx, a.mq); err != nil {
		return nil, err
	}
	if err := contracts.Topology().Apply(ctx, a.mq); err != nil {
		return nil, err
	}

	// One database, several modules -- the monolith's actual advantage. One
	// connection, because SQLite takes one writer at a time and a pool would
	// turn that into "database is locked" rather than into waiting.
	if a.db, err = sql.Open("sqlite", "file:"+filepath.Join(dir, "policy.db")+"?_pragma=busy_timeout(5000)"); err != nil {
		return nil, err
	}
	a.db.SetMaxOpenConns(1)

	if a.policies, err = policies.Start(ctx, a.mq, a.db); err != nil {
		return nil, err
	}
	if a.underwriting, err = underwriting.Start(ctx, a.mq); err != nil {
		return nil, err
	}
	a.documents = documents.New(a.mq)
	if a.billing, err = billing.Start(ctx, a.mq, a.db); err != nil {
		return nil, err
	}
	if a.claims, err = claims.Start(ctx, a.mq); err != nil {
		return nil, err
	}
	return a, nil
}

// stop closes the modules in reverse, then the connection and the database.
func (a *application) stop() error {
	var errs []error
	if a.claims != nil {
		errs = append(errs, a.claims.Close())
	}
	if a.billing != nil {
		errs = append(errs, a.billing.Close())
	}
	if a.underwriting != nil {
		errs = append(errs, a.underwriting.Close())
	}
	if a.policies != nil {
		errs = append(errs, a.policies.Close())
	}
	if a.mq != nil {
		errs = append(errs, a.mq.Close())
	}
	if a.db != nil {
		errs = append(errs, a.db.Close())
	}
	return errors.Join(errs...)
}

// cleanSlate deletes every queue this application reads or counts, so a message
// an earlier scenario or an earlier run left behind cannot be counted by this
// one. The topology and the pipeline recreate them.
func cleanSlate(ctx context.Context, mq *acemq.Conn) error {
	for _, queue := range []string{
		contracts.UnderwritingQueue, contracts.PoliciesQueue, contracts.BillingQueue,
		contracts.ClaimsQueue, contracts.AuditQueue, contracts.PolicyLookupQueue,
		"underwriting.register", "underwriting.price", "underwriting.decide",
	} {
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
			return errors.New("the application did not reach the expected state in time")
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}

// brokerURL is ACEMQ_POLICY_URL, then ACEMQ_URL, then the compose broker.
//
// A variable of its own because this app wants a virtual host of its own: it
// deletes its queues before every scenario, and those deletions should not be
// able to reach anything else.
func brokerURL() string {
	for _, name := range []string{"ACEMQ_POLICY_URL", "ACEMQ_URL"} {
		if url := os.Getenv(name); url != "" {
			return url
		}
	}
	return "amqp://guest:guest@localhost:5672/"
}
