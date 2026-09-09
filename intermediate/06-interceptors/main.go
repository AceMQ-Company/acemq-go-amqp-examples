// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// One rule about tenancy, applied to every publish and every delivery on a
// connection.
//
//	docker compose up -d
//	go run ./intermediate/06-interceptors
//
// The rule is the sort of thing that is true of a service rather than of a
// message: every message this process sends belongs to the tenant it is working
// for, no message goes out without one, no card number reaches the broker, and a
// message belonging to a tenant this process does not serve is refused before
// any handler sees it.
//
// Written into each publisher and each handler, a rule like that is correct
// until somebody adds the eleventh publisher. Registered on the connection, it
// covers the publishers inside patterns.Requester and the outbox relay too —
// code this example never calls and never could have edited.
//
// Interceptors intercept rather than observe, which is the part worth seeing:
// returning an error from a publish interceptor stops the publish and the caller
// gets the error, and returning one from a consume interceptor dead-letters the
// message with the reason instead of running the handler.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-interceptors.payments"

// tenantHeader is outside the x-acemq- namespace on purpose. That prefix is the
// engine's: a header carrying it is materialised onto the envelope if this
// version knows it and dropped either way, so an application header put there
// would go out on the wire and vanish before the handler.
const tenantHeader = "x-tenant"

// served are the tenants this process is deployed for. Everything else on the
// queue belongs to somebody else's deployment.
var served = map[string]bool{"acme": true}

type Payment struct {
	PaymentID string `json:"paymentId"`
	Card      string `json:"card"`
	Cents     int64  `json:"cents"`
}

// tenantKey carries the current tenant on the context, as a request-scoped value
// put there by whatever accepted the work — an HTTP middleware, a consumer.
type tenantKey struct{}

func withTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

func tenantFrom(ctx context.Context) string {
	tenant, _ := ctx.Value(tenantKey{}).(string)
	return tenant
}

// errNoTenant is what stops a publish. A named error rather than a string, so
// the caller can tell this refusal from a broker that was not there.
var errNoTenant = errors.New("this message has no tenant, and every message must belong to one")

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// What each interceptor saw, so the checks at the end can prove they ran and
	// prove they ran in this order. Behind a mutex because a consume interceptor
	// runs on the consumer's goroutine and a publish interceptor on whichever
	// goroutine published — an interceptor is shared state by definition.
	seen := &record{}

	mq, err := acemq.Connect(ctx, brokerURL(),
		acemq.WithOrigin("examples@06-interceptors"),

		// Publish interceptors run in the order they are added.
		//
		// First: the tenant, from the context. A publish with none is stopped
		// here rather than sent and sorted out later — which is what
		// intercepting buys over observing.
		acemq.WithPublishInterceptor(func(ctx context.Context, c *acemq.PublishContext) error {
			tenant := tenantFrom(ctx)
			if tenant == "" {
				return fmt.Errorf("acemq-example: %w", errNoTenant)
			}
			c.SetHeader(tenantHeader, tenant)
			seen.add(&seen.stamped, tenant)
			return nil
		}),

		// Second: the card number never leaves this process.
		//
		// Interceptors run before the payload is encoded, so this changes the
		// value rather than the bytes — there is no JSON to rewrite yet, and a
		// codec swapped for YAML tomorrow does not break it. The envelope it
		// reads was written by the interceptor above, which is the order stated
		// as a check rather than as a comment.
		acemq.WithPublishInterceptor(func(_ context.Context, c *acemq.PublishContext) error {
			if c.Envelope.Headers[tenantHeader] == nil {
				return errors.New("acemq-example: the tenant interceptor did not run first")
			}
			payment, ok := c.Payload.(Payment)
			if !ok {
				// Something else on this connection publishing something else.
				// An interceptor is asked about every message, including the
				// ones it has no opinion about.
				return nil
			}
			seen.add(&seen.redacted, payment.PaymentID)
			payment.Card = "•••• " + last4(payment.Card)
			c.Payload = payment
			return nil
		}),

		// And on the way in: a message for a tenant this process does not serve
		// never reaches a handler. Returning an error dead-letters it with the
		// reason, because an interceptor that says no will say no again to the
		// same message and retrying it would only spend the retry ladder.
		acemq.WithConsumeInterceptor(func(_ context.Context, c *acemq.ConsumeContext) error {
			tenant := headerString(c.Envelope.Headers, tenantHeader)
			if !served[tenant] {
				seen.add(&seen.refused, tenant)
				return fmt.Errorf(
					"acemq-example: this message belongs to tenant %q, and this process serves %v",
					tenant, keys(served))
			}
			return nil
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	// The dead-letter queue is where a refused message lands, so it has to exist
	// for the refusal to be visible rather than merely to have happened.
	if err := acemq.NewTopology().
		Queue(queue).
		DeadLetters(queue).
		Apply(ctx, mq); err != nil {
		log.Fatal(err)
	}

	handled := make(chan acemq.Message[Payment], 4)
	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[Payment]) acemq.Ack {
			handled <- m
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	payments := acemq.NewPublisher[Payment](mq, "", queue)

	// ---- a tenant this process serves --------------------------------------

	ours := withTenant(ctx, "acme")
	if err := payments.Send(ours,
		Payment{PaymentID: "p-1", Card: "4242424242424242", Cents: 4250}); err != nil {
		log.Fatal(err)
	}
	log.Println("published p-1 for acme")

	// ---- a tenant it does not ----------------------------------------------

	theirs := withTenant(ctx, "globex")
	if err := payments.Send(theirs,
		Payment{PaymentID: "p-2", Card: "4000000000000002", Cents: 900}); err != nil {
		log.Fatal(err)
	}
	log.Println("published p-2 for globex")

	// ---- and one with no tenant at all -------------------------------------

	// ctx rather than one carrying a tenant: a code path that forgot. The
	// message is not published, and the caller is told so.
	noTenantErr := payments.Send(ctx,
		Payment{PaymentID: "p-3", Card: "4111111111111111", Cents: 100})
	log.Printf("publishing p-3 without a tenant: %v", noTenantErr)

	// ---- what arrived ------------------------------------------------------

	var accepted acemq.Message[Payment]
	select {
	case accepted = <-handled:
	case <-ctx.Done():
		log.Fatal("p-1 never reached the handler")
	}

	log.Println()
	log.Printf("handler saw    %s  tenant=%v  card=%q",
		accepted.Payload.PaymentID, accepted.Envelope.Headers[tenantHeader], accepted.Payload.Card)
	log.Printf("on the wire    %s", accepted.Body)

	// The refused message is republished to the dead-letter queue with the
	// reason on it, which takes a moment: the consumer does that after the
	// interceptor returns.
	dead, reason := waitForDeadLetter(ctx, mq)
	log.Printf("dead-lettered  %s  because: %s", dead.PaymentID, reason)

	stamped, redacted, refused := seen.read()

	log.Println()
	log.Printf("stamped %v, redacted %v, refused %v", stamped, redacted, refused)

	// ---- what all of that has to say, checked ------------------------------

	// The publish that had no tenant was stopped, and stopped by this rule
	// rather than by a broker that happened to be missing.
	if noTenantErr == nil {
		log.Fatal("a message with no tenant was published")
	}
	if !errors.Is(noTenantErr, errNoTenant) {
		log.Fatalf("p-3 failed for the wrong reason: %v", noTenantErr)
	}

	// Both publish interceptors ran on both messages that went out, and neither
	// ran on the one that did not: the first refused it, and the second was
	// never reached — a chain stops at the first error.
	if strings.Join(stamped, ",") != "acme,globex" {
		log.Fatalf("the tenant interceptor stamped %v", stamped)
	}
	if strings.Join(redacted, ",") != "p-1,p-2" {
		log.Fatalf("the redacting interceptor saw %v", redacted)
	}

	// The handler got the message for the tenant it serves, with the header the
	// interceptor put there.
	if accepted.Payload.PaymentID != "p-1" {
		log.Fatalf("the handler saw %s", accepted.Payload.PaymentID)
	}
	if got := headerString(accepted.Envelope.Headers, tenantHeader); got != "acme" {
		log.Fatalf("the tenant header arrived as %q", got)
	}

	// And the card number never reached the broker. This is checked against the
	// body as it arrived rather than against the decoded payload, because the
	// body is what the broker wrote to its disk and what its management
	// interface would show.
	if strings.Contains(string(accepted.Body), "4242424242424242") {
		log.Fatalf("the card number is on the wire: %s", accepted.Body)
	}
	if accepted.Payload.Card != "•••• 4242" {
		log.Fatalf("the card was not redacted: %q", accepted.Payload.Card)
	}

	// The other message never reached the handler at all.
	select {
	case m := <-handled:
		log.Fatalf("%s reached the handler, and it is not ours", m.Payload.PaymentID)
	case <-time.After(2 * time.Second):
	}

	// It is on the dead-letter queue instead, with a reason somebody draining
	// that queue can act on: which tenant, and what this process serves.
	if dead.PaymentID != "p-2" {
		log.Fatalf("the dead-lettered message is %s", dead.PaymentID)
	}
	if !strings.Contains(reason, "interceptor") || !strings.Contains(reason, "globex") {
		log.Fatalf("the dead-letter reason does not say what happened: %q", reason)
	}
	if len(refused) != 1 || refused[0] != "globex" {
		log.Fatalf("the consume interceptor refused %v", refused)
	}

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
	}
}

// waitForDeadLetter pulls the refused message off the dead-letter queue.
//
// Pull rather than Consume because there is one message and this is a tool
// reading it, which is what Pull is for. Polling is the wrong shape for ordinary
// work and the right shape here.
func waitForDeadLetter(ctx context.Context, mq *acemq.Conn) (Payment, string) {
	deadline := time.After(30 * time.Second)
	for {
		pulled, payment, found, err := acemq.PullInto[Payment](
			ctx, mq, acemq.DeadLetterQueue(queue))
		if err != nil {
			log.Fatal(err)
		}
		if found {
			if err := pulled.Ack(); err != nil {
				log.Fatal(err)
			}
			return payment, pulled.Envelope.Error
		}
		select {
		case <-deadline:
			log.Fatal("nothing reached the dead-letter queue")
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// record is what the three interceptors wrote down, behind a mutex.
type record struct {
	mu       sync.Mutex
	stamped  []string
	redacted []string
	refused  []string
}

func (r *record) add(to *[]string, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*to = append(*to, value)
}

func (r *record) read() (stamped, redacted, refused []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stamped, r.redacted, r.refused
}

func last4(card string) string {
	if len(card) < 4 {
		return card
	}
	return card[len(card)-4:]
}

// headerString reads a header that may have arrived as a string or as bytes,
// which is what an AMQP long string can be.
func headerString(headers map[string]any, name string) string {
	switch value := headers[name].(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return ""
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// brokerURL is the compose broker unless ACEMQ_URL names another.
//
// Repeated in every example rather than shared: each directory is meant to be
// readable on its own, and a helper somewhere else is one more thing to find.
func brokerURL() string {
	if url := os.Getenv("ACEMQ_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}
