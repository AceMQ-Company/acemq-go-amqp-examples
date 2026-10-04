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

// What happens to the message being handled when the process is told to stop.
//
//	docker compose up -d
//	go run ./intermediate/11-graceful-shutdown
//
// Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
// killed, and whatever is still inside a handler dies with it. Those messages
// were never acknowledged, so the broker redelivers them — correct, and the
// reason a deployment shows up as a spike of duplicate work when nobody arranged
// otherwise.
//
// Consumer.Close is the arrangement. It stops delivery, hands back what arrived
// but was never started, waits for the handlers already running — for at most
// DrainTimeout, twenty seconds unless you say otherwise — and only then lets go
// of the channel. This example runs the same shutdown four ways and checks what
// each one leaves behind.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-shutdown-orders"

const orders = 10

// Long enough that a message is genuinely still being handled at shutdown.
const work = 300 * time.Millisecond

// The bound the last two runs set, far below the twenty-second default so the
// example does not sit through it.
const bound = 500 * time.Millisecond

type Order struct {
	ID string `json:"id"`
}

// outcome is what one shutdown left behind.
type outcome struct {
	err      error
	took     time.Duration
	started  int64 // handlers that ran
	finished int64 // handlers that accepted their message
	left     int64 // on the queue afterwards
	dlq      int64 // in {queue}.dlq afterwards
}

func (o outcome) stranded() int {
	var timeout *acemq.DrainTimeoutError
	if errors.As(o.err, &timeout) {
		return timeout.Stranded
	}
	return 0
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	log.Printf("Close waits up to acemq.DefaultDrainTimeout = %v for running handlers", acemq.DefaultDrainTimeout)

	var started, finished atomic.Int64
	reset := func() { started.Store(0); finished.Store(0) }

	// A handler that does its work and accepts. It never looks at its context:
	// at a graceful stop the message in hand should be finished, not abandoned.
	finish := func(ctx context.Context, m acemq.Message[Order]) acemq.Ack {
		started.Add(1)
		time.Sleep(work)
		finished.Add(1)
		return acemq.Accept()
	}

	// 1. The shape a service wants: SIGTERM, the message in hand finishes, Close
	// returns nil long before the default bound.
	reset()
	enough := shutdown(ctx, mq, "enough time", finish, &started, &finished, acemq.Prefetch(1))

	// 2. The same with a prefetch of five. The broker has handed this process
	// five messages, one of them running. Close runs that one and returns the
	// other four to the queue unstarted — the drain is proportional to
	// Concurrency, not Prefetch.
	reset()
	prefetched := shutdown(ctx, mq, "prefetch 5", finish, &started, &finished, acemq.Prefetch(5))

	// 3. A handler that is stuck and does not watch its context — a blocking
	// call with no context, a loop that never checks. Close still returns at
	// the bound, reports it, and the message goes back to the broker.
	reset()
	release := make(chan struct{})
	stuck := shutdown(ctx, mq, "stuck",
		func(ctx context.Context, m acemq.Message[Order]) acemq.Ack {
			started.Add(1)
			<-release
			return acemq.Accept()
		}, &started, &finished, acemq.Prefetch(1), acemq.DrainTimeout(bound))
	close(release) // let the stranded goroutine end; its ack has nowhere to go

	// 4. A handler that gives up when the bound cancels its context, on a
	// consumer with no retries and a queue with no x-dead-letter-exchange. The
	// rejection is still filed in {queue}.dlq with its reason.
	reset()
	gaveUp := shutdown(ctx, mq, "gives up",
		func(ctx context.Context, m acemq.Message[Order]) acemq.Ack {
			started.Add(1)
			select {
			case <-time.After(10 * time.Second):
				finished.Add(1)
				return acemq.Accept()
			case <-ctx.Done():
				return acemq.Reject(fmt.Errorf("%s abandoned at shutdown: %w", m.Payload.ID, ctx.Err()))
			}
		}, &started, &finished, acemq.Prefetch(1), acemq.RetryWith(acemq.NoRetry()), acemq.DrainTimeout(bound))

	// ---- what all of that has to say, checked ------------------------------

	check := func(ok bool, claim string, o outcome) {
		if !ok {
			log.Fatalf("%s: %+v", claim, o)
		}
	}
	check(enough.err == nil && enough.finished == 1 && enough.left == orders-1 && enough.took < work,
		"with enough time the message in hand should finish, nine stay queued, Close returns nil", enough)
	check(prefetched.err == nil && prefetched.started == 1 && prefetched.finished == 1 && prefetched.left == orders-1,
		"Close should run the one handler and requeue the four prefetched messages unstarted", prefetched)
	check(errors.Is(stuck.err, acemq.ErrDrainTimeout) && stuck.stranded() == 1,
		"a stuck handler should end Close with ErrDrainTimeout and Stranded 1", stuck)
	check(stuck.took >= bound && stuck.took < bound+2*time.Second,
		"Close should return at the bound plus the half-second grace, not wait for the stuck handler", stuck)
	check(stuck.finished == 0 && stuck.left == orders && stuck.dlq == 0,
		"the stuck handler's message should be back on the queue, not lost and not dead-lettered", stuck)
	check(errors.Is(gaveUp.err, acemq.ErrDrainTimeout) && gaveUp.stranded() == 1,
		"a handler running at the bound should be reported as stranded", gaveUp)
	check(gaveUp.dlq == 1 && gaveUp.left == orders-1,
		"a rejection made after the bound cancelled the handler should still reach the dead-letter queue", gaveUp)

	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
	}
	log.Print("every claim held")
}

// shutdown fills a queue with ten orders, starts a consumer, and closes it half
// way through the first message.
func shutdown(ctx context.Context, mq *acemq.Conn, label string, handle acemq.Handler[Order],
	started, finished *atomic.Int64, opts ...acemq.ConsumeOption) outcome {
	for _, q := range []string{queue, acemq.DeadLetterQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
	}
	// A plain queue: no x-dead-letter-exchange behind it, so the only route to
	// {queue}.dlq is the one the consumer publishes itself.
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
	publisher := acemq.NewPublisher[Order](mq, "", queue)
	for i := 1; i <= orders; i++ {
		if err := publisher.Send(ctx, Order{ID: fmt.Sprintf("o-%d", i)}); err != nil {
			log.Fatal(err)
		}
	}

	// The handlers' context is not the one SIGTERM would cancel. Close cancels
	// a child of it at the drain bound, and not before.
	consumer, err := acemq.Consume(context.Background(), mq, queue, handle, opts...)
	if err != nil {
		log.Fatal(err)
	}

	// SIGTERM arrives here, with the first message half handled.
	for started.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(work / 2)

	began := time.Now()
	closeErr := consumer.Close()
	took := time.Since(began)
	if closeErr != nil && !errors.Is(closeErr, acemq.ErrDrainTimeout) {
		log.Fatal(closeErr)
	}

	// The queues settle a moment after the channel closes; wait for every order
	// to be accounted for rather than reading a count mid-flight.
	var left, dlq int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if left, err = mq.MessageCount(ctx, queue); err != nil {
			log.Fatal(err)
		}
		if dlq, err = mq.MessageCount(ctx, acemq.DeadLetterQueue(queue)); err != nil {
			log.Fatal(err)
		}
		if finished.Load()+left+dlq == orders {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := outcome{closeErr, took, started.Load(), finished.Load(), left, dlq}
	log.Printf("%-11s Close in %4dms  stranded=%d  ran=%d finished=%d  queue=%d dlq=%d",
		label, took.Milliseconds(), got.stranded(), got.started, got.finished, got.left, got.dlq)
	return got
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
