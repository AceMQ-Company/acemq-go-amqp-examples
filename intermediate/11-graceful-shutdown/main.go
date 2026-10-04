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
// Consumer.Close is the arrangement. It stops new deliveries and waits for the
// handlers to finish what they hold, so the work in hand is acknowledged rather
// than redone. What it does not have is a deadline: it waits as long as the
// handlers take. The bound comes from outside — a timer, and a context the
// handlers watch — and this example runs the same shutdown three ways to show
// what each part of that buys.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-shutdown-orders"

// Long enough that a message is genuinely still being handled at shutdown.
const work = 300 * time.Millisecond

type Order struct {
	ID string `json:"id"`
}

// outcome is what one shutdown left behind.
type outcome struct {
	drained  bool
	took     time.Duration
	handled  int64
	gaveBack int64
	left     int64
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	// Enough time for the message in hand. This is the shape a service wants:
	// SIGTERM, close within the grace period, exit.
	enough := shutdown(ctx, mq, "enough time", 1, 10*time.Second)

	// The same, with a prefetch of five. Close finishes everything the broker
	// had already handed this process, not only the message being worked on —
	// so the grace period has to cover prefetch × handler time, not one handler.
	prefetched := shutdown(ctx, mq, "prefetch 5", 5, 10*time.Second)

	// And a grace period shorter than that. When the timer fires the handlers'
	// context is cancelled, they give their messages back, and Close returns.
	// Nothing is lost: what was not finished is on the queue for whoever starts
	// next.
	short := shutdown(ctx, mq, "not enough", 5, 500*time.Millisecond)

	// ---- what all of that has to say, checked ------------------------------

	if !enough.drained || enough.handled != 1 || enough.left != 9 {
		log.Fatalf("with enough time, the one message in hand should be finished and nine left: %+v", enough)
	}
	if !prefetched.drained || prefetched.handled != 5 || prefetched.left != 5 {
		log.Fatalf("Close should finish all five the broker had handed over: %+v", prefetched)
	}
	// Five handlers' worth of work at 300ms does not fit in 500ms. Which messages
	// finished depends on the clock; that nothing went missing does not.
	if short.drained || short.handled >= 5 || short.gaveBack == 0 {
		log.Fatalf("a grace period shorter than the work should end with messages given back: %+v", short)
	}
	if short.handled+short.left != 10 {
		log.Fatalf("handled %d and %d left on the queue: a message went missing", short.handled, short.left)
	}

	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
	}
}

// shutdown fills a queue, starts a consumer, and stops it half way through the
// first message, giving it grace to finish.
func shutdown(ctx context.Context, mq *acemq.Conn, label string, prefetch int, grace time.Duration) outcome {
	if err := mq.DeleteQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
	publisher := acemq.NewPublisher[Order](mq, "", queue)
	for i := 1; i <= 10; i++ {
		if err := publisher.Send(ctx, Order{ID: fmt.Sprintf("o-%d", i)}); err != nil {
			log.Fatal(err)
		}
	}

	// The handlers' context, and the one thing that bounds them. It is not the
	// context SIGTERM cancels: cancelling the handlers the moment the signal
	// arrives is the abrupt stop this is trying to avoid. It is cancelled when
	// the grace period runs out, and not before.
	handlerCtx, giveUp := context.WithCancel(ctx)
	defer giveUp()

	var handled, gaveBack atomic.Int64
	started := make(chan struct{}, 10)

	consumer, err := acemq.Consume(handlerCtx, mq, queue,
		func(ctx context.Context, m acemq.Message[Order]) acemq.Ack {
			started <- struct{}{}
			select {
			case <-time.After(work):
				handled.Add(1)
				return acemq.Accept()
			case <-ctx.Done():
				// Out of time. Asking for a retry on a context that has been
				// cancelled is how a handler hands a message back: the library
				// will not republish on it, so the delivery is returned to the
				// broker unacknowledged, exactly as it arrived.
				//
				// So long as the retry policy has an attempt left, which with
				// none configured it always does. On the last attempt Retry
				// means dead-letter, that publish is refused the same way, and
				// the delivery is rejected without requeue — see the README.
				gaveBack.Add(1)
				return acemq.Retry(ctx.Err())
			}
		}, acemq.Prefetch(prefetch))
	if err != nil {
		log.Fatal(err)
	}

	// SIGTERM arrives here, with the first message half handled.
	<-started
	time.Sleep(work / 2)

	began := time.Now()
	drained, err := closeWithin(consumer, grace, giveUp)
	if err != nil {
		log.Fatal(err)
	}
	took := time.Since(began)

	// The queue settles a moment after the channel closes; wait for every
	// message to be accounted for rather than reading a count mid-flight.
	var left int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if left, err = mq.MessageCount(ctx, queue); err != nil {
			log.Fatal(err)
		}
		if handled.Load()+left == 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := outcome{drained, took, handled.Load(), gaveBack.Load(), left}
	log.Printf("%-11s grace=%-5v drained=%-5v in %4dms  handled=%d gave back=%d, %d left on the queue",
		label, grace, got.drained, got.took.Milliseconds(), got.handled, got.gaveBack, got.left)
	return got
}

// closeWithin closes a consumer, waiting at most grace for its handlers.
//
// Close has no deadline of its own. Run in the background it can be raced
// against a timer, and when the timer wins the handlers' context is cancelled so
// they stop and give back what they hold, and Close then returns promptly.
//
// That last step depends on the handlers watching their context. One that does
// not — a blocking call with no context, a loop that never checks — holds Close
// for as long as it runs, and no timer here can change that. In a real service
// the process exiting is then the bound, and the broker redelivers whatever was
// unacknowledged.
func closeWithin(consumer *acemq.Consumer, grace time.Duration, giveUp context.CancelFunc) (bool, error) {
	closed := make(chan error, 1)
	go func() { closed <- consumer.Close() }()

	select {
	case err := <-closed:
		return true, err
	case <-time.After(grace):
		giveUp()
		return false, <-closed
	}
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
