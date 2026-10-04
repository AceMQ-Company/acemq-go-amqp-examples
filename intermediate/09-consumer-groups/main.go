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

// Four slow invoices, handled four times faster by four consumers than by one.
//
//	docker compose up -d
//	go run ./intermediate/09-consumer-groups
//
// acemq.Concurrency(4) and a group of four look like the same thing and are not.
// One consumer is one channel with one prefetch, however many handlers run
// behind it, so with a prefetch of one its four handlers still take their
// messages one at a time. Four consumers are four channels with four prefetches,
// and the broker round-robins between them.
//
// The example runs the same four messages both ways and prints how many were in
// hand at once and how long each took.
package main

import (
	"context"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-group-invoices"

var invoices = []string{"INV-1", "INV-2", "INV-3", "INV-4"}

// Long enough that two handlers overlapping is not a coincidence, and short
// enough that the slow half of the example is still a couple of seconds.
const work = 500 * time.Millisecond

type Invoice struct {
	Number string `json:"number"`
}

// watcher counts handlers running at the same moment — the number this example
// is about. Elapsed time and messages handled follow from it.
type watcher struct {
	mu      sync.Mutex
	running int
	most    int
	handled []string
	done    chan struct{}
}

func newWatcher() *watcher { return &watcher{done: make(chan struct{})} }

func (w *watcher) handle(_ context.Context, m acemq.Message[Invoice]) acemq.Ack {
	w.mu.Lock()
	w.running++
	w.most = max(w.most, w.running)
	w.mu.Unlock()

	time.Sleep(work)

	w.mu.Lock()
	defer w.mu.Unlock()
	w.running--
	w.handled = append(w.handled, m.Payload.Number)
	if len(w.handled) == len(invoices) {
		close(w.done)
	}
	return acemq.Accept()
}

// finished waits for every invoice and says how long that took.
func (w *watcher) finished(ctx context.Context, started time.Time, what string) time.Duration {
	select {
	case <-w.done:
		return time.Since(started)
	case <-ctx.Done():
		w.mu.Lock()
		defer w.mu.Unlock()
		log.Fatalf("%s handled %d of %d invoices", what, len(w.handled), len(invoices))
		return 0
	}
}

func fill(ctx context.Context, mq *acemq.Conn) {
	publisher := acemq.NewPublisher[Invoice](mq, "", queue)
	for _, number := range invoices {
		if err := publisher.Send(ctx, Invoice{Number: number}); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}

	// ---- four consumers, each with its own channel and prefetch -----------

	grouped := newWatcher()
	fill(ctx, mq)

	started := time.Now()
	// The size is an argument, which is the point of having a group: it is the
	// number most often changed after a service is running, and the most awkward
	// to change when the consumers are four lines of start-up code.
	//
	// Options apply to every member, so each holds one message at a time.
	group, err := patterns.NewConsumerGroup(ctx, mq, queue, 4, grouped.handle,
		acemq.Prefetch(1))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("group of %d on %s", group.Size(), group.Queue())
	groupTook := grouped.finished(ctx, started, "the group")

	// One call, and every member is stopped — each one's running handlers waited
	// for, and all of them closed even if one fails, because leaving three
	// running after a shutdown the caller believes happened is worse than the
	// failure that started it.
	if err := group.Close(); err != nil {
		log.Fatal(err)
	}

	// ---- one consumer running four handlers --------------------------------

	single := newWatcher()
	fill(ctx, mq)

	started = time.Now()
	one, err := acemq.Consume(ctx, mq, queue, single.handle,
		acemq.Prefetch(1), acemq.Concurrency(4), acemq.ConsumerTag("go-group-invoices-single"))
	if err != nil {
		log.Fatal(err)
	}
	singleTook := single.finished(ctx, started, "the single consumer")
	if err := one.Close(); err != nil {
		log.Fatal(err)
	}

	log.Println()
	log.Printf("%-28s %8s %8s", "", "at once", "seconds")
	log.Printf("%-28s %8d %8.1f", "group of 4, prefetch 1", grouped.most, groupTook.Seconds())
	log.Printf("%-28s %8d %8.1f", "1 consumer, concurrency 4", single.most, singleTook.Seconds())

	// ---- what all of that has to say, checked ------------------------------

	for what, w := range map[string]*watcher{"the group": grouped, "the single consumer": single} {
		handled := slices.Sorted(slices.Values(w.handled))
		if !slices.Equal(handled, invoices) {
			log.Fatalf("%s lost or duplicated an invoice: %v", what, handled)
		}
	}

	// The claim the pattern makes: four consumers hold four messages at once
	// because there are four prefetches, and one consumer holds one however many
	// handlers are behind it.
	if grouped.most != len(invoices) {
		log.Fatalf("a group of four held %d messages at once, not four", grouped.most)
	}
	if single.most != 1 {
		log.Fatalf("one consumer with a prefetch of one held %d messages at once; "+
			"the prefetch is per consumer and this is no longer true", single.most)
	}
	if groupTook >= singleTook {
		log.Fatalf("the group took %v and the single consumer %v, which is not what four prefetches buy",
			groupTook, singleTook)
	}

	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
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
