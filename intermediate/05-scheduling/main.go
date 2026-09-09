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

// Delivering a message later, with no scheduler process, no plugin and no cron.
//
//	docker compose up -d
//	go run ./intermediate/05-scheduling
//
// The obvious way to do this is a per-message time to live, and it does not
// work: RabbitMQ expires messages only from the head of a classic queue, so a
// four-hour message put in front of a one-minute message delivers the one-minute
// message in four hours, with nothing reporting it. The queue looks healthy and
// the message is not lost — it is simply late by a factor nobody predicted, and
// it fails under mixed load rather than under the uniform load of a test.
//
// What happens instead is a ladder of queues each with a uniform time to live —
// acemq.schedule.{1h,10m,1m,10s,1s} — dead-lettering into acemq.schedule.due,
// where the scheduler either delivers the message or puts it in the largest rung
// that does not overshoot what is left. Every message in a rung has the same
// delay, so the head is always the one due soonest and head-of-line expiry is
// harmless. A one-minute delay costs one hop; a one-day delay costs twenty-four.
package main

import (
	"context"
	"log"
	"os"
	"sort"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const (
	exchange = "go-reminders"
	queue    = "go-reminders.due"
)

type Reminder struct {
	ReminderID string `json:"reminderId"`
	AskedFor   string `json:"askedFor"`
}

type arrival struct {
	at          time.Duration
	reminder    Reminder
	contentType string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := acemq.NewTopology().
		Exchange(exchange, "topic").
		Queue(queue).
		Binding(queue, exchange, "reminder.#").
		Apply(ctx, mq); err != nil {
		log.Fatal(err)
	}

	started := time.Now()
	arrived := make(chan arrival, 8)
	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[Reminder]) acemq.Ack {
			arrived <- arrival{
				at:          time.Since(started),
				reminder:    m.Payload,
				contentType: m.ContentType,
			}
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// NewScheduler declares the exchange, the five rungs and the control queue,
	// and starts consuming the control queue. Every name, argument and header is
	// shared with the Java, .NET, Python and Ruby libraries, because two services
	// scheduling on one broker declare the same queues — and a rung redeclared
	// with a different argument table is answered PRECONDITION_FAILED, so the
	// second service does not start.
	//
	// It has to still be running when the messages come due. A scheduler that has
	// been closed is a ladder nobody is watching, and the messages sit in
	// acemq.schedule.due until something reads it again.
	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		log.Fatal(err)
	}
	defer scheduler.Close()

	// A moment in the past is delivered at once rather than refused: a renewal
	// date that has already gone by is a reminder that is late, not an error.
	if err := scheduler.At(ctx, time.Now().Add(-time.Minute), exchange, "reminder.due",
		Reminder{ReminderID: "R-0", AskedFor: "past"}); err != nil {
		log.Fatal(err)
	}
	if err := scheduler.In(ctx, 3*time.Second, exchange, "reminder.due",
		Reminder{ReminderID: "R-1", AskedFor: "3s"}); err != nil {
		log.Fatal(err)
	}
	if err := scheduler.In(ctx, 5*time.Second, exchange, "reminder.due",
		Reminder{ReminderID: "R-2", AskedFor: "5s"}); err != nil {
		log.Fatal(err)
	}

	seen := make([]arrival, 0, 3)
	deadline := time.After(45 * time.Second)
	for len(seen) < 3 {
		select {
		case a := <-arrived:
			seen = append(seen, a)
		case <-deadline:
			log.Fatalf("expected 3 reminders, saw %d", len(seen))
		}
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i].at < seen[j].at })

	log.Printf("scheduled %d, delivered %d, hops %d",
		scheduler.Scheduled(), scheduler.Delivered(), scheduler.Hops())
	log.Println()

	// Read the two columns together. A message is delivered as soon as less than
	// one second is left, because another hop through the smallest rung would
	// cost more than the accuracy it buys — so a three-second delay lands at
	// about two. That is the trade this design makes, stated rather than hidden:
	// delivery is accurate to about the smallest rung, and something that must
	// fire at 09:00:00.000 wants a scheduler rather than a message broker.
	log.Println("asked for   arrived at")
	for _, a := range seen {
		log.Printf("%9s   %5.1fs   %s", a.reminder.AskedFor, a.at.Seconds(), a.reminder.ReminderID)
	}

	// The payload is encoded once, when it is scheduled, and carried as bytes
	// from then on with its content type in a header that is put back on the
	// message finally delivered. A scheduler that decoded would acquire opinions
	// about message formats it has no business having, and would fail on the
	// first message written by something it does not know how to read.
	log.Println()
	log.Printf("content type on arrival: %s", seen[0].contentType)

	// ---- what the two columns have to say, checked -------------------------

	when := map[string]time.Duration{}
	for _, a := range seen {
		when[a.reminder.ReminderID] = a.at
	}

	// The already-due one should not have waited for anything.
	if when["R-0"] >= 1500*time.Millisecond {
		log.Fatalf("the past-dated reminder waited %s", when["R-0"])
	}

	// And the delayed ones should have waited. This is the check worth having: a
	// scheduler that delivered everything immediately would satisfy every other
	// assertion here. The bounds are loose by a second on purpose — see above.
	if when["R-1"] < 1500*time.Millisecond {
		log.Fatalf("R-1 arrived too early, after %s", when["R-1"])
	}
	if when["R-2"] < 3500*time.Millisecond {
		log.Fatalf("R-2 arrived too early, after %s", when["R-2"])
	}
	if when["R-2"] <= when["R-1"] {
		log.Fatalf("R-2 arrived at %s, before R-1 at %s", when["R-2"], when["R-1"])
	}
	if seen[0].contentType != "application/json" {
		log.Fatalf("the content type did not survive the ladder: %q", seen[0].contentType)
	}
	if scheduler.Delivered() != 3 {
		log.Fatalf("the scheduler delivered %d, not 3", scheduler.Delivered())
	}
	// R-0 was due already and took no hops; R-1 takes two and R-2 four, so the
	// number printed above is normally six. The check is a floor rather than an
	// equality because a busy broker can add one: what it proves is that the
	// messages went through the ladder rather than being delivered on the spot.
	if scheduler.Hops() < 4 {
		log.Fatalf("the ladder took %d hops, which is too few to have been used",
			scheduler.Hops())
	}

	if err := scheduler.Close(); err != nil {
		log.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	// The reminder queues go; the rungs stay, because they are shared with every
	// other service scheduling against this broker and are not this example's to
	// remove.
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
