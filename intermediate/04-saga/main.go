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

// Undoing work that spans three services, and what is left when the undo fails.
//
//	docker compose up -d
//	go run ./intermediate/04-saga
//
// Reserving stock, taking a payment and booking a courier are three services
// with three databases. There is no transaction across them, so "roll it back"
// is not something a database can be asked to do — it has to be done by running
// the opposite of each step that succeeded, in reverse.
//
// Two things to watch. The compensations run backwards, newest first, because
// the later steps are the ones built on the earlier ones. And when a
// compensation itself fails the saga reports it rather than returning an error:
// something is now half-undone and needs a person, and an error thrown into a
// message handler is a poor way to tell anyone that.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"reflect"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const (
	exchange = "go-saga-events"
	ledger   = "go-saga.ledger"
)

type Order struct {
	OrderID string `json:"orderId"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := acemq.NewTopology().
		Exchange(exchange, "topic").
		Queue(ledger).
		Binding(ledger, exchange, "#").
		Apply(ctx, mq); err != nil {
		log.Fatal(err)
	}

	// Everything the saga publishes, in the order the broker delivered it. A
	// saga has no wire contract of its own — it publishes whatever its steps
	// publish — so this queue is how the example shows what really happened
	// rather than what the result claims happened.
	events := make(chan string, 16)
	recorder, err := acemq.Consume(ctx, mq, ledger,
		func(_ context.Context, m acemq.Message[Order]) acemq.Ack {
			events <- m.RoutingKey
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer recorder.Close()

	// A step is work plus its opposite. Both are the same shape, because
	// compensating is not a special kind of operation — it is another call to
	// another service, and it can fail like any other.
	announce := func(key string) func(context.Context, *Order) error {
		return func(ctx context.Context, order *Order) error {
			return acemq.NewPublisher[*Order](mq, exchange, key).Send(ctx, order)
		}
	}
	refuse := func(reason string) func(context.Context, *Order) error {
		return func(context.Context, *Order) error { return errors.New(reason) }
	}

	// ---- everything works ---------------------------------------------------
	//
	// Booking the courier has no Undo, which is allowed and means what it says:
	// it is the last step, so if it fails there is nothing of it to undo. On a
	// step that changed something it would be a bug, and nothing here can tell
	// the two apart — which is the argument for writing Undo first.
	placed, err := patterns.NewSaga("place-order", []patterns.SagaStep[*Order]{
		{Name: "reserve stock", Do: announce("stock.reserved"), Undo: announce("stock.released")},
		{Name: "take payment", Do: announce("payment.taken"), Undo: announce("payment.refunded")},
		{Name: "book courier", Do: announce("courier.booked")},
	}, patterns.SagaLog[*Order](log.Printf))
	if err != nil {
		log.Fatal(err)
	}

	happy := placed.Run(ctx, &Order{OrderID: "ORD-1"})
	happySaw := settle(events, 3)
	log.Printf("%s", happy)
	log.Printf("  the broker saw: %v", happySaw)
	log.Println()

	// ---- the payment is refused --------------------------------------------
	//
	// One step failed, so the saga stops and undoes the steps before it. The
	// order the broker sees is the point: stock.reserved then stock.released,
	// and nothing about a payment, because the payment never happened.
	refused, err := patterns.NewSaga("place-order", []patterns.SagaStep[*Order]{
		{Name: "reserve stock", Do: announce("stock.reserved"), Undo: announce("stock.released")},
		{Name: "take payment", Do: refuse("the card was declined"), Undo: announce("payment.refunded")},
		{Name: "book courier", Do: announce("courier.booked")},
	}, patterns.SagaLog[*Order](log.Printf))
	if err != nil {
		log.Fatal(err)
	}

	declined := refused.Run(ctx, &Order{OrderID: "ORD-2"})
	declinedSaw := settle(events, 2)
	log.Printf("%s", declined)
	log.Printf("  the broker saw: %v", declinedSaw)
	log.Println()

	// ---- and the run that matters ------------------------------------------
	//
	// The courier cannot be booked, so the payment is refunded and the stock
	// reservation is released — except the warehouse will not release a
	// reservation that has already been picked. The compensation itself fails.
	stuck, err := patterns.NewSaga("place-order", []patterns.SagaStep[*Order]{
		{Name: "reserve stock", Do: announce("stock.reserved"),
			Undo: refuse("the reservation has already been picked")},
		{Name: "take payment", Do: announce("payment.taken"), Undo: announce("payment.refunded")},
		{Name: "book courier", Do: refuse("no courier serves that postcode")},
	}, patterns.SagaLog[*Order](log.Printf))
	if err != nil {
		log.Fatal(err)
	}

	half := stuck.Run(ctx, &Order{OrderID: "ORD-3"})
	halfSaw := settle(events, 3)
	log.Printf("%s", half)
	log.Printf("  the broker saw: %v", halfSaw)
	log.Println()
	log.Printf("unresolved: %v — that is the row a person has to look at", half.Unresolved)

	// ---- what each run claimed, checked ------------------------------------

	if !happy.Complete() {
		log.Fatalf("the first run should have completed: %s", happy)
	}
	if want := []string{"reserve stock", "take payment", "book courier"}; !reflect.DeepEqual(happy.Completed, want) {
		log.Fatalf("the first run ran %v, not %v", happy.Completed, want)
	}
	if happy.HasUnresolved() {
		log.Fatalf("nothing failed, so nothing can be unresolved: %v", happy.Unresolved)
	}
	if want := []string{"stock.reserved", "payment.taken", "courier.booked"}; !reflect.DeepEqual(happySaw, want) {
		log.Fatalf("the broker saw %v, not %v", happySaw, want)
	}

	if !declined.Compensated() || declined.FailedAt != "take payment" {
		log.Fatalf("the second run should have failed at take payment: %s", declined)
	}
	if declined.HasUnresolved() {
		log.Fatalf("every compensation succeeded, so nothing is unresolved: %v", declined.Unresolved)
	}
	if want := []string{"stock.reserved", "stock.released"}; !reflect.DeepEqual(declinedSaw, want) {
		log.Fatalf("the broker saw %v, not %v", declinedSaw, want)
	}

	if half.FailedAt != "book courier" {
		log.Fatalf("the third run should have failed at book courier: %s", half)
	}
	// Backwards, newest first: the payment is refunded before the stock is
	// released, because the payment was taken after the stock was reserved.
	// Undoing them in the order they were done would undo them in the order
	// they depend on each other. The release never appears, because it failed.
	if want := []string{"stock.reserved", "payment.taken", "payment.refunded"}; !reflect.DeepEqual(halfSaw, want) {
		log.Fatalf("the broker saw %v, not %v", halfSaw, want)
	}
	// The whole point of the third run. One compensation failed and the other
	// did not, so the saga reports exactly one name — and reports it instead of
	// stopping, which is what let the payment be refunded at all.
	if want := []string{"reserve stock"}; !reflect.DeepEqual(half.Unresolved, want) {
		log.Fatalf("the third run left %v unresolved, not %v", half.Unresolved, want)
	}

	if err := recorder.Close(); err != nil {
		log.Fatal(err)
	}
	for _, queue := range []string{ledger, acemq.DeadLetterQueue(ledger), acemq.ParkedQueue(ledger)} {
		if err := mq.DeleteQueue(ctx, queue); err != nil {
			log.Fatal(err)
		}
	}

	log.Println()
	log.Println("a saga makes the end state correct, not the middle: between a step")
	log.Println("succeeding and its compensation running, the world has seen the step.")
	log.Println("A customer charged and then refunded got two emails from their bank.")
}

// settle waits for the events one run published and returns them in order.
//
// Publishing is asynchronous, so a saga that has returned is not a saga whose
// events have arrived. Reading exactly as many as the run should have produced
// is also the check that it produced no more.
func settle(events <-chan string, want int) []string {
	seen := make([]string, 0, want)
	deadline := time.After(15 * time.Second)
	for len(seen) < want {
		select {
		case key := <-events:
			seen = append(seen, key)
		case <-deadline:
			log.Fatalf("expected %d events, saw %v", want, seen)
		}
	}
	// Nothing else should turn up. A compensation that ran when it should not
	// have would arrive here rather than in the result.
	select {
	case extra := <-events:
		log.Fatalf("expected %d events, but %s arrived as well", want, extra)
	case <-time.After(300 * time.Millisecond):
	}
	return seen
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
