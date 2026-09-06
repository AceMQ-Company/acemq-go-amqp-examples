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

// Declaring a topology, and noticing when the broker disagrees.
//
//	docker compose up -d
//	go run ./basic/03-topology-and-drift
//
// A service and its broker disagreeing about a queue is the failure that shows
// up as messages going somewhere nobody is looking.
package main

import (
	"context"
	"log"
	"os"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	topology := acemq.NewTopology().
		Exchange("orders-events", "topic").
		Queue("shipping-orders", acemq.DeadLetterTo("shipping-dead")).
		Queue("shipping-dead").
		Binding("shipping-orders", "orders-events", "order.placed").
		Binding("shipping-orders", "orders-events", "order.cancelled")

	// Readable before it is applied. A deployment that changes a broker should
	// be something somebody can look at first.
	log.Printf("this is what will be declared:\n%s", topology)

	if err := topology.Apply(ctx, mq); err != nil {
		log.Fatal(err)
	}
	log.Println("applied")

	// Applying the same topology again is fine.
	if err := topology.Apply(ctx, mq); err != nil {
		log.Fatal(err)
	}
	log.Println("applied again, unchanged, without complaint")

	reports, err := topology.Check(ctx, mq)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("drift against the broker: %d differences", len(reports))

	// Now ask for something the broker will refuse: the same queue, pointing
	// its dead letters somewhere else.
	changed := acemq.NewTopology().
		Exchange("orders-events", "topic").
		Queue("shipping-orders", acemq.DeadLetterTo("somewhere-else")).
		Queue("shipping-dead").
		Binding("shipping-orders", "orders-events", "order.placed")

	reports, err = changed.Check(ctx, mq)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range reports {
		log.Printf("drift: %s", r)
	}
	if len(reports) == 0 {
		log.Println("no drift reported, which is not what this example expects")
	}

	// A mistake the broker cannot catch: a binding to a queue nothing
	// declares. It would work wherever that queue happens to exist already,
	// and fail on a fresh environment for reasons nobody can see.
	invalid := acemq.NewTopology().
		Exchange("orders-events", "topic").
		Binding("a-queue-nobody-declared", "orders-events", "#")
	if err := invalid.Validate(); err != nil {
		log.Printf("caught before the broker saw it: %v", err)
	}

	// And the topology routes, which is the only proof that matters.
	arrived := make(chan OrderPlaced, 1)
	consumer, err := acemq.Consume(ctx, mq, "shipping-orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "orders-events", "order.placed").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		log.Fatal(err)
	}

	select {
	case order := <-arrived:
		log.Printf("routed end to end: %s", order.OrderID)
	case <-ctx.Done():
		log.Fatal("nothing routed through the topology")
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
