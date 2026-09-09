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

// Writing the message in the same transaction as the work.
//
//	docker compose up -d
//	go run ./intermediate/03-outbox
//
// A service that writes to a database and then publishes has two things that
// can fail independently, and the gap between them is where messages are lost
// or invented.
package main

import (
	"context"
	"log"
	"os"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type OrderPlaced struct {
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

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		log.Fatal(err)
	}

	arrived := make(chan OrderPlaced, 4)
	consumer, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// In memory here so the example needs no database. It has none of the
	// property the pattern exists for: nothing shares a transaction with your
	// work, so a crash between the work committing and the record being
	// written loses the message exactly as publishing directly would.
	//
	// Use patterns.NewSQLOutboxStore with your own *sql.Tx for anything real —
	// it takes anything with ExecContext, which is what lets the record commit
	// with the work.
	store := patterns.NewInMemoryOutboxStore()

	// This is what a real handler does inside its transaction.
	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		log.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		log.Fatal(err)
	}
	log.Printf("recorded %s in the outbox; nothing has been published yet", record.ID)

	// Adding it again is not an error — a caller retrying its own transaction
	// must not turn one message into two.
	if err := store.Add(ctx, record); err != nil {
		log.Fatal(err)
	}
	log.Printf("recorded again; the outbox holds %d message(s)", store.Len())

	relay := patterns.NewOutboxRelay(mq, store, patterns.RelayInterval(200*time.Millisecond))
	relay.Start(ctx)
	defer relay.Close()
	log.Println("the relay is running")

	select {
	case order := <-arrived:
		log.Printf("published by the relay and consumed: %s", order.OrderID)
	case <-ctx.Done():
		log.Fatal("the relay never published it")
	}

	time.Sleep(300 * time.Millisecond)
	log.Printf("the outbox now holds %d message(s)", store.Len())
	log.Println()
	log.Println("a record is removed only after the broker confirms it, so a crash")
	log.Println("in between republishes rather than loses — which is why anything")
	log.Println("consuming this needs to be idempotent. See example 02.")
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
