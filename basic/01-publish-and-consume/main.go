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

// Publishing a message and consuming it.
//
//	docker compose up -d
//	go run ./basic/01-publish-and-consume
//
// The smallest thing that is still honest: a durable queue, a confirmed
// publish, and a consumer that says what it did with the message.
package main

import (
	"context"
	"log"
	"os"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

// OrderPlaced is the message. The json tags are what make a Go field and a Java
// field the same field — without them the name goes on the wire capitalised and
// nothing in another language reads it.
type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL(),
		// Names this process in RabbitMQ's management interface. Worth setting
		// before you need it, which will be while working out who is holding a
		// message.
		acemq.WithOrigin("examples@01-publish-and-consume"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		log.Fatal(err)
	}

	arrived := make(chan acemq.Message[OrderPlaced], 1)
	consumer, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- m
			// Returning the decision rather than calling a method means a
			// handler that forgets to decide does not compile.
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	publisher := acemq.NewPublisher[OrderPlaced](mq, "", "orders")

	// SendResult rather than Send, so the example can show what the broker
	// said. Send is the same call without the answer.
	result, err := publisher.SendResult(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published %s, confirmed by the broker: %v", result.MessageID, result.Confirmed)

	select {
	case m := <-arrived:
		log.Printf("consumed  %s: order %s for %d cents",
			m.Envelope.ID, m.Payload.OrderID, m.Payload.TotalCents)
		log.Printf("          type=%q attempt=%d origin=%s",
			m.Envelope.Type, m.Envelope.Attempt, m.Envelope.Origin)
	case <-ctx.Done():
		log.Fatal("the message never arrived")
	}
}

// brokerURL is the compose broker unless ACEMQ_URL names another.
//
// Repeated in every example rather than shared: each directory is meant to be
// readable on its own, and a helper somewhere else is one more thing to go and
// find.
func brokerURL() string {
	if url := os.Getenv("ACEMQ_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}
