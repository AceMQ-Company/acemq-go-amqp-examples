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

// Retrying a message, and giving up on it.
//
//	docker compose up -d
//	go run ./basic/02-retries-and-dead-letters
//
// The attempt counter is the thing to watch. A broker requeues the bytes it was
// given, so the header on the wire still says 1 however many times the message
// has come back — the count comes from the redelivery flag instead, which is
// why the number below actually moves.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type Payment struct {
	PaymentID string `json:"paymentId"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL(),
		// Three attempts in all, a short wait between them, and no jitter so
		// the output is readable. Use jitter in anything real: messages that
		// failed together must not come back together.
		acemq.WithRetry(acemq.FixedRetry(3, 300*time.Millisecond).WithJitter(0)))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	// Only the source queue is declared here. A consumer declares the
	// dead-letter half of its own topology as it starts — acemq.dlx,
	// payments.dlq and payments.parked, with their bindings — so a message the
	// library gives up on has somewhere to land whether or not anyone
	// remembered to apply a topology first. That matters: the broker discards
	// an unroutable message without a trace, and the one message just declared
	// worth keeping would be the one that vanished.
	if err := mq.DeclareQueue(ctx, "payments"); err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	var attempts []int

	consumer, err := acemq.Consume(ctx, mq, "payments",
		func(_ context.Context, m acemq.Message[Payment]) acemq.Ack {
			mu.Lock()
			attempts = append(attempts, m.Envelope.Attempt)
			mu.Unlock()

			log.Printf("handling %s, attempt %d, age %s",
				m.Payload.PaymentID, m.Envelope.Attempt, m.Envelope.Age().Round(time.Millisecond))

			// Always fails. The policy decides when to stop.
			return acemq.Retry(errors.New("the payment gateway is not answering"))
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// payments.dlq, declared by the consumer above rather than here: it is
	// classic where a durable queue declared by hand is quorum, and a queue
	// cannot be redeclared as a different type. A dead-letter queue nobody
	// reads is a place messages go to be forgotten quietly, which is worse
	// than dropping them, so something has to read it — here, this.
	dead := make(chan acemq.Message[Payment], 1)
	deadConsumer, err := acemq.Consume(ctx, mq, acemq.DeadLetterQueue("payments"),
		func(_ context.Context, m acemq.Message[Payment]) acemq.Ack {
			dead <- m
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer deadConsumer.Close()

	if err := acemq.NewPublisher[Payment](mq, "", "payments").
		Send(ctx, Payment{PaymentID: "p-1"}); err != nil {
		log.Fatal(err)
	}

	select {
	case m := <-dead:
		mu.Lock()
		log.Printf("dead-lettered after attempts %v", attempts)
		mu.Unlock()
		log.Printf("the envelope kept its history: id=%s first seen %s ago",
			m.Envelope.ID, m.Envelope.Age().Round(time.Millisecond))
	case <-ctx.Done():
		log.Fatal("the message never reached the dead-letter queue")
	}

	// Closed before the next consumer starts. Two consumers on one queue
	// compete for the same message, and the output would read as though one
	// message had been handled twice.
	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}

	// An error that will not improve should not wait for the attempts to run
	// out. Marking it skips straight to dead-lettering.
	log.Println()
	log.Println("the same queue, with a reason that retrying cannot fix:")

	fatal := make(chan struct{}, 1)
	fatalConsumer, err := acemq.Consume(ctx, mq, "payments",
		func(_ context.Context, m acemq.Message[Payment]) acemq.Ack {
			log.Printf("handling %s, attempt %d", m.Payload.PaymentID, m.Envelope.Attempt)
			fatal <- struct{}{}
			return acemq.Retry(acemq.Fatal(errors.New("this payment has no customer")))
		})
	if err != nil {
		log.Fatal(err)
	}
	defer fatalConsumer.Close()

	if err := acemq.NewPublisher[Payment](mq, "", "payments").
		Send(ctx, Payment{PaymentID: "p-2"}); err != nil {
		log.Fatal(err)
	}

	<-fatal
	time.Sleep(500 * time.Millisecond)
	log.Println("handled once, then dead-lettered: the mark won over the request")
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
