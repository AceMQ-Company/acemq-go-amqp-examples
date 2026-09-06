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

// Handling a message once, even when it arrives twice.
//
//	docker compose up -d
//	go run ./intermediate/05-idempotent-consumer
//
// Retries and redeliveries mean a message can arrive more than once, so a
// handler that changes anything needs to be able to tell.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type Charge struct {
	ChargeID string `json:"chargeId"`
	Cents    int64  `json:"cents"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL(),
		acemq.WithRetry(acemq.FixedRetry(5, 200*time.Millisecond).WithJitter(0)))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "charges"); err != nil {
		log.Fatal(err)
	}

	// In this process only, which is right for one worker and wrong the moment
	// there are two: each would have its own memory and both would believe
	// they were first. A shared store — ideally the same database as the work,
	// in the same transaction — is what makes this hold. See patterns.NewSQLIdempotencyStore.
	store := patterns.NewInMemoryIdempotencyStore(time.Hour)

	var mu sync.Mutex
	charged := 0
	deliveries := 0

	handler := patterns.Idempotent(store,
		func(_ context.Context, m acemq.Message[Charge]) acemq.Ack {
			mu.Lock()
			charged++
			mu.Unlock()
			log.Printf("charging %s for %d cents", m.Payload.ChargeID, m.Payload.Cents)
			return acemq.Accept()
		})

	// Wrapped again, only to count how many times the message really arrived.
	counting := func(ctx context.Context, m acemq.Message[Charge]) acemq.Ack {
		mu.Lock()
		deliveries++
		n := deliveries
		mu.Unlock()

		if n == 1 {
			// Fail the first delivery so the broker sends it again. The
			// idempotency guard forgets a failed message, which is what lets
			// the retry actually run.
			log.Printf("delivery %d: failing on purpose", n)
			return acemq.Retry(errors.New("the card processor timed out"))
		}
		log.Printf("delivery %d", n)
		return handler(ctx, m)
	}

	consumer, err := acemq.Consume(ctx, mq, "charges", counting)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	publisher := acemq.NewPublisher[Charge](mq, "", "charges")
	charge := Charge{ChargeID: "c-1", Cents: 4250}

	// The same message identifier three times: one logical message, published
	// more than once, which is what an at-least-once producer looks like.
	for i := 0; i < 3; i++ {
		if err := publisher.Send(ctx, charge, acemq.MessageID("charge-c-1")); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	log.Printf("delivered %d times, charged %d time(s)", deliveries, charged)
	if charged != 1 {
		log.Printf("expected exactly one charge")
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
