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

// Surviving a broker that goes away.
//
//	docker compose up -d
//	go run ./advanced/07-connection-recovery
//
// Then, while it runs:
//
//	docker compose restart broker
//
// Without recovery a dropped connection is the quietest failure there is: the
// delivery channel closes, the consumer goroutine ends, the object still looks
// alive, and the service consumes nothing for ever while saying nothing.
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type Tick struct {
	N int `json:"n"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Dialled directly rather than through acemq.Connect, because the recovery
	// settings belong to the transport.
	transport, err := rabbitmq.Dial(ctx, brokerURL(), rabbitmq.Config{
		Name:          "examples/07-connection-recovery",
		RecoveryDelay: 500 * time.Millisecond,
		OnRecovery: func(e rabbitmq.RecoveryEvent) {
			// Recovery nobody can see is only half an improvement on dying
			// quietly. Log it, alert on it, count it.
			log.Printf("connection: %s", e)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	log.Printf("the transport recovers by itself: %v", mq.Supports(acemq.CapabilityRecovery))

	if err := mq.DeclareQueue(ctx, "ticks", acemq.AutoDelete()); err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	received := 0

	consumer, err := acemq.Consume(ctx, mq, "ticks",
		func(_ context.Context, m acemq.Message[Tick]) acemq.Ack {
			mu.Lock()
			received++
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	publisher := acemq.NewPublisher[Tick](mq, "", "ticks")

	log.Println("publishing one message a second — restart the broker and watch")
	log.Println("  docker compose restart broker")

	// Shortened in CI, where nobody is standing by to restart anything.
	runFor := 45 * time.Second
	if seconds := os.Getenv("ACEMQ_EXAMPLE_SECONDS"); seconds != "" {
		if n, err := strconv.Atoi(seconds); err == nil {
			runFor = time.Duration(n) * time.Second
		}
	}

	deadline := time.Now().Add(runFor)
	for n := 1; time.Now().Before(deadline); n++ {
		if err := publisher.Send(ctx, Tick{N: n}); err != nil {
			// While the broker is away this fails, and says so, rather than
			// blocking for ever.
			log.Printf("publish %d failed: %v", n, err)
		}
		time.Sleep(time.Second)

		if n%10 == 0 {
			mu.Lock()
			log.Printf("published %d, consumed %d", n, received)
			mu.Unlock()
		}
	}

	mu.Lock()
	log.Printf("finished: consumed %d messages", received)
	mu.Unlock()
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
