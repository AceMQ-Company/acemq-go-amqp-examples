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

// Putting dead-lettered messages back.
//
//	docker compose up -d
//	go run ./basic/04-replay
//
// Dead-lettering is half the story. The messages are still there, and the point
// of keeping them is that they get another run once whatever broke is fixed —
// which is the thing somebody actually does at three in the morning, usually
// for one tenant at a time rather than all two thousand at once.
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type Invoice struct {
	InvoiceID string `json:"invoiceId"`
	Tenant    string `json:"tenant"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "go-replay-invoices-dead"); err != nil {
		log.Fatal(err)
	}
	if err := mq.DeclareQueue(ctx, "go-replay-invoices"); err != nil {
		log.Fatal(err)
	}

	// ---- the morning after -------------------------------------------------
	//
	// Three invoices are on the dead-letter queue: the ledger service was down
	// and the handler rejected them, which is what example 02 shows. They are
	// put there directly here so the example starts where the interesting part
	// starts, and so it says the same thing every time it runs.
	dead := acemq.NewPublisher[Invoice](mq, "", "go-replay-invoices-dead")
	for _, invoice := range []Invoice{
		{InvoiceID: "inv-1", Tenant: "acme"},
		{InvoiceID: "inv-2", Tenant: "globex"},
		{InvoiceID: "inv-3", Tenant: "acme"},
	} {
		if err := dead.Send(ctx, invoice); err != nil {
			log.Fatal(err)
		}
	}

	// The service that will handle them once they are back. Starting it first
	// matters: replaying into a queue nobody is reading only moves the problem.
	var acme sync.WaitGroup
	acme.Add(2)
	var rest sync.WaitGroup
	rest.Add(1)
	var mu sync.Mutex
	handled := 0

	fixed, err := acemq.Consume(ctx, mq, "go-replay-invoices",
		func(_ context.Context, m acemq.Message[Invoice]) acemq.Ack {
			log.Printf("  handled %s for %s", m.Payload.InvoiceID, m.Payload.Tenant)
			mu.Lock()
			handled++
			done := handled
			mu.Unlock()
			if done <= 2 {
				acme.Done()
			} else {
				rest.Done()
			}
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer fixed.Close()

	// ---- the fix is deployed, but only for one tenant ---------------------
	//
	// A filter is normally about picking out one tenant or one kind of failure.
	// What it declines is left where it was rather than discarded, which is what
	// makes a replay something that can be done in stages.
	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue: "go-replay-invoices-dead",
		// The default exchange routes to the queue named by the key, so this is
		// where the messages go back to.
		RoutingKey: "go-replay-invoices",
		// Always give a limit. Without one, a replay against a queue somebody is
		// still writing to may never stop.
		Limit: 10,
		Filter: func(_ acemq.Envelope, body []byte) bool {
			// The filter sees the body rather than a decoded payload: what is on
			// a dead-letter queue is not guaranteed to be anything a codec can
			// read, and a replay should not fall over because one message is not.
			return strings.Contains(string(body), `"acme"`)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("replayed %d, left %d where they were, stopped because it %s",
		result.Moved, result.Skipped, result.Reason)

	acme.Wait()

	// ---- and later, the rest ----------------------------------------------
	//
	// What the filter declined is still on the dead-letter queue, which is the
	// point: a replay in stages can be stopped after the first stage goes wrong.
	result, err = patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue:      "go-replay-invoices-dead",
		RoutingKey: "go-replay-invoices",
		Limit:      10,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("replayed the remaining %d, stopped because it %s", result.Moved, result.Reason)

	rest.Wait()
	log.Print("the dead-letter queue is empty and every invoice was handled once")
}

func brokerURL() string {
	if url := os.Getenv("ACEMQ_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}
