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

// Asking a question and waiting for the answer.
//
//	docker compose up -d
//	go run ./intermediate/01-request-reply
//
// A synchronous shape drawn on an asynchronous system, which costs something:
// a caller blocked on a reply holds a goroutine and a deadline, and a queue
// that backs up turns into a service that stops responding. Reach for it where
// a caller genuinely cannot proceed without the answer.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type PriceRequest struct {
	SKU string `json:"sku"`
}

type PriceResponse struct {
	Cents int64 `json:"cents"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		log.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "prices",
		func(_ context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
			if m.Payload.SKU == "unknown" {
				// The failure goes back to the caller rather than being
				// swallowed, so it learns instead of waiting out its timeout.
				return PriceResponse{}, errors.New("no such product")
			}
			return PriceResponse{Cents: int64(len(m.Payload.SKU)) * 100}, nil
		},
		acemq.Concurrency(4))
	if err != nil {
		log.Fatal(err)
	}
	defer responder.Close()

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](
		ctx, mq, "", "prices", patterns.Timeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer requester.Close()

	log.Printf("replies come back on %s", requester.ReplyQueue())

	response, err := requester.Do(ctx, PriceRequest{SKU: "widget"})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("widget costs %d cents", response.Cents)

	// Many in flight at once. Replies are paired with requests by correlation
	// identifier, so nobody gets somebody else's answer.
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sku := strings.Repeat("x", n)
			r, err := requester.Do(ctx, PriceRequest{SKU: sku})
			if err != nil {
				log.Printf("  %-12s failed: %v", sku, err)
				return
			}
			if r.Cents != int64(n)*100 {
				log.Printf("  %-12s WRONG ANSWER: %d", sku, r.Cents)
			}
		}(i)
	}
	wg.Wait()
	log.Println("ten concurrent requests, each got its own answer")

	// A responder that fails says so.
	if _, err := requester.Do(ctx, PriceRequest{SKU: "unknown"}); err != nil {
		log.Printf("the responder's reason came back: %v", err)
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
