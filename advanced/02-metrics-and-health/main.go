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

// Metrics and health over HTTP.
//
//	docker compose up -d
//	go run ./advanced/02-metrics-and-health
//
// Then:
//
//	curl localhost:9090/acemq-metrics
//	curl localhost:9090/acemq-health
//	curl localhost:9090/acemq-info
//
// The paths match the Java and .NET libraries, so a scrape configuration or a
// probe written for one works against another.
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/AceMQ-Company/acemq-go-amqp/actuator"
	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type Event struct {
	Kind string `json:"kind"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Nothing is measured until something asks: the default observer does
	// nothing, so a program that never reads metrics does not pay for them.
	metrics := acemq.NewMetrics()

	mq, err := acemq.Connect(ctx, brokerURL(), acemq.WithObserver(metrics))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "events"); err != nil {
		log.Fatal(err)
	}

	consumer, err := acemq.Consume(ctx, mq, "events",
		func(_ context.Context, m acemq.Message[Event]) acemq.Ack {
			if m.Payload.Kind == "bad" {
				return acemq.Reject(errors.New("nothing handles this kind"))
			}
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	act := actuator.New(actuator.Options{
		Metrics: metrics,
		Conn:    mq,
		Name:    "examples",
		Version: "1.0.0",
	})

	// Bound to loopback on purpose. Nothing here is authenticated: health says
	// which dependencies are down and metrics say how much traffic there is,
	// which is more than an anonymous caller should learn about a service.
	server := &http.Server{Addr: "127.0.0.1:9090", Handler: act, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("the actuator stopped: %v", err)
		}
	}()
	defer func() { _ = server.Close() }()

	log.Printf("serving %v on 127.0.0.1:9090", act.Paths())

	publisher := acemq.NewPublisher[Event](mq, "", "events")
	for i := 0; i < 20; i++ {
		kind := "good"
		if i%5 == 0 {
			kind = "bad"
		}
		if err := publisher.Send(ctx, Event{Kind: kind}); err != nil {
			log.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second)

	show(ctx, "/acemq-health")
	show(ctx, "/acemq-info")
	log.Println()
	log.Println("and the metrics Prometheus would scrape:")
	show(ctx, "/acemq-metrics")
}

func show(ctx context.Context, path string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:9090"+path, nil)
	if err != nil {
		log.Fatal(err)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s -> %d\n%s", path, response.StatusCode, body)
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
