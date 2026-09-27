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

// A three-step flow declared in one place, with a queue between every step.
//
//	docker compose up -d
//	go run ./intermediate/08-a-declared-pipeline
//
// One handler doing validate-then-reserve-then-dispatch in sequence is simpler
// until something goes wrong with it: a slow dispatch holds up validation, a
// crash halfway loses the middle, and scaling is all or nothing. A queue between
// each pair of steps changes all three — a crash leaves the message where it
// was, a slow step grows its own queue while the others carry on, and the one
// step that needs four consumers gets four.
//
// Two things to watch. The second order is digital, so `reserve` returns false
// and the run stops there: a decision rather than a failure, and nothing is
// published onwards. And `dispatch` never sees it — which is the point of
// printing what each step saw rather than what the pipeline was asked to do.
//
// The step names are wire format: they are the routing keys and the queue
// suffixes, and the queues are fulfilment.validate, fulfilment.reserve and
// fulfilment.dispatch. That is the same topology Java, .NET, Python and Ruby
// declare, so a step of this pipeline could be running in any of them.
package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

// Order is what goes in at the first step.
type Order struct {
	ID      string `json:"id"`
	Digital bool   `json:"digital"`
}

// Reservation is what the second step produces, and the third consumes. The
// types have to line up across every boundary, and NewPipeline says so at
// start-up if they do not.
type Reservation struct {
	OrderID string `json:"orderId"`
	Shelf   string `json:"shelf"`
}

func main() {
	ctx := context.Background()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatalf("cannot reach the broker: %v", err)
	}
	defer func() { _ = mq.Close() }()

	// What each step saw, so the output shows the flow rather than the intent.
	var mu sync.Mutex
	seen := map[string][]string{}
	note := func(step, what string) {
		mu.Lock()
		defer mu.Unlock()
		seen[step] = append(seen[step], what)
	}

	// Both runs have to be waited for, and one of them ends early — so the wait
	// is on two different signals rather than on a count of finished orders.
	dispatched := make(chan string, 2)
	stopped := make(chan string, 2)

	pipeline, err := patterns.NewPipeline[Order](ctx, mq, "fulfilment",
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[Order]) (Order, bool, error) {
				note("validate", m.Payload.ID)
				return m.Payload, true, nil
			},
			patterns.StepDescribedAs("reject anything nobody can ship")),

		patterns.PipelineStep("reserve",
			func(_ context.Context, m acemq.Message[Order]) (Reservation, bool, error) {
				note("reserve", m.Payload.ID)
				if m.Payload.Digital {
					// Nothing to reserve, so the run stops here. Returning false
					// publishes nothing and accepts the message: a decision, not a
					// failure, and reported as ended_early rather than as an error.
					stopped <- m.Payload.ID
					return Reservation{}, false, nil
				}
				return Reservation{OrderID: m.Payload.ID, Shelf: "A-14"}, true, nil
			},
			patterns.StepConsumers(4),
			patterns.StepRetry(acemq.ExponentialRetry(4, time.Second, time.Minute)),
			patterns.StepDescribedAs("hold stock for 15 minutes so payment cannot oversell")),

		patterns.PipelineStep("dispatch",
			func(_ context.Context, m acemq.Message[Reservation]) (struct{}, bool, error) {
				note("dispatch", m.Payload.OrderID)
				dispatched <- m.Payload.OrderID
				// The last step returns nothing anybody publishes, which is why a
				// terminal step conventionally returns struct{}.
				return struct{}{}, true, nil
			},
			patterns.StepDescribedAs("hand the parcel to the courier")),
	)
	if err != nil {
		// A type that does not line up, a step name that cannot be a routing key,
		// or a broker that refused a declaration. All three fail here rather than
		// on the first message.
		log.Fatalf("cannot declare the pipeline: %v", err)
	}
	defer func() { _ = pipeline.Close() }()

	// Nothing in the library logs. This is the line that says what the flow does,
	// and the application decides where it goes.
	log.Printf("%s", pipeline.Describe())

	physical, err := pipeline.Send(ctx, Order{ID: "A-1"})
	if err != nil {
		log.Fatalf("cannot start a run: %v", err)
	}
	digital, err := pipeline.Send(ctx, Order{ID: "D-2", Digital: true})
	if err != nil {
		log.Fatalf("cannot start a run: %v", err)
	}
	log.Printf("two runs started: %s (physical) and %s (digital)", short(physical), short(digital))

	// One of each, and then stop. An example that waits for a message that is
	// never coming is an example that hangs in somebody's CI.
	deadline := time.After(30 * time.Second)
	for range 2 {
		select {
		case id := <-dispatched:
			log.Printf("dispatched %s", id)
		case id := <-stopped:
			log.Printf("%s needs no shipping, so the run ended at reserve", id)
		case <-deadline:
			log.Fatal("a run never finished; is the broker reachable?")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	log.Printf("validate saw %v", seen["validate"])
	log.Printf("reserve  saw %v", seen["reserve"])
	// D-2 is absent, and that is the whole point of the false.
	log.Printf("dispatch saw %v", seen["dispatch"])

	log.Printf("the queues are %s, %s and %s — one per step, which is what lets you"+
		" scale or watch one of them alone",
		pipeline.QueueFor("validate"), pipeline.QueueFor("reserve"), pipeline.QueueFor("dispatch"))
}

// short trims a run identifier to something readable in a log line.
func short(runID string) string {
	if len(runID) > 8 {
		return runID[:8]
	}
	return runID
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
