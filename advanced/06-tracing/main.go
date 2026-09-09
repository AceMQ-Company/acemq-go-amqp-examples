// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// A consumer's span joined to the publish that caused it, across the broker.
//
//	docker compose up -d
//	go run ./advanced/06-tracing
//
// The join is the whole point, and it is the one thing a messaging system needs
// from tracing that an HTTP client does not. The publish happened in another
// process, possibly minutes ago; nothing on the goroutine that receives the
// delivery remembers it. So the parent comes out of the message's own
// traceparent header rather than out of whatever this goroutine happened to be
// doing.
//
// This example proves that rather than describing it. It sends two messages —
// one published inside a trace and one published outside any trace at all — and
// checks that the first consumer span is a child of its publish and the second
// is a root. Same handler, same connection, same queue: the only difference is
// whether the message carried a traceparent.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
//
// The adapter is a module of its own, so a service that publishes messages and
// traces nothing never resolves OpenTelemetry at all.
//
// # traceparent, not x-acemq-traceparent
//
// The W3C names, deliberately: other tooling already knows them, and renaming
// them would make this library's traces invisible to everything that did not
// know to look. Java, .NET, Python and Ruby write the same two, so a Go consumer
// joins a Java producer's trace without either end being configured for the
// other.
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
	"github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const queue = "go-tracing.orders"

type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

// arrival is what the handler saw: the payload, and the trace context that
// arrived on the message rather than the one the process happened to hold.
type arrival struct {
	order       OrderPlaced
	traceParent string
	messageID   string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A recorder rather than an exporter, because this example has to read the
	// spans back and check them. A service exports to a collector instead, and
	// nothing else here changes: the adapter takes whatever tracer provider the
	// application configured.
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
		// Every span, so the two this example needs cannot be sampled away. A
		// service samples; a proof does not.
		sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	tracing := otel.New(otel.WithTracerProvider(provider))

	// Registered once, and every publisher on the connection carries the trace —
	// including the ones inside patterns.Requester and the outbox relay, which
	// this file never constructs. It writes nothing when nothing is being
	// traced, so a process with no SDK configured publishes exactly the headers
	// it did before.
	mq, err := acemq.Connect(ctx, brokerURL(),
		acemq.WithPublishInterceptor(tracing.PublishInterceptor()),
		acemq.WithOrigin("examples@06-tracing"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}

	arrived := make(chan arrival, 4)

	// otel.Handle opens the CONSUMER span around the handler and hands its
	// ending to the engine, so the span says what the engine did with the
	// message rather than what the handler asked for. A handler that asks for a
	// retry on a message's last attempt gets a dead letter, and a span ended
	// when the handler returned would say retried for a message nobody will
	// ever try again.
	consumer, err := acemq.Consume(ctx, mq, queue,
		otel.Handle(tracing, queue,
			func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
				arrived <- arrival{
					order:       m.Payload,
					traceParent: headerString(m.Envelope.Headers, acemq.HeaderTraceParent),
					messageID:   m.Envelope.ID,
				}
				return acemq.Accept()
			}))
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// ---- a message published inside a trace --------------------------------

	// otel.NewPublisher opens the PRODUCER span; a publisher rather than a
	// wrapper function because the span is named after the destination, and the
	// destination is what a publisher knows.
	traced := otel.NewPublisher[OrderPlaced](tracing, mq, "", queue)
	if err := traced.Send(ctx, OrderPlaced{OrderID: "A-7"},
		acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}
	first := next(ctx, arrived)

	// ---- and one published outside any trace at all ------------------------

	// A plain publisher, on a context with no span on it. The interceptor is
	// still registered and still runs; it simply has nothing to write.
	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "A-8"},
			acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}
	second := next(ctx, arrived)

	// Three spans: one publish, and one per delivery. The second message had no
	// publish span, which is the point of it. A consume span is ended by the
	// engine after the handler returns, so the recorder is read once they have
	// finished rather than as soon as the payloads arrive.
	spans := waitForSpans(recorder, 3)

	publish := oneNamed(spans, queue+" publish")
	process := allNamed(spans, queue+" process")
	if len(process) != 2 {
		log.Fatalf("expected two consumer spans, found %d", len(process))
	}
	joined, orphan := process[0], process[1]
	// The two consumer spans arrive in the order the messages were handled, and
	// the first message is the traced one — but ordering by arrival is a thing
	// that can quietly stop being true, so they are told apart by which one has
	// a parent.
	if !joined.Parent().IsValid() {
		joined, orphan = orphan, joined
	}

	log.Println()
	log.Printf("%-28s %s  parent=%s", publish.Name(),
		publish.SpanContext().TraceID(), shortID(publish.Parent()))
	log.Printf("%-28s %s  parent=%s", joined.Name(),
		joined.SpanContext().TraceID(), shortID(joined.Parent()))
	log.Printf("%-28s %s  parent=%s", orphan.Name(),
		orphan.SpanContext().TraceID(), shortID(orphan.Parent()))

	log.Println()
	log.Printf("A-7 arrived carrying traceparent %s", first.traceParent)
	log.Printf("A-8 arrived carrying traceparent %q", second.traceParent)

	// ---- what all of that has to say, checked ------------------------------

	if first.order.OrderID != "A-7" || second.order.OrderID != "A-8" {
		log.Fatalf("the wrong messages arrived: %s and %s",
			first.order.OrderID, second.order.OrderID)
	}

	// The join. The consumer's span is a child of the publish span — the same
	// trace, and the publish's span id as its parent — and the two ran in
	// different goroutines with a broker between them.
	if joined.SpanContext().TraceID() != publish.SpanContext().TraceID() {
		log.Fatalf("the consumer span is in trace %s and the publish is in %s",
			joined.SpanContext().TraceID(), publish.SpanContext().TraceID())
	}
	if joined.Parent().SpanID() != publish.SpanContext().SpanID() {
		log.Fatalf("the consumer span's parent is %s, and the publish is %s",
			joined.Parent().SpanID(), publish.SpanContext().SpanID())
	}

	// And it came off the message rather than out of ambient context. The
	// traceparent that arrived names the publish span's trace, and the second
	// message — which carried none — produced a span with no parent in a trace
	// of its own. Same handler, same connection, same queue.
	if !strings.Contains(first.traceParent, publish.SpanContext().TraceID().String()) {
		log.Fatalf("the traceparent on the message is %q, and the trace is %s",
			first.traceParent, publish.SpanContext().TraceID())
	}
	if second.traceParent != "" {
		log.Fatalf("the untraced message carried a traceparent: %q", second.traceParent)
	}
	if orphan.Parent().IsValid() {
		log.Fatalf("the untraced message's span has a parent: %s", orphan.Parent().SpanID())
	}
	if orphan.SpanContext().TraceID() == joined.SpanContext().TraceID() {
		log.Fatal("both consumer spans are in one trace, so the join is not coming from the header")
	}

	// The kinds are the OpenTelemetry messaging conventions, shared with the
	// Java adapter: a publish is a PRODUCER and a delivery a CONSUMER. A reader
	// who cannot tell those apart cannot tell a slow broker from a slow handler.
	if publish.SpanKind() != trace.SpanKindProducer {
		log.Fatalf("the publish span is a %s", publish.SpanKind())
	}
	if joined.SpanKind() != trace.SpanKindConsumer {
		log.Fatalf("the consume span is a %s", joined.SpanKind())
	}

	// The publish span carries the message's identity, which it cannot have had
	// when it was opened: the envelope is built inside the publish, and the
	// interceptor fills these in when it exists. Without the interceptor
	// registered the span would name the destination and nothing about the
	// message.
	if got := attribute(publish, otel.AttrMessageID); got != first.messageID {
		log.Fatalf("the publish span names message %q and the consumer saw %q",
			got, first.messageID)
	}

	// The outcome is the engine's word, and the same string acemq.consume.total
	// is tagged with for this delivery — so a dashboard filtered to dead letters
	// and a trace search for the same thing cannot return different sets.
	if got := attribute(joined, otel.AttrOutcome); got != otel.OutcomeAcked {
		log.Fatalf("the consumer span's outcome is %q", got)
	}

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := mq.DeleteQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
}

// waitForSpans waits until at least n spans have ended.
//
// A consume span ends when the engine has settled the message, which is after
// the handler returned — so reading the recorder as soon as the payload arrives
// finds a span that is still open.
func waitForSpans(recorder *tracetest.SpanRecorder, n int) []sdktrace.ReadOnlySpan {
	deadline := time.After(30 * time.Second)
	for {
		if ended := recorder.Ended(); len(ended) >= n {
			return ended
		}
		select {
		case <-deadline:
			log.Fatalf("only %d spans ended, expected %d", len(recorder.Ended()), n)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func allNamed(spans []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func oneNamed(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	found := allNamed(spans, name)
	if len(found) != 1 {
		log.Fatalf("expected one span named %q, found %d", name, len(found))
	}
	return found[0]
}

func attribute(span sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func shortID(parent trace.SpanContext) string {
	if !parent.IsValid() {
		return "none, this is a root"
	}
	return parent.SpanID().String()
}

// next takes the next delivery, or gives up saying so.
func next(ctx context.Context, from <-chan arrival) arrival {
	select {
	case a := <-from:
		return a
	case <-ctx.Done():
		log.Fatal("a message never arrived")
		return arrival{}
	}
}

// headerString reads a header that may have arrived as a string or as bytes,
// which is what an AMQP long string can be.
func headerString(headers map[string]any, name string) string {
	switch value := headers[name].(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return ""
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
