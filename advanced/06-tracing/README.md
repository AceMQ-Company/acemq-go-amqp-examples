# advanced/06 — tracing across the broker

A consumer's span is a child of the publish that caused it, and the parent came
off the message rather than out of ambient context.

## What it shows

- **The join**, asserted: same trace id, and the publish's span id as the
  consumer span's parent.
- **Where the parent came from**, asserted by contrast: a second message
  published outside any trace produces a consumer span that is a root, in a trace
  of its own. Same handler, same connection, same queue.
- **`traceparent` on the wire**, checked against the trace it names.
- **The span's outcome is the engine's word**, not the handler's request.

## Running it

```bash
docker compose up -d
go run ./advanced/06-tracing
```

## What to look for

```
go-tracing.orders publish    9050122543fe2a964fc1b0c3efb16fd0  parent=none, this is a root
go-tracing.orders process    9050122543fe2a964fc1b0c3efb16fd0  parent=032052f045fd3396
go-tracing.orders process    ab3b669205cc8fd6190c52fc69ee0b12  parent=none, this is a root

A-7 arrived carrying traceparent 00-9050122543fe2a964fc1b0c3efb16fd0-032052f045fd3396-01
A-8 arrived carrying traceparent ""
```

**The first two lines share a trace id**, and the second names the first's span
id as its parent. A publish and a delivery, in different goroutines with a broker
between them, in one trace.

**The third line is the proof.** It is the same handler on the same connection
reading the same queue, and its span is a root in a trace of its own — because
that message carried no `traceparent`. If the join were coming from ambient
context, from the goroutine, or from anything else in the process, this span
would have joined too.

Read the `traceparent` values underneath: `00-<trace id>-<span id>-01`, and the
trace id and span id in the first are exactly the two the spans above report.

## Why this is the thing worth proving

An HTTP client's parent is on the same stack, microseconds ago. A message's
parent is in another process, possibly minutes ago, on a machine that may have
been replaced since. Nothing on the goroutine that receives a delivery remembers
it, and anything current on that goroutine belongs to some other message.

So `StartConsume` extracts the parent from the message's own headers and does not
consult ambient context on that path at all. Joining those two spans is the one
thing a messaging system needs from tracing that an HTTP client does not, and it
is the thing that quietly stops working.

## The names on the wire

`traceparent` and `tracestate` — the W3C names, deliberately not `x-acemq-`
prefixed. Other tooling already knows them, and renaming them would make this
library's traces invisible to everything that did not know to look.

Java, .NET, Python and Ruby write the same two, so a Go consumer joins a Java
producer's trace without either end being configured for the other.

## Three pieces of wiring

```go
tracing := otel.New()

mq, _ := acemq.Connect(ctx, url,
    acemq.WithPublishInterceptor(tracing.PublishInterceptor()))

orders := otel.NewPublisher[OrderPlaced](tracing, mq, "", "orders")

_, _ = acemq.Consume(ctx, mq, "orders", otel.Handle(tracing, "orders", handle))
```

The **interceptor** writes the trace context onto every message published on the
connection — including the ones inside `patterns.Requester` and the outbox relay,
which your code never constructs. It also completes the publish span's
attributes: the span is opened before the envelope exists, so the message id and
type are filled in when it does. The example asserts that the publish span names
the message the consumer received.

It writes nothing when nothing is being traced, so a process with no SDK
configured publishes exactly the headers it did before.

## The engine has the last word

`otel.Handle` hands the span's ending to the engine rather than closing it when
the handler returns. A handler asking for a retry is a *request*: a message on its
last attempt is dead-lettered instead, and a span ended early would say `retried`
for a message nobody will ever try again — which is exactly what somebody
searching a trace backend for dead letters fails to find.

The outcome written on the span is `acemq.Settlement.Outcome`, the same string
`acemq.consume.total` is tagged with for that delivery. It is not derived a second
time from the handler's `Ack`, so a dashboard filtered to dead-lettered messages
and a trace search for the same thing cannot return different sets. The example
asserts the outcome is `acked`.

## A recorder, not an exporter

This example reads its spans back to check them, so it uses
`tracetest.NewSpanRecorder` and samples everything. A service configures an
exporter and a sampler instead, and nothing else changes: the adapter takes
whatever tracer provider the application set up, and emits nothing at all until
one exists — which is the right default for a library, since the exporter and the
sampler are the application's business.

## Events rather than spans

A retry, a dead letter, an outbox failure and a finished pipeline run are
recorded as events on the span that was already open rather than as spans of
their own. `acemq.Observer` answers *how much*; this answers *what happened to
this message*. Neither substitutes for the other: a counter says a thousand
messages were dead-lettered, and a trace says this one was published by checkout,
retried twice over four minutes and given up on.

## The module

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
```

A module of its own, so a service that publishes messages and traces nothing
never resolves OpenTelemetry at all. It pins OpenTelemetry v1.38.0, the newest
release that still builds on Go 1.23 — the floor the library targets.
