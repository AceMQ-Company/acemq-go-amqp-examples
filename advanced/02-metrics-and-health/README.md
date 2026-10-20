# advanced/02 — metrics and health over HTTP

A consumer handles twenty events and rejects four of them, while the `actuator`
package serves what happened over HTTP: `/acemq-health` for a probe,
`/acemq-info` for what is running, and `/acemq-metrics` for Prometheus. The
paths are the same as in the Java and .NET libraries, so a scrape configuration
or a probe written for one works against the others.

## What it shows

- **`acemq.NewMetrics`** passed to `acemq.WithObserver`. Nothing is measured
  unless an observer is given, so a program that never reads metrics does not
  pay for them.
- **`actuator.New`**, an `http.Handler` with three paths.
- **Health with a reason**: the broker's part reports whether the connection is
  blocked, how many consumers are attached and the round-trip time.

## Running it

```bash
docker compose up -d
go run ./advanced/02-metrics-and-health
```

It prints all three responses itself. While it runs you can also ask:

```bash
curl localhost:9090/acemq-metrics
curl localhost:9090/acemq-health
curl localhost:9090/acemq-info
```

## What to look for

```
serving [/acemq-metrics /acemq-health /acemq-info] on 127.0.0.1:9090
/acemq-health -> 200
{"status":"up","checked":"2026-10-13T18:29:56.951682Z","parts":{"broker":{"status":"up","checked":"2026-10-13T18:29:56.951702Z","parts":{"blocked":false,"consumers":1,"roundTripMillis":5}}}}
/acemq-info -> 200
{"library":"acemq-go-amqp","libraryVersion":"0.9.10","name":"examples","transport":{"dead-lettering":true,"publisher-confirms":true,"quorum-queues":true,"recovery":true,"streams":true},"version":"1.0.0"}

and the metrics Prometheus would scrape:
/acemq-metrics -> 200
# TYPE acemq_consume_total counter
acemq_consume_total{outcome="acked",queue="events"} 16
# TYPE acemq_consume_total counter
acemq_consume_total{outcome="rejected",queue="events"} 4
# TYPE acemq_publish_total counter
acemq_publish_total{exchange="",outcome="confirmed",routing_key="events"} 20
# TYPE acemq_consume_in_flight gauge
acemq_consume_in_flight{queue="events"} 0
# TYPE acemq_consume_attempts summary
acemq_consume_attempts_count{message_type="events",queue="events"} 20
acemq_consume_attempts_sum{message_type="events",queue="events"} 20
...
# TYPE acemq_publish_duration_max gauge
acemq_publish_duration_max{exchange="",outcome="confirmed",routing_key="events"} 0.002583334
```

The metrics are cut short here; the full response also has the minimum, maximum
and total time for each outcome.

**`acked 16`, `rejected 4`** is the handler's decisions counted by outcome. Every
fifth event is `bad` and rejected. A dashboard that shows only throughput would
show twenty and look healthy.

**`acemq_consume_attempts_sum` equals its count**, so every message was handled
on its first attempt. A sum that grows faster than the count is the earliest sign
of a dependency that keeps failing.

**`"blocked":false`** is the broker's own flow control. When the broker raises
a memory or disk alarm it blocks publishers, and health reports that with the
broker's reason instead of hanging — see
[advanced/07-health-when-the-broker-blocks](../07-health-when-the-broker-blocks).

**`/acemq-info`** names the library version and what the transport supports,
which is the first thing to check when two services disagree about behaviour.

## Bound to loopback on purpose

The server listens on `127.0.0.1`. Nothing here is authenticated, and health
says which dependencies are down while metrics say how much traffic there is —
more than an anonymous caller should learn about a service. Put it behind
whatever protects the rest of your internal endpoints.

## In the documentation

- [Tutorial: seeing what happens](https://acemq-company.github.io/acemq-go-amqp/tutorial-observability.html)
- [Metrics, tracing and health](https://acemq-company.github.io/acemq-go-amqp/observability.html)
