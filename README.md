# AceMQ for Go — examples

[![ci](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/attribution-guard.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.23%2B-00ADD8)](#requirements)

Runnable examples for [AceMQ for Go](https://github.com/AceMQ-Company/acemq-go-amqp).
Each one is a single `main.go`: open a directory and the whole example is in
front of you, with no shared helpers to trace. The newer ones have a `README.md`
beside them for the part that is worth explaining rather than narrating.

They depend on the **released** version, so they resolve exactly what the
documentation tells you to depend on — and an example that stops compiling
against a release is a red build here rather than a surprise for whoever copies
it.

## Running one

```bash
docker compose up -d
go run ./basic/01-publish-and-consume
```

Point them somewhere else with `ACEMQ_URL`:

```bash
ACEMQ_URL=amqps://guest:guest@broker:5671/ go run ./basic/01-publish-and-consume
```

## What is here

### basic

| | |
|---|---|
| [01-publish-and-consume](basic/01-publish-and-consume) | A durable queue, a confirmed publish, and a consumer that says what it did. |
| [02-retries-and-dead-letters](basic/02-retries-and-dead-letters) | The attempt counter moving, a message giving up, and an error marked fatal skipping the wait. |
| [03-topology-and-drift](basic/03-topology-and-drift) | Declaring a topology, printing it before applying it, and catching a broker that disagrees. |
| [04-replay](basic/04-replay) | Dead-lettered invoices put back one tenant at a time, and the rest afterwards. |
| [05-codecs](basic/05-codecs) | Four formats on one queue read by one consumer, and the two codecs that interpret nothing. |
| [06-streams](basic/06-streams) | A log read four times: a projection stopped and resumed from its own checkpoint, a retry refused, and nothing consumed. |

### intermediate

| | |
|---|---|
| [01-request-reply](intermediate/01-request-reply) | Ten concurrent requests, each getting its own answer, and a responder failure reaching the caller. |
| [02-idempotent-consumer](intermediate/02-idempotent-consumer) | One logical message delivered four times and charged once. |
| [03-outbox](intermediate/03-outbox) | Recording a message beside the work, and a relay publishing what was committed. |
| [04-saga](intermediate/04-saga) | Three services undone in reverse, and a compensation that fails and leaves a row for a person. |
| [05-scheduling](intermediate/05-scheduling) | Reminders delivered later through a ladder of queues, with the accuracy it costs shown rather than claimed. |
| [06-interceptors](intermediate/06-interceptors) | One tenancy rule on the connection: a publish stopped, a card number redacted, a delivery refused. |
| [07-binary-codecs](intermediate/07-binary-codecs) | Avro through a schema registry and protobuf, and the framing that keeps them apart. |
| [08-a-declared-pipeline](intermediate/08-a-declared-pipeline) | Two orders through three steps with a queue between each, and the one whose run ends early. |
| [09-consumer-groups](intermediate/09-consumer-groups) | Four consumers against one consumer running four handlers, and the prefetch that makes the difference. |
| [10-schema-evolution](intermediate/10-schema-evolution) | Two services on two versions of one Avro schema reading each other, and the two ways to get it wrong. |
| [11-graceful-shutdown](intermediate/11-graceful-shutdown) | Close finishing the work in hand, requeueing what was prefetched, and its DrainTimeout bound cutting off a stuck handler without losing a message. |

### advanced

| | |
|---|---|
| [01-connection-recovery](advanced/01-connection-recovery) | Restart the broker underneath it and watch the consumer come back. |
| [02-metrics-and-health](advanced/02-metrics-and-health) | `/acemq-metrics`, `/acemq-health` and `/acemq-info`, on the same paths as Java and .NET. |
| [03-claim-check](advanced/03-claim-check) | Two reports one byte apart, on either side of the threshold, and what each puts on the wire. |
| [04-encrypting-payloads](advanced/04-encrypting-payloads) | A key rotated without an outage, and what one altered byte does. |
| [05-development-certificates](advanced/05-development-certificates) | TLS on a laptop, and a development certificate refused however trust is configured. |
| [06-tracing](advanced/06-tracing) | A consumer span that is a child of its publish across the broker, proved by a message that carries no trace. |
| [07-health-when-the-broker-blocks](advanced/07-health-when-the-broker-blocks) | A real memory alarm, and health answering `up` in four microseconds with the broker's reason — beside a round trip on the same connection that never answers at all. |
| [08-a-standing-load-something-else-can-watch](advanced/08-a-standing-load-something-else-can-watch) | A load that does not finish, printing one JSON reading per second — so a fault drill can read what the client saw rather than what the broker did. |

### apps

Several patterns at once, which is where a library's features stop being
demonstrated one at a time and start having to agree with each other.

| | |
|---|---|
| [01-order-fulfilment](apps/01-order-fulfilment) | Five services, one broker, no shared database: an outbox at the edge, a charge that refuses a duplicate, a flaky warehouse retried, and a timeline rebuilt from one correlation id. Checked against the Java services on the same broker. |

## The two that need a broker of their own

[advanced/05-development-certificates](advanced/05-development-certificates)
needs a TLS listener holding certificates generated on this machine, so it comes
with a compose profile and two commands:

```bash
go run github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@v0.9.3 --out certs --broker localhost
chmod 644 certs/server.key
docker compose --profile tls up -d
```

[advanced/07-health-when-the-broker-blocks](advanced/07-health-when-the-broker-blocks)
puts a broker into a genuine memory alarm, and an alarm is a property of the
node rather than of one connection — every publisher on that broker is refused
for as long as it lasts. On the shared broker it would fail whichever other
example happened to be running, so it gets one of its own:

```bash
docker compose --profile alarm up -d
```

It puts the watermark back when it is done, including when it fails.

## The one worth doing by hand

[advanced/01-connection-recovery](advanced/01-connection-recovery) is the only
one that needs you. Start it, and while it runs:

```bash
docker compose restart broker
```

The publisher will report failures while the broker is away — rather than
blocking for ever, which is what a client that ignores this looks like — and
then the connection comes back, the topology is redeclared and the consumer
reattaches. Without that, a dropped connection is the quietest failure there is:
the delivery channel closes, the consumer goroutine ends, the object still looks
alive, and the service consumes nothing while saying nothing.

## Requirements

Go 1.23 or later, and Docker. RabbitMQ **3.13 or 4.x**, the range the library
supports; `compose.yaml` brings up 4.x and CI runs every example against both.

Two exceptions need **Go 1.25**, and each is a module of its own for that
reason, so neither asks anything of the other twenty-four — which still build on
1.23, checked with a 1.23 toolchain on every push. `advanced/06-tracing` imports
`telemetry/otel`, which raised its floor to 1.25 when it took OpenTelemetry 1.46,
so anyone tracing already needs it. `apps/01-order-fulfilment` uses the pure-Go
SQLite driver, which needs it too.

## How these stay honest

CI compiles and **runs every one of them against a real broker**, on every push
and once a week. Examples rot: the library moves on, the example does not, and
a newcomer's first experience is a build error. A weekly failure here is how we
find out that a released version broke something the documentation still claims.

The workflow finds examples rather than listing them, so one added without
touching CI is still run — and it fails if it finds fewer than it expects, since
a `find` that matches nothing would otherwise pass without running anything.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
