# AceMQ for Go — examples

[![ci](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-go-amqp-examples/actions/workflows/attribution-guard.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.23%2B-00ADD8)](#requirements)

Runnable examples for [AceMQ for Go](https://github.com/AceMQ-Company/acemq-go-amqp).
Each one is a single `main.go`: open a directory and the whole example is in
front of you, with no shared helpers to trace.

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

### intermediate

| | |
|---|---|
| [01-request-reply](intermediate/01-request-reply) | Ten concurrent requests, each getting its own answer, and a responder failure reaching the caller. |
| [02-idempotent-consumer](intermediate/02-idempotent-consumer) | One logical message delivered four times and charged once. |
| [03-outbox](intermediate/03-outbox) | Recording a message beside the work, and a relay publishing what was committed. |

### advanced

| | |
|---|---|
| [01-connection-recovery](advanced/01-connection-recovery) | Restart the broker underneath it and watch the consumer come back. |
| [02-metrics-and-health](advanced/02-metrics-and-health) | `/acemq-metrics`, `/acemq-health` and `/acemq-info`, on the same paths as Java and .NET. |

## The one worth doing by hand

Example 07 is the only one that needs you. Start it, and while it runs:

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

Go 1.23 or later, and Docker.

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
