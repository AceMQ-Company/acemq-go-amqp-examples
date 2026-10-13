# advanced/01 — surviving a broker that goes away

A publisher and a consumer run for forty-five seconds while you restart the
broker underneath them. Publishes fail loudly while the broker is away instead
of blocking, the connection reports each recovery attempt, and once the broker
is back the topology is redeclared and the consumer carries on by itself.

## What it shows

- **`rabbitmq.Dial` with recovery settings**: how long to wait between attempts,
  and an `OnRecovery` callback that sees every step.
- **A publish that fails rather than hangs** while there is no connection.
- **A consumer that reattaches** after the connection comes back, without the
  code that started it doing anything.

## Running it

This is the one example that needs you. Start it:

```bash
docker compose up -d
go run ./advanced/01-connection-recovery
```

and while it runs, in another terminal:

```bash
docker compose restart broker
```

`ACEMQ_EXAMPLE_SECONDS` shortens the run; CI sets it, because nobody is there to
restart anything.

## What to look for

```
the transport recovers by itself: true
publishing one message a second — restart the broker and watch
  docker compose restart broker
published 10, consumed 10
connection: connection lost (attempt 0): Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
publish 13 failed: acemq: cannot publish message 7a1acb92-716e-439e-a768-5ffc5ce3a2cc to exchange "" with key "ticks": Exception (504) Reason: "channel/connection is not open"
connection: connection retrying (attempt 1)
connection: connection retrying (attempt 1): acemq: cannot reach the broker: Exception (501) Reason: "read tcp [::1]:59939->[::1]:5793: read: connection reset by peer"
publish 14 failed: acemq: cannot publish message 5765e375-5a98-4e1a-a261-82fc47681c25 to exchange "" with key "ticks": Exception (504) Reason: "channel/connection is not open"
connection: connection retrying (attempt 2)
connection: connection retrying (attempt 2): acemq: cannot reach the broker: Exception (501) Reason: "read tcp [::1]:59940->[::1]:5793: read: connection reset by peer"
publish 15 failed: acemq: cannot publish message 39f10fe9-fb96-401f-ad09-a7adbafd42a0 to exchange "" with key "ticks": Exception (504) Reason: "channel/connection is not open"
publish 16 failed: acemq: cannot publish message c3774830-248e-4607-aec7-9b4b1eb8289a to exchange "" with key "ticks": Exception (504) Reason: "channel/connection is not open"
connection: connection retrying (attempt 3)
connection: connection recovered (attempt 3)
published 20, consumed 16
published 30, consumed 26
published 40, consumed 36
finished: consumed 36 messages
```

This run was shortened to forty seconds and its broker listened on 5793; yours
will say 5672, and the attempt numbers and the publishes that fail depend on how
long the restart takes.

**`connection lost`, then `retrying`, then `recovered`** come from the
`OnRecovery` callback. Recovery that nobody can see is only half an improvement
on dying quietly, so log it, count it and alert on it.

**`publish 13 failed`** is the publisher telling the caller the truth. A client
that ignores a dropped connection blocks here for ever.

**`consumed 36` out of 40 published** is every message that was published
successfully: 40 minus the four that failed. The consumer missed nothing that
reached the broker, and nobody restarted it.

## The failure this prevents

Without recovery a dropped connection is the quietest failure there is. The
delivery channel closes, the consumer goroutine ends, the object still looks
alive, and the service consumes nothing while saying nothing.

## What recovery does not do

It does not resend the publishes that failed. Those errors went back to the
caller, and the caller decides: retry, record it in an
[outbox](../../intermediate/03-outbox), or fail the request.

## In the documentation

- [Retries, redelivery and shutdown: when the connection drops](https://acemq-company.github.io/acemq-go-amqp/reliability.html#when-the-connection-drops)
- [The lifecycle of a service](https://acemq-company.github.io/acemq-go-amqp/lifecycle.html)
