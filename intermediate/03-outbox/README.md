# intermediate/03 — writing the message in the same transaction as the work

A service that writes to its database and then publishes has two steps that can
fail separately, and the gap between them is where messages are lost or
invented. This example records an order event in an outbox instead of
publishing it, and a relay publishes what was recorded and removes it only once
the broker confirms.

## What it shows

- **`patterns.Record`** builds the message, and the store holds it. Nothing is
  published at that point.
- **Recording twice is harmless.** A caller retrying its own transaction does
  not turn one message into two.
- **`patterns.NewOutboxRelay`** polls the store, publishes, and deletes a record
  only after the broker's confirm.

## Running it

```bash
docker compose up -d
go run ./intermediate/03-outbox
```

## What to look for

```
recorded ff3e27ce-1aa6-4543-8206-ac4b6c8f3807 in the outbox; nothing has been published yet
recorded again; the outbox holds 1 message(s)
the relay is running
published by the relay and consumed: o-1
the outbox now holds 0 message(s)

a record is removed only after the broker confirms it, so a crash
in between republishes rather than loses — which is why anything
consuming this needs to be idempotent. See example 02.
```

**`the outbox holds 1 message(s)` after two adds.** The record has an id, and
adding the same id again is not an error and not a second row.

**`published by the relay and consumed: o-1`** is the relay, not the code that
did the work, putting the message on the broker. The work and the record commit
together; publishing happens afterwards, and can be retried as many times as it
takes.

**`the outbox now holds 0 message(s)`** only after the confirm. If the process
died between the publish and the delete, the next relay would publish the record
again. An outbox turns "maybe lost" into "maybe twice", and the consumer handles
"twice" — see [intermediate/02-idempotent-consumer](../02-idempotent-consumer).

## The in-memory store proves nothing

`patterns.NewInMemoryOutboxStore` lets the example run without a database, and
it has none of the property the pattern exists for: nothing shares a
transaction with your work, so a crash between the work committing and the
record being written loses the message exactly as publishing directly would.

For anything real use `patterns.NewSQLOutboxStore` with your own `*sql.Tx`. It
takes anything with `ExecContext`, which is what lets the record commit in the
same transaction as the rows it describes.

## In the documentation

- [Tutorial: never processing twice](https://acemq-company.github.io/acemq-go-amqp/tutorial-exactly-once.html)
- [Patterns: the outbox](https://acemq-company.github.io/acemq-go-amqp/patterns.html#the-outbox)
