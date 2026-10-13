# intermediate/02 — handling a message once, even when it arrives twice

One charge is published three times with the same message id, and its first
delivery fails on purpose and is retried, so the consumer receives it four times —
and `patterns.Idempotent` makes sure the card is charged exactly once.

## What it shows

- **Redelivery is normal.** A producer that publishes at least once, and a retry
  after a failure, both mean the same message arrives again.
- **`patterns.Idempotent`** wraps a handler and remembers which message ids it
  has handled.
- **A failure is forgotten**, so the retry after it runs the work instead of
  being skipped as a duplicate.

## Running it

```bash
docker compose up -d
go run ./intermediate/02-idempotent-consumer
```

## What to look for

```
delivery 1: failing on purpose
delivery 2
charging c-1 for 4250 cents
delivery 3
delivery 4
delivered 4 times, charged 1 time(s)
```

**Four deliveries.** Three publishes of `charge-c-1`, plus one redelivery of the
copy that failed. That is what at-least-once looks like from the consumer's side.

**`charging` appears once.** Delivery 2 is the first to reach the guarded
handler, so it does the work and the id is recorded as done. Deliveries 3 and 4
carry the same id, so the guard accepts them without calling the handler. A
duplicate is accepted rather than rejected: the work was done, and dead-lettering
it would raise an alarm about something that went right.

**Delivery 1 failed and was not remembered.** If a failed message were recorded,
its retry would be skipped as a duplicate and the charge would never happen.
Forgetting on failure is the honest ordering, and it is why this is a guard
against duplicates rather than a promise of exactly-once.

## The key is the message id

The publisher sets it with `acemq.MessageID("charge-c-1")`. An id derived from
the business operation — this charge, this order — is what makes two publishes
of the same thing look the same. A random id per publish would make every copy
look new.

## Where the store lives

`patterns.NewInMemoryIdempotencyStore` is right for one worker and wrong the
moment there are two: each has its own memory and both believe they were first.
Use `patterns.NewSQLIdempotencyStore`, ideally in the same database as the work
and in the same transaction. A crash between the handler finishing and the
acknowledgement reaching the broker still produces one more delivery, and only
a record committed with the work closes that gap.

A redelivery that finds the id claimed but not yet finished — a handler still
running, or one that crashed holding the claim — is neither run nor accepted. It
is put back after a short delay without spending a retry attempt.

## In the documentation

- [Tutorial: never processing twice](https://acemq-company.github.io/acemq-go-amqp/tutorial-exactly-once.html)
- [Patterns: idempotency](https://acemq-company.github.io/acemq-go-amqp/patterns.html#idempotency)
- [Retries, redelivery and shutdown](https://acemq-company.github.io/acemq-go-amqp/reliability.html)
