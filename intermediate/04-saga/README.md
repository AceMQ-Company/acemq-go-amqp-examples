# intermediate/04 — undoing work that spans three services

Three runs of the same order: one that works, one where the payment is refused,
and one where the undo itself fails.

## What it shows

- **Reverse-order compensation**, on a queue rather than in a variable — the
  ledger records what the broker actually saw.
- **A compensation that fails**, and what a saga does about it instead of
  returning an error.
- **A step with no `Undo`**, which is allowed and means what it says.

## Running it

```bash
docker compose up -d
go run ./intermediate/04-saga
```

## What to look for

```
SagaResult{place-order completed: reserve stock -> take payment -> book courier}
  the broker saw: [stock.reserved payment.taken courier.booked]

SagaResult{place-order failed at take payment, compensated [reserve stock]}
  the broker saw: [stock.reserved stock.released]

SagaResult{place-order failed at book courier, compensated [reserve stock take payment], UNRESOLVED [reserve stock]}
  the broker saw: [stock.reserved payment.taken payment.refunded]

unresolved: [reserve stock] — that is the row a person has to look at
```

**`payment.refunded` comes before the stock release, not after.** The
compensations run backwards, newest first, because the later steps are the ones
built on the earlier ones. Refunding after releasing the stock would be undoing
them in the order they were done, which is the order in which they depend on
each other.

**The third run is the one that matters.** The courier cannot be booked, the
payment is refunded, and then the warehouse refuses to release a reservation it
has already picked — a compensation *itself* fails. The saga does not return an
error and does not stop; it records the step in `Unresolved` and carries on with
the rest. Something is now half-undone and needs a person, and an error thrown
into a message handler is a poor way to tell anyone that: it gets retried, then
dead-lettered, and the fact that a payment was refunded but the stock was not
released ends up in a queue nobody reads.

`Unresolved` is a value the calling code can act on. Everything else a saga
reports is recoverable by construction; these are real-world effects that
happened, were meant to be undone, and were not. No retry will resolve them.

**The second run publishes nothing about a payment.** The payment never
happened, so there is nothing to refund — a compensation runs only for a step
that completed.

## Why a result and not an error

A failed saga is not an exceptional condition to a caller that has to decide
what happens next. `Run` returns a `SagaResult` with `Complete`, `Compensated`
and `HasUnresolved` on it, and the interesting part is the last one rather than
the failure.

A step that panics is treated as a step that failed, for the same reason: the
compensation for everything before it still has to run, and unwinding past the
compensation is the one outcome this pattern exists to prevent.

## What a saga is not

It is not a distributed transaction and does not pretend to be one. Between a
step succeeding and its compensation running, the world has seen the step: a
customer whose card was charged and then refunded got two emails from their
bank. A saga makes the **end state** correct, not the middle, and choosing it
means deciding that is acceptable for this workflow.

It also has no wire contract. Nothing is published by the saga itself and no
header is set — the events on the ledger queue are published by the steps,
because that is what those steps do. A Go saga and a Java one are the same idea
rather than two ends of one conversation. What matches across the libraries is
the behaviour: reverse-order compensation, a missing compensation skipped, a
failing compensation collected rather than allowed to stop the rest.

## Write `Undo` first

`Undo` is optional, and a `nil` `Undo` on a step that ran is skipped rather than
treated as an error. Booking the courier here is the last step, so if it fails
there is nothing of it to undo. On a step that changed something it would be a
bug, and nothing in the library can tell the two apart — which is the argument
for writing the compensation before the action.
