# basic/06 — streams

Ten orders written once and read four times: a projection stopped half way and
carried on, a retry the library refuses, and a reader at the end that still
finds all ten.

## What it shows

- **Retention is the argument that matters.** `DeclareStream` takes a
  `StreamRetention`, and the zero value is legal and almost always wrong.
- **Checkpointing is yours.** The broker keeps nobody's position. The reader
  records the `x-stream-offset` of the last message it handled, and resumes from
  **one past it**.
- **`acemq.Retry` is refused on a stream**, and the message parked with the
  reason, rather than appended to the log a second time.
- **Reading did not consume.** Three readers acknowledged everything, and a
  fourth attached afterwards reads the same ten in the same order.

## Running it

```bash
docker compose up -d
go run ./basic/06-streams
```

## What to look for

```
wrote      10 orders to go-stream-orders
read       5, then the reader stopped
checkpoint offset 4, from the x-stream-offset on the last one handled
resumed    [o-5 o-6 o-7 o-8 o-9]
retry      refused: 1 parked on go-stream-orders.parked
parked     o-3, with the handler's reason kept: "the ledger is down"
a new reader still saw all 10
```

**`checkpoint offset 4`** is an offset, not a count. Offsets on a fresh stream
start at zero and never move: message four is message four for every reader, for
as long as retention keeps it. `patterns.StreamOffsetOf(m.Envelope)` reads it off
the delivery, and `patterns.FromOffset(checkpoint + 1)` is how the next run picks
up exactly where this one stopped — no gap, and nothing handled twice.

**`a new reader still saw all 10`** is the line the example would fail on if a
stream behaved like a queue. It is also the proof that the refused retry did not
append: the auditor waits half a second past the tenth message for an eleventh,
and there is none.

## Say where to start

A reader not told where to start reads from `FromNext()`. That is right for a
consumer joining a live system, and silently wrong for a projection being
built, which would skip its own history and look perfectly healthy while being
empty. State the position; do not inherit it.

| | starts at |
| --- | --- |
| `FromFirst()` | the oldest message retention has not yet discarded — not necessarily the first ever written |
| `FromNext()` | the next message published; the default |
| `FromLast()` | the last chunk, which is "the recent past" rather than an exact count |
| `FromOffset(n)` | exactly `n`, which is what a checkpoint gives back |
| `FromTimestamp(t)` | the first message at or after `t` — "everything since the incident started" |

## Stopping without losing your place

The first reader stops after five the way the library's documentation says to:
settle whatever else arrives with `acemq.Accept()` but do not advance the
checkpoint, then close. Whatever the broker had already pushed past the fifth
message is accepted and not counted, so the checkpoint says where the work
stopped whatever the prefetch was. Acknowledging a stream message removes
nothing, so accepting one you did not handle loses nothing either.

## Retry is refused, and why

On a queue, `acemq.Retry` republishes the message onto the queue it came from. On
a stream that appends a second copy to the log — for every reader to see now,
and on every replay afterwards — so `patterns.ReadStream` will not do it. The
message is parked on `{stream}.parked` as a copy, with a reason that says the
retry was refused, names the two honest alternatives, and keeps the handler's
own error at the end. The original is still in the stream, because nothing is
ever removed from one.

The two alternatives, both one line in the handler:

- **Park it deliberately**: `return acemq.Park(err)`. Somebody looks at the
  parked queue later.
- **Checkpoint and move on**: record the offset, `return acemq.Accept()`, and
  count the gap — nothing else will.

Java draws this line in the type system: its stream consumer is a separate type
that does not offer the outcomes a stream cannot honour. Go's handler signature
is shared between queues and streams, so the line is drawn when the verb is
used instead.

## Exactly once, if you want it

Save the checkpoint **in the same transaction as the projection's own writes**.
Anywhere else and the pair is at-least-once, which is fine when the handler is
idempotent and quietly wrong when it is not. See
[intermediate/02-idempotent-consumer](../../intermediate/02-idempotent-consumer).

## Retention, and why `SegmentBytes` is in the example

```go
patterns.StreamRetention{
	MaxAge:       time.Hour,
	MaxBytes:     20 << 20,
	SegmentBytes: 1 << 20,
}
```

A stream is stored as a series of segment files and retention discards a whole
file at a time, so nothing at all goes until a whole segment can. With
RabbitMQ's 500 MB default, a stream told to keep an hour keeps everything until
it has half a gigabyte to drop. And a stream with no retention grows until the
disk is full, which is not a stream problem but a broker-wide alarm that stops
every publisher on the node.

## No plugin needed

Stream queues are part of RabbitMQ from 3.9 onwards, and this is an ordinary AMQP
consumer with an `x-stream-offset` argument. The `rabbitmq_stream` plugin serves
a separate, faster stream protocol on port 5552 that this library does not use,
so the plain broker in `compose.yaml` is enough — and CI runs it against 3.13 as
well as 4.x.

## A stream is declared, not converted

`x-queue-type` is part of a queue's identity, so a name that already exists as a
classic or quorum queue cannot be re-declared as a stream: the answer is
`PRECONDITION_FAILED`, and it does not mention streams. This example deletes the
name before declaring it for that reason — and because a leftover stream would
hand this run everything every previous run wrote.

## When not to use one

Streams give up the failure-handling this library builds on *moving* a message:
retry ladders, dead-letter queues, replay. If a bad message should end up
somewhere a person can deal with it, you want a queue. Streams are for logs that
get replayed — projections, audit trails, event sourcing.
