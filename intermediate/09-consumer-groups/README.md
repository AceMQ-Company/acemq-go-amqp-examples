# intermediate/09 — consumer groups

Four slow invoices, handled four times faster by four consumers than by one
consumer running four handlers.

## What it shows

- **`patterns.NewConsumerGroup`**, sized by a number and stopped by one call.
- **Concurrency and prefetch are different numbers**, and the example measures
  the difference rather than describing it: how many messages were in hand at
  the same moment, and how long the batch took.
- **A prefetch belongs to a consumer**, not to a process — which is the whole of
  what a group buys that `acemq.Concurrency` does not.

## Running it

```bash
docker compose up -d
go run ./intermediate/09-consumer-groups
```

## What to look for

```
group of 4 on go-group-invoices

                              at once  seconds
group of 4, prefetch 1              4      0.5
1 consumer, concurrency 4           1      2.0
```

**Both rows are running four handlers.** The difference is how many messages the
broker will hand over before it hears back. One consumer with
`acemq.Concurrency(4)` and `acemq.Prefetch(1)` holds one message, so three of its
four handlers sit idle with nothing to work on. Four consumers have four
prefetches, so all four messages are out at once and the batch takes as long as
one invoice rather than four.

The example asserts it both ways: it fails if the group does not reach four in
hand, and it fails if the single consumer ever reaches two.

## Concurrency, or a group?

**Concurrency** is how many handlers one consumer runs at once. **Prefetch** is
how many messages the broker may give that consumer before it acknowledges any.
Raise the prefetch first: `acemq.Concurrency(8)` with `acemq.Prefetch(32)` on one
consumer is cheaper than eight consumers — one channel, one set of broker-side
bookkeeping — and is the right answer when the handlers spend their time waiting
on something else.

Reach for a group when:

- the handlers are slow enough that one channel's prefetch is what is holding
  things up, or
- a fair share **across processes** matters. The broker round-robins between
  consumers, so four here compete evenly with four in another instance, where one
  consumer with four handlers would get a quarter of what four consumers get.

| Prefetch | Use when |
| --- | --- |
| 1 | Handlers are slow or uneven. Fair distribution matters more than round trips. |
| 10–50 | Handlers are fast and similar. Round-trip cost starts to dominate. |
| Unbounded | Never. One consumer takes the queue and memory grows without limit. |

A queue with idle consumers and a growing backlog is almost always a prefetch
problem, not a scaling problem.

## One call to stop them

`group.Close()` closes every member and waits for each one's running handlers,
so a message being worked on is finished and acknowledged rather than handed to
somebody else. Every member is closed even if one fails, and the errors come
back joined: leaving three running after a shutdown the caller believes happened
is worse than the failure that started it. Four consumers started by hand are
four things to remember to close.

If a member cannot start, those already started are closed before
`NewConsumerGroup` returns the error — a half-started group would hold messages
nothing is going to handle.

## What the broker sees

Each member is tagged `acemq-{queue}-{n}`, numbered from one, so the management
interface shows four named rows on the queue rather than four identical ones, and
"which consumer is holding that message" has an answer.

## What Go does not have

Java's group can be resized while it runs (`scaleTo`). Go's is fixed at the
size it was started with — start a new group and close the old one to change it.

Draining against a deadline it does have: `acemq.DrainTimeout` passed to
`NewConsumerGroup` bounds `Close`, twenty seconds by default, and the members
close side by side, so the group takes one bound rather than one per member.
[intermediate/11-graceful-shutdown](../11-graceful-shutdown) shows what happens
at that bound.

## A group is not a partition

Every member reads the same queue and the broker decides who gets what, so two
messages about the same order can be handled at the same moment by different
consumers. If that matters, the queue is the wrong shape, and the answer is a
queue per key — not a group.
