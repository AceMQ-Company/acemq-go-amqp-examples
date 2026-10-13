# basic/03 — declaring a topology, and noticing when the broker disagrees

An exchange, two queues and two bindings declared as one value that can be
printed before it is applied, applied twice without complaint, and then checked
against the broker — which catches a queue whose dead-letter target has changed,
and a binding to a queue nothing declares before the broker ever sees it.

## What it shows

- **A topology you can read first.** Printing it lists every declaration, so a
  change to a broker is something somebody can review.
- **`Apply` is repeatable.** Applying the same topology again changes nothing.
- **`Check` reports drift**: a difference between what the code says and what
  the broker has.
- **`Validate` catches what the broker cannot**, such as a binding to a queue
  this topology never declares.

## Running it

```bash
docker compose up -d
go run ./basic/03-topology-and-drift
```

## What to look for

```
this is what will be declared:
Topology: 1 exchanges, 2 queues, 2 bindings
  declare exchange orders-events (topic)
  declare queue shipping-orders (durable, quorum, x-dead-letter-exchange=shipping-dead)
  declare queue shipping-dead (durable, quorum)
  declare binding shipping-orders (from orders-events on order.placed)
  declare binding shipping-orders (from orders-events on order.cancelled)
applied
applied again, unchanged, without complaint
drift against the broker: 0 differences
drift: queue shipping-orders: the broker refused the declaration: Exception (406) Reason: "PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange' for queue 'shipping-orders' in vhost '/': received 'somewhere-else' but current is 'shipping-dead'"
caught before the broker saw it: acemq: binding orders-events -> a-queue-nobody-declared (#) names queue "a-queue-nobody-declared", which this topology does not declare
routed end to end: o-1
```

**`0 differences`, then one.** The second topology asks for the same queue with
its dead letters pointed somewhere else. A queue's arguments are fixed when it is
created, so the broker refuses the declaration with `PRECONDITION_FAILED`.
`Check` declares each queue on a channel of its own — a refused declaration kills
its channel — and turns the refusal into a report naming the queue and the
argument. A queue that does not exist yet is passed over rather than created, so
`Check` is safe against a broker you are only inspecting.

**This is the failure that otherwise shows up as missing messages.** A service
deployed with the new topology would carry on running, and its dead letters
would go to `shipping-dead` while its owners looked in `somewhere-else`. Running
`Check` at startup, or in a deployment step, is how you find out first.

**`caught before the broker saw it`** is a mistake no broker can catch. The
binding would work wherever `a-queue-nobody-declared` already existed, and fail
on a fresh environment for a reason nobody could see. `Validate` needs no
connection, so it can run in a unit test.

**`routed end to end`** is the only proof that matters: a message published to
`orders-events` with the key `order.placed` arrived on `shipping-orders`.

## Changing a queue's arguments

Drift on a queue argument cannot be fixed by declaring harder. The queue has to
be replaced: create one under a new name, move the consumers, drain the old one
and delete it. `Check` tells you that this is the situation before the
deployment does.

## In the documentation

- [Exchanges, queues and bindings](https://acemq-company.github.io/acemq-go-amqp/topology.html)
