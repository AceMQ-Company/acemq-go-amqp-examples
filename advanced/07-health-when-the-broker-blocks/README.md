# advanced/07 — health when the broker blocks

A broker under a real memory alarm, a connection it has stopped reading, and
`Health` answering **up** in four microseconds with the broker's own reason.

## What it shows

- **`mq.Health(ctx)` on a genuinely blocked connection** — `up`, with
  `blocked: true` and the reason RabbitMQ gave.
- **No round trip.** The report carries no `roundTripMillis` at all while
  blocked, because nothing was asked.
- **A publish refused rather than parked**, carrying the same reason.
- **`AggregateHealth` keeping the reason** on the line a readiness endpoint
  serves.
- **A control**, which is what makes the rest mean anything: a queue declare on
  that same connection that has still not come back after five seconds, and
  completes the moment the alarm clears.

## Running it

```bash
docker compose --profile alarm up -d
go run ./advanced/07-health-when-the-broker-blocks
```

## What it prints

```
  before    status up in 4.706ms
             parts map[blocked:false consumers:0 roundTripMillis:4]

dropping the memory high watermark to nothing, which puts the node in
the state a production broker reaches under memory pressure
  publish    0 went unconfirmed, which is the broker having stopped reading mid-publish
  blocked    the broker sent connection.blocked: "low on memory"
  publish    refused in 98µs: acemq: publishing is paused because the broker blocked this connection (low on memory)

  during    status up in 4µs
             parts map[blocked:true blockedReason:low on memory consumers:0]
             the broker has blocked this connection; publishing is paused: low on memory
  aggregate  up: broker: the broker has blocked this connection; publishing is paused: low on memory

  control    a queue declare on this same connection has still not come
             back after 5s.

putting the memory high watermark back to 0.4
  released   the same declare completed once the broker started reading: <nil>

  after     status up in 753µs
             parts map[blocked:false consumers:0 roundTripMillis:0]
```

Four microseconds against 4.7 milliseconds, and the difference is not that the
blocked check is quicker. It is that the blocked check does not ask.

## Why up, and not down or degraded

**Not down**, because a blocked connection is the broker protecting itself from a
memory or disk alarm. An application that fails its own readiness check for it is
one an orchestrator restarts into the same blocked broker, having thrown away
whatever it was holding — and a fleet doing that together stops draining the
queues at the moment the broker most needs them drained. Consumers are
unaffected by an alarm; only publishers are paused. The instance that stays up
is the one that can help.

**Not degraded** either. Degraded is for this instance being worse at its job
than it should be. A block is the broker's state, identical across every
replica, so an alert that fires for all of them at once is one no deployment can
act on. The fact worth alerting on is in `blockedReason`, and it is the broker's
own words: `low on memory` is actionable, `the publish failed` is not.

## Why the round trip is skipped rather than shortened

`Health` normally declares a temporary queue, which is the cheapest thing AMQP
offers that proves the connection actually works. On a blocked connection that
declaration is **not refused — it goes unanswered**, because RabbitMQ has stopped
reading the socket. A check that probes first spends its entire deadline
discovering what the connection already knew, and then reports a timeout rather
than the reason.

That is the shape this example exists to hold in place, and the control is what
proves it is still the shape: the declare it starts while blocked is still
waiting after five seconds, and finishes as soon as the watermark goes back.
Without it, `up in 4µs` would read exactly the same against a broker that was
never blocked at all.

## The alarm, and putting it back

```
rabbitmqctl set_vm_memory_high_watermark 0
```

is a real alarm on a real node — the state a production broker reaches under
memory pressure, not a flag set on the client. The example raises it through
`docker exec` on its own broker's container, because an alarm is set on the node
and there is no AMQP for it.

It is put back, to the image's default of `0.4`, in a `defer` that runs however
the example ends, including when an assertion fails. Two reasons:

- Left down, every publisher on that broker is refused until somebody notices.
- **Closing a blocked connection waits on a broker that is not reading**, so the
  restore is deferred *after* the close and therefore runs *before* it.

## Its own broker

An alarm is a property of the node. Every publisher on the broker that raises one
stops being served for as long as it lasts, so this example runs against
`alarm-broker` from `compose.yaml` rather than the shared one — otherwise it
would fail whichever other example happened to be publishing, and would look
innocent while doing it. CI gives it a container of its own for the same reason.

It reads `ACEMQ_ALARM_URL` rather than `ACEMQ_URL`, deliberately, so that
pointing the examples at another broker cannot accidentally point this one at a
broker shared with anything else.

## Related

- [advanced/02](../02-metrics-and-health) — the same facts over HTTP, on a
  broker that is not blocked
- [advanced/01](../01-connection-recovery) — the other way a broker goes away,
  and the one that does report down
