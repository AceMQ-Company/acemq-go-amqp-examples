# intermediate/05 — delivering a message later

Three reminders: one already overdue, one in three seconds, one in five. No
scheduler process, no plugin, no cron.

## What it shows

- **A ladder of queues, each with a uniform time to live.** A message hops
  through them until it is due.
- **The accuracy that buys, and what it costs.**
- **The payload is encoded once and carried as bytes**, with its content type in
  a header that is put back on the message finally delivered.

## Running it

```bash
docker compose up -d
go run ./intermediate/05-scheduling
```

It takes about five seconds, because it is waiting for real delays.

## What to look for

```
scheduled 3, delivered 3, hops 6

asked for   arrived at
     past     0.0s   R-0
       3s     2.0s   R-1
       5s     4.0s   R-2

content type on arrival: application/json
```

**Read the two columns together.** A three-second delay lands at about two. A
message is delivered as soon as less than one second is left, because another
hop through the smallest rung would cost more than the accuracy it buys.

That is the trade this design makes, and it is stated rather than hidden:
delivery is accurate to about the smallest rung. Something that must fire at
09:00:00.000 wants a scheduler, not a message broker.

**`hops 6`** is the other number. R-0 was due already and took none; R-1 took two
and R-2 four. A one-minute delay costs one hop and a one-day delay costs
twenty-four — long delays are several broker round trips rather than one, which
is the honest cost of not requiring a plugin.

The example asserts a floor on the hop count rather than the exact six, because a
busy broker can add one. What the floor proves is that the messages went through
the ladder instead of being delivered on the spot — a scheduler that ignored
every delay would satisfy the rest of the checks.

## Why not a per-message time to live

Because a classic queue expires messages only from its **head**. Put a four-hour
message in and a one-minute message behind it, and the one-minute message is
delivered in four hours — and nothing reports it. The queue looks healthy, the
message is not lost, it is simply late by a factor nobody predicted.

It is the single most common way a home-made scheduler fails, and it fails in
production under mixed load rather than in testing under uniform load.

The ladder avoids it by giving every message in a rung the same delay, so the
head is always the message due soonest.

The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
properly and is a plugin — so it is not available everywhere, and a library that
silently required it would be a library that works on your laptop.

## The names are the contract

`acemq.schedule.{1h,10m,1m,10s,1s}`, `acemq.schedule.due` and the four headers a
scheduled message carries are shared with the Java, .NET, Python and Ruby
libraries. Two services scheduling on one broker declare the same queues, and a
rung declared with a different argument table is a `PRECONDITION_FAILED` for
whichever declares second. Nothing about it is a local decision.

`patterns.ScheduleTopology()` returns the whole thing, so a deployment that
applies its topology up front can include the scheduler's without starting a
consumer.

## Lifetimes

`NewScheduler` starts a consumer on the control queue, and this example does not
close it until the last reminder has arrived. A scheduler that has been closed is
a ladder nobody is watching: the messages sit in `acemq.schedule.due` until
something reads it again.

The example deletes its own reminder queues on the way out and leaves the rungs
alone. They are shared with every other service scheduling against that broker
and are not one example's to remove.

## A moment in the past

Delivered at once rather than refused. A renewal date that has already gone by is
a reminder that is late, not an error.
