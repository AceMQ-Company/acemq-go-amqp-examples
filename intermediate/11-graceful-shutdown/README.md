# intermediate/11 — graceful shutdown

What happens to the message being handled when the process is told to stop, and
what `Close` does when a handler will not stop in time.

## What it shows

- **`Consumer.Close` waits for the handler that is running**, so the message in
  hand is acknowledged rather than redelivered to somebody else.
- **It does not run what was prefetched but never started.** Those go back to
  the queue, so the drain is proportional to `Concurrency`, not `Prefetch`.
- **It is bounded.** `DrainTimeout`, twenty seconds by default
  (`acemq.DefaultDrainTimeout`), and at the bound it returns an error matching
  `acemq.ErrDrainTimeout` with `Stranded` counting the handlers still running —
  even when a handler ignores its context.
- **A handler that gives up at the bound still dead-letters.** The rejection is
  filed in `{queue}.dlq` with its reason, on a queue with no broker-side
  dead-letter exchange behind it.

The example checks each of these and exits non-zero if one stops holding.

## Running it

```bash
docker compose up -d
go run ./intermediate/11-graceful-shutdown
```

## What to look for

```
Close waits up to acemq.DefaultDrainTimeout = 20s for running handlers
enough time Close in  144ms  stranded=0  ran=1 finished=1  queue=9 dlq=0
prefetch 5  Close in  146ms  stranded=0  ran=1 finished=1  queue=9 dlq=0
stuck       Close in 1005ms  stranded=1  ran=1 finished=0  queue=10 dlq=0
gives up    Close in  502ms  stranded=1  ran=1 finished=0  queue=9 dlq=1
every claim held
```

Each run puts ten orders on a queue, starts a consumer, and calls `Close` half
way through the first 300 ms message.

**`enough time`** is the shape a service wants. Close returned as soon as the
message in hand was finished, about 150 ms later, and the other nine were never
taken.

**`prefetch 5`** looks the same, and that is the point. The broker had handed
this process five messages; Close stopped delivery, ran the one that had a
handler, and put the four that did not back on the queue, flagged redelivered,
for whoever starts next. Before 0.9.2 it ran all five, so the grace period had to
cover prefetch × handler time. Now it covers the handlers actually running.

**`stuck`** is a handler that never looks at its context — a blocking call with
no context, a loop that never checks. With `DrainTimeout(500ms)` Close waited
the bound, cancelled the handlers' context, gave them the library's half-second
to settle, released the channel and returned. The message was never
acknowledged, so the broker put it back: ten on the queue, nothing lost, nothing
dead-lettered.

**`gives up`** watches its context and rejects when the bound cancels it, on a
consumer with `RetryWith(acemq.NoRetry())`. The rejection lands in
`{queue}.dlq`. Before 0.9.2 that publish was refused on the cancelled context and
the delivery rejected without requeue — on a queue like this one, with no
`x-dead-letter-exchange`, the message was gone.

## Reading the result of `Close`

```go
consumer, err := acemq.Consume(handlers, mq, "orders", handle,
	acemq.DrainTimeout(15*time.Second))

// ...

if err := consumer.Close(); errors.Is(err, acemq.ErrDrainTimeout) {
	var timeout *acemq.DrainTimeoutError
	errors.As(err, &timeout)
	log.Printf("%d handler(s) on %s cut off at shutdown; their messages will be redelivered",
		timeout.Stranded, timeout.Queue)
}
```

A drain timeout is the signal worth logging and alerting on. It says the bound
is shorter than the work in hand, or a handler is stuck, and both are worth
knowing before they turn into a redelivery spike nobody can explain.
`DrainTimeout(0)` waits without a bound, the behaviour before 0.9.2.

Set the bound **comfortably under** whatever kills the process —
`terminationGracePeriodSeconds` (thirty by default), `TimeoutStopSec`,
`docker stop -t` (ten by default) — so the drain ends in your own log line
rather than in SIGKILL. `Conn.Close` and `ConsumerGroup.Close` close their
consumers side by side, so the whole shutdown takes one bound, not one per
consumer.

## Two contexts, not one

```go
signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

handlers, release := context.WithCancel(context.Background())
defer release()
consumer, err := acemq.Consume(handlers, mq, "orders", handle)

<-signals.Done()
stop() // a second Ctrl-C now kills the process
err = consumer.Close()
```

The handlers must **not** be given the context SIGTERM cancels. If they were,
the signal would cancel every handler the moment it arrived — the abrupt stop
this is trying to avoid. `Close` cancels the handlers' context itself, at the
bound and not before.

## What redelivery costs you

Nothing, if the handler is idempotent. Everything, if it charges a card. A
graceful shutdown reduces duplicates; it does not eliminate them — a stranded
handler's message is redelivered, a handler that gives up late may find its
dead letter filed and its acknowledgement refused by the closed channel, and a
power cut has no SIGTERM. See
[intermediate/02-idempotent-consumer](../02-idempotent-consumer).

The library's own account of all of this is
[docs/lifecycle.md](https://github.com/AceMQ-Company/acemq-go-amqp/blob/main/docs/lifecycle.md).
