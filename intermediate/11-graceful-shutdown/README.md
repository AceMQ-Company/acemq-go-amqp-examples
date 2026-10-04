# intermediate/11 — graceful shutdown

What happens to the message being handled when the process is told to stop, and
how to put a bound on the wait.

## What it shows

- **`Consumer.Close` waits for in-flight handlers**, so the message in hand is
  acknowledged rather than redelivered to somebody else.
- **It finishes everything already prefetched**, not only the message being
  worked on — which is what the grace period actually has to cover.
- **It has no deadline of its own.** The bound is a timer and a context the
  handlers watch; when it fires they give their messages back, and nothing is
  lost.

## Running it

```bash
docker compose up -d
go run ./intermediate/11-graceful-shutdown
```

## What to look for

```
enough time grace=10s   drained=true  in  149ms  handled=1 gave back=0, 9 left on the queue
prefetch 5  grace=10s   drained=true  in 1358ms  handled=5 gave back=0, 5 left on the queue
not enough  grace=500ms drained=false in  501ms  handled=2 gave back=3, 8 left on the queue
```

Each run puts ten orders on a queue, starts a consumer, and asks it to stop half
way through the first 300 ms message.

**`enough time`** is the shape a service wants. Close returned as soon as the
message in hand was finished, about 150 ms later, and the other nine were never
taken.

**`prefetch 5`** is the line people do not expect. The broker had already handed
this process five messages, and Close finished all of them before returning —
1.36 s, not 150 ms. A message delivered into the process is work Close considers
in hand. Size the grace period for **prefetch × handler time ÷ concurrency**, or
lower the prefetch on consumers whose handlers are slow.

**`not enough`** is the same consumer with half a second. When the timer fired,
the handlers' context was cancelled: the one running gave its message back, so
did the ones queued behind it, and Close returned at 501 ms. Exactly which
messages finished depends on the clock; that `handled + left` is ten does not,
and the example asserts it.

## The bound comes from outside

Java's consumer has `drain(Duration)`, which waits with a deadline and says
whether it made it. Go's `Close` waits as long as the handlers take. The bound is
eight lines:

```go
func closeWithin(consumer *acemq.Consumer, grace time.Duration, giveUp context.CancelFunc) (bool, error) {
	closed := make(chan error, 1)
	go func() { closed <- consumer.Close() }()

	select {
	case err := <-closed:
		return true, err
	case <-time.After(grace):
		giveUp() // cancels the context the handlers were given
		return false, <-closed
	}
}
```

`false` is the signal worth logging and alerting on. It says the grace period is
shorter than the work in hand, or a handler is stuck, and both are worth knowing
before they turn into a redelivery spike nobody can explain.

## Two contexts, not one

```go
signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

handlers, giveUp := context.WithCancel(context.Background())
consumer, err := acemq.Consume(handlers, mq, "orders", handle)

<-signals.Done()
if drained, _ := closeWithin(consumer, 25*time.Second, giveUp); !drained {
	log.Print("shut down with work still in hand; it will be redelivered")
}
```

The handlers must **not** be given the context SIGTERM cancels. If they were,
the signal would cancel every handler the moment it arrived — the abrupt stop
this is trying to avoid, with extra steps. They get their own, cancelled when the
grace period runs out and not before.

Set the grace **slightly under** `terminationGracePeriodSeconds`, so the timer
loses the race to your own log line rather than to SIGKILL.

## Giving a message back

```go
case <-ctx.Done():
	return acemq.Retry(ctx.Err())
```

A handler out of time returns `Retry`. The library will not republish on a
cancelled context, so the delivery is returned to the broker unacknowledged,
exactly as it arrived, for whoever starts next. It does not use up an attempt
and it does not go round a retry ladder.

**One exception, and it can lose the message: the last attempt.** When the
retry policy has no attempts left — always, under `acemq.NoRetry()` — `Retry`
means "dead-letter it", the engine's republish to `{queue}.dlq` is refused on the
cancelled context like any other, and the delivery is *rejected without requeue*
to the broker's own dead-lettering. On a queue declared without
`x-dead-letter-exchange`, that is nowhere: the message is gone. This example's
consumer has no retry policy, so every attempt has another behind it. If yours
has one, give the source queue a broker-side dead-letter exchange as the
backstop, or make sure handlers finish inside the grace period.

## What no timer can fix

The bound only works for handlers that watch their context. One that does not —
a blocking call with no context, a loop that never checks — holds Close for as
long as it runs. In a real service the process exiting is then the bound, and
the broker redelivers whatever was unacknowledged.

`Conn.Close` closes every consumer on the connection the same way, so it waits
for the same handlers, without a deadline either.

## What redelivery costs you

Nothing, if the handler is idempotent. Everything, if it charges a card. A
graceful shutdown reduces duplicates; it does not eliminate them — a power cut
has no SIGTERM. See
[intermediate/02-idempotent-consumer](../02-idempotent-consumer).
