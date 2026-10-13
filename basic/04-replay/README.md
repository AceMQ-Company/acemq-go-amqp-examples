# basic/04 — putting dead-lettered messages back

Three invoices sit on a dead-letter queue after an outage, and `patterns.Replay`
puts them back on their work queue in two stages: first only one tenant's,
chosen by a filter, while the other tenant's invoice stays where it was, and
then the rest — each one handled exactly once.

## What it shows

- **A replay in stages.** A filter picks out the messages for one tenant, and
  what it declines is left on the dead-letter queue rather than discarded.
- **A limit on every replay**, so one run against a queue that is still being
  written to cannot go on for ever.
- **The reason a replay stopped**, reported rather than guessed.

## Running it

```bash
docker compose up -d
go run ./basic/04-replay
```

## What to look for

```
  handled inv-1 for acme
  handled inv-3 for acme
replayed 2, left 1 where they were, stopped because it drained
  handled inv-2 for globex
replayed the remaining 1, stopped because it drained
the dead-letter queue is empty and every invoice was handled once
```

**`left 1 where they were`** is the property that makes this safe to do at three
in the morning. The fix went out for `acme`, so only `acme`'s invoices go back.
`globex`'s invoice is still on the dead-letter queue in its original position,
and if the first stage had gone wrong you could stop there.

**`stopped because it drained`** means the queue ran out before the limit of ten
was reached. The others are `limit`, `deadline` and `cancelled`. "Moved 500"
means something quite different when the limit was 500, and the reason is the
difference between "done" and "done for now".

**Each invoice is handled once.** The consumer was started before the replay,
because replaying into a queue nobody is reading only moves the problem.

## The filter sees bytes

```go
Filter: func(_ acemq.Envelope, body []byte) bool {
	return strings.Contains(string(body), `"acme"`)
},
```

The filter gets the envelope and the raw body, not a decoded payload. A
dead-letter queue holds whatever was given up on, including messages no codec
can read, and a replay should not fail because one of them is there.

## Back on attempt one

A replayed message starts again on attempt one with its dead-letter reason
cleared. A message given up on at the last attempt of its policy would otherwise
be dead-lettered again before any handler saw it, and the person who had just
fixed the bug would have moved it from one queue to the same queue. Set
`KeepAttempts` to put back exactly what was there, for an audit or for a
consumer that counts attempts itself.

## Where the messages came from

The example puts the invoices on the dead-letter queue directly, so it starts
where the interesting part starts and prints the same thing every time. In a
real system they would have got there the way
[basic/02-retries-and-dead-letters](../02-retries-and-dead-letters) shows.

## In the documentation

- [Tutorial: surviving failure](https://acemq-company.github.io/acemq-go-amqp/tutorial-surviving-failure.html)
- [Patterns: replay](https://acemq-company.github.io/acemq-go-amqp/patterns.html#replay)
- [Retries, redelivery and shutdown](https://acemq-company.github.io/acemq-go-amqp/reliability.html)
