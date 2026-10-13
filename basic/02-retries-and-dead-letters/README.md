# basic/02 — retrying a message, and giving up on it

A payment handler that always fails, retried three times under a fixed policy
and then dead-lettered with its history intact — followed by a second payment
whose error is marked fatal, which skips the retries and goes straight to the
dead-letter queue.

## What it shows

- **The attempt counter moving**: 1, 2, 3, then the message is given up on.
- **A dead-letter queue declared for you.** Starting a consumer declares
  `acemq.dlx`, `payments.dlq` and `payments.parked`, so a message the library
  gives up on has somewhere to land.
- **`acemq.Fatal`** — an error that retrying cannot fix, which overrides the
  handler's request to retry.

## Running it

```bash
docker compose up -d
go run ./basic/02-retries-and-dead-letters
```

## What to look for

```
handling p-1, attempt 1, age 2ms
handling p-1, attempt 2, age 308ms
handling p-1, attempt 3, age 610ms
dead-lettered after attempts [1 2 3]
the envelope kept its history: id=ef25f1b4-e355-4e77-a20c-f97df8691f28 first seen 611ms ago

the same queue, with a reason that retrying cannot fix:
handling p-2, attempt 1
handled once, then dead-lettered: the mark won over the request
```

**The attempt number actually moves.** A broker requeues the bytes it was given,
so a header written at publish time would say 1 however many times the message
came back. The library counts from the redelivery instead, which is why the
policy can stop at three.

**The age goes up by about 300 ms each time**, which is the
`FixedRetry(3, 300*time.Millisecond)` wait. Jitter is switched off so the output
is readable. Leave it on in anything real: messages that failed together must
not all come back at the same moment.

**`first seen 611ms ago` on the dead-lettered copy** is the envelope keeping its
original timestamp and id. Whoever reads the dead-letter queue sees when the
message first arrived, not when it was last moved.

**`p-2` is handled once.** The handler asks to retry, but the error inside is
wrapped in `acemq.Fatal`, and the mark wins. A missing customer will still be
missing in 300 ms, so waiting would only delay the person who has to fix it.

## Something has to read the dead-letter queue

The example reads `acemq.DeadLetterQueue("payments")` to prove the message
landed. A dead-letter queue nobody reads is where messages go to be forgotten
quietly, which is worse than dropping them. Getting them back once the cause is
fixed is [basic/04-replay](../04-replay).

The dead-letter queue is classic where a durable queue declared by hand is
quorum, and a queue cannot be redeclared as another type. That is why the
example lets the consumer declare it rather than declaring it first.

## In the documentation

- [Tutorial: surviving failure](https://acemq-company.github.io/acemq-go-amqp/tutorial-surviving-failure.html)
- [Retries, redelivery and shutdown](https://acemq-company.github.io/acemq-go-amqp/reliability.html)
