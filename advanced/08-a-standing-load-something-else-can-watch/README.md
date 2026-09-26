# advanced/08 — a standing load something else can watch

A load that does not finish. It publishes and consumes at a steady rate and prints
one JSON object per second saying what has happened to it, so that something
outside the process can read what the client saw rather than what the broker did.

Every other example here runs, proves a point and exits. This one is the thing a
fault drill breaks the cluster underneath.

## What it shows

- **A client reporting its own state**, in a shape another program can read: one
  JSON object per line on stdout, oldest first.
- **Back-pressure separated from failure.** A send this library declines because
  the broker blocked the connection is counted as `refused`, not `failed`. They are
  different events, and a library that refuses promptly should not score worse than
  one that hides the same condition by parking the caller.
- **`blocked` derived from `BlockedReason()`**, so a line cannot claim to be
  blocked without saying why, or give a reason while claiming not to be.
- **Deltas, not just totals.** `publishRate` and `consumeRate` cover the last
  interval, because a total on its own cannot tell a client that is working from
  one that worked earlier and has since stopped.
- **A final line on the way out**, so the last interval is accounted for instead of
  being the one a reader has to guess about.

## Running it

```bash
docker compose up -d

go run ./advanced/08-a-standing-load-something-else-can-watch \
    -broker amqp://guest:guest@localhost:5672 > readings.jsonl
```

Flags: `-broker`, `-queue`, `-rate` (per second), `-interval`, `-for`. With no
`-for` it runs until interrupted, which is what a drill campaign wants. Readings go
to stdout and everything else to stderr, so redirecting stdout gives a file
containing nothing but the timeline.

## What it prints

```json
{"at":"2026-09-26T19:15:22Z","elapsedMs":7000,"blocked":false,"published":2032,"confirmed":2032,"consumed":2032,"failed":0,"publishRate":295.98,"consumeRate":295.98,"refused":0}
```

Under a memory alarm the same line reads:

```json
{"at":"...","elapsedMs":34000,"blocked":true,"published":9100,"confirmed":6001,"consumed":6001,"failed":0,"publishRate":0,"consumeRate":0,"refused":3098,"reason":"low on memory"}
```

`confirmed` has stopped moving, `refused` is climbing, and `reason` is the broker's
own words. That is the whole point of the example: none of it is visible to
anything that asks the broker how it is doing — the cluster is healthy, and this
application is not publishing.

## The fields

| Field | Is |
|---|---|
| `blocked` | the broker is refusing to read from this connection now |
| `published` | sends attempted since the start |
| `confirmed` | sends the broker has acknowledged |
| `consumed` | deliveries handled |
| `failed` | sends that failed for a reason other than back-pressure |
| `publishRate` / `consumeRate` | confirms and deliveries per second over the last interval |
| `refused` | sends declined because the connection was blocked |
| `reason` | what the broker said when it blocked the connection |

The first seven names are a contract with whatever reads them, so they are not
renamed for tidiness: a reader looking for `confirmed` and finding `acked` sees a
client reporting nothing, which is indistinguishable from a well-behaved client on
a quiet cluster.

## Why a drill cannot use a probe instead

A probe that connects to the broker answers "is the cluster usable". It cannot
answer whether the application was told the broker had stopped reading from it,
whether it stopped publishing, or whether it recovered on its own — and those
differ between client libraries that are otherwise equivalent. The only thing that
can report them is a client, which is why this example exists in a repository of
examples rather than in whatever is doing the watching.

## Also see

- `advanced/07-health-when-the-broker-blocks` — what `Health` says about the same
  condition, asserted rather than merely reported.
- `advanced/01-connection-recovery` — what happens to a consumer when the
  connection goes away and comes back.
- `advanced/02-metrics-and-health` — the same counters exposed to a metrics
  scraper instead of a log.
