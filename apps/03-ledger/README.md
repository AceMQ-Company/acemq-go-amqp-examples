# apps/03 — event-sourced ledger

The log **is** the system of record. Balances are not stored; they are what you
get by adding up the log, and can be deleted and rebuilt at any time.

[apps/01](../01-order-fulfilment) and [apps/02](../02-policy-administration)
publish events describing what happened to a system of record that lives in a
database. Here there is no such database. Every entry is appended to a stream and
nothing is ever updated or deleted — money moved wrongly is corrected by posting
the opposite entry, exactly as a paper ledger does, and both entries stay.

That is what lets a ledger answer *"what did we believe on Tuesday"*, which is
the question auditors actually ask and the one a mutable balances table cannot
answer at all.

It is a port of the Java example of the same name. The stream, the exchange, the
queues, the routing keys, the message types and every JSON field are the same,
character for character.

## Why a stream and not a queue

**A queue is emptied by being read. A stream is not.** That single difference is
the reason this application uses one:

- the writer reads the whole journal at start-up to recompute balances;
- a statement projection reads the same journal, from the same offset, at the
  same time, and neither reader affects the other;
- a projection written next year starts at offset zero and gets all of history.

On a queue, exactly one of those readers would get each entry. Which is correct
for a *command* — a transfer must be applied once — and wrong for a *fact*.

Note the two together: commands go to an ordinary queue in
[`contracts.Topology`](contracts/contracts.go), entries go to a stream the ledger
declares with its retention. Getting that backwards is the most common mistake in
event-sourced systems.

## The modules

| Module | |
|---|---|
| [ledger](ledger/ledger.go) | The only writer. Decides whether a transfer is allowed and appends the entries |
| [projections](projections/projections.go) | A statement per account, built by reading from offset zero. Stores nothing the log does not contain |
| [transfers](transfers/transfers.go) | Where transfers are asked for, and refusals noticed |

## One writer, deliberately

Every transfer produces two entries that sum to zero. That invariant cannot be
enforced by two processes appending independently — a stream will happily accept
an unbalanced pair from each of them. Making the writer singular is what makes
the invariant checkable at all.

It is also what makes the writer's own balance tracking correct: after rebuilding
from the journal it maintains its own totals, which is safe *because* nothing else
writes.

## The bug this design has already had

The first Java version of the balances kept following the stream **and** applied
each entry as the writer wrote it. Every entry was therefore counted twice — once
locally, once when it came back round — and an account ended up with double its
balance.

The fix is the shape [the code](ledger/balances.go) has: **read to the end, then
stop reading.** Keeping only the stream has the opposite problem, a transfer
decided against a balance that does not yet include the transfer before it.

It is worth knowing that this is the failure mode. A projection that both
subscribes and self-updates is a natural thing to write and silently wrong.

## Amounts are integers

```go
AmountMinor int64 `json:"amountMinor"`
```

Whole minor units — pennies, cents. A ledger in `float64` is a ledger that
disagrees with itself after enough additions, and the disagreement appears in
production, at scale, in the direction nobody expected.

Signed, too, rather than a debit/credit flag: a sum over a column is then simply
a sum, and "which sign means debit" stops being a question every reader answers
for themselves.

## Running it

```bash
docker compose up -d
go run ./apps/03-ledger
```

RabbitMQ streams need no plugin — `x-queue-type: stream` is core since 3.9 and
reachable over AMQP 0-9-1, which is why this runs against the same broker as
every other example. It needs no database either, so unlike apps/01 and apps/02
it is part of the root module and builds on Go 1.23.

```
--- a transfer posts two entries that sum to zero
the ledger rebuilt its balances from 0 entries
--- a transfer that would overdraw is refused, and the refusal is recorded
the ledger rebuilt its balances from 3 entries
--- a projection built from offset zero agrees with the writer
the ledger rebuilt its balances from 4 entries
--- a projection added later still gets all of history
the ledger rebuilt its balances from 9 entries
--- two readers of the same stream do not compete for entries
the ledger rebuilt its balances from 12 entries
--- a projection from now sees only what is new
the ledger rebuilt its balances from 15 entries
--- a ledger rebuilt from the journal agrees with the one that wrote it
the ledger rebuilt its balances from 16 entries
16 entries replayed, 10 accounts, 44100 in total
the ledger and every projection of it agree
```

The first five are the Java system test's five, with the same assertions. The
journal is emptied once, when the run starts, and never again: every scenario
starts a fresh ledger, so every scenario after the first is also a restart that
rebuilt its balances from what the ones before it wrote. Two claims the Java test
leaves unchecked are checked here as well:

- **A projection from now sees only what is new.** Java's reader can start at
  the next entry and its test never does. This one starts after sixteen entries
  exist and must read exactly the one written after it.
- **A restart changes nothing.** The README's claim, made checkable: the writer
  is stopped and started again, and must hold the same balances for every
  account, have replayed every entry the run ever wrote, and hold exactly the
  money that was paid in — not a penny more, which is what the double count
  above would have produced.

Any assertion that breaks ends the run with a non-zero exit, which is how CI runs
it.

### Give it a virtual host of its own

```bash
docker compose exec -e HOME=/var/lib/rabbitmq broker rabbitmqctl add_vhost ledger
docker compose exec -e HOME=/var/lib/rabbitmq broker rabbitmqctl set_permissions -p ledger guest ".*" ".*" ".*"
ACEMQ_LEDGER_URL=amqp://guest:guest@localhost:5672/ledger go run ./apps/03-ledger
```

The run deletes the journal before it starts, so a ledger an earlier run left
behind cannot be added to this one's. Deleting a ledger's journal is the one
thing a real ledger never does, and it belongs on a virtual host nothing else
uses; CI runs it on one. `ACEMQ_LEDGER_URL` wins over `ACEMQ_URL`.

## What writing this found

**A reconnection replayed every stream reader.** The library reattached a
consumer after a dropped connection with the arguments it started with, and for a
stream those include where to start reading. A projection that began at the first
entry was handed the whole journal a second time — 3,000 entries became 6,001
deliveries in the reproduction, every balance doubled, the bug above arriving by
another road — and a reader that began at the next entry skipped everything
appended while it was disconnected. Fixed in the library, which now carries a
stream reader on from the oldest entry it had not finished, but **not yet
released**: this app depends on 0.9.5, which still has it. The run never drops a
connection, so it is unaffected; a long-lived projection on 0.9.5 is not.

## Does it talk to the Java one?

Not checked, so not claimed — and on RabbitMQ 3.13 it would not work. The
queues and their classic type, the routing keys and every JSON field are the Java
app's, but the journal's retention is not written the same way. Java writes an
hour's `x-max-age` as `3600s`; Go writes it as `1h`. RabbitMQ 4 compares the two
as durations and accepts either declaration after the other. RabbitMQ 3.13
compares them as text, and whichever app declares the journal second fails:

```
PRECONDITION_FAILED - inequivalent arg 'x-max-age' for queue 'ledger.journal'
in vhost 'ledger': received '3600s' but current is '1h'
```

Not changed here, because it is not this app's to change: Java and .NET write
seconds, Go, Python and Ruby write the largest whole unit, and moving one library
to the other side would break its own existing streams on 3.13 the same way.

## What is honestly not here

- **Snapshots.** A rebuild is O(history), and at a billion entries that stops
  being free. The answer is "the balance at offset N, plus everything after N".
  A real technique, deliberately omitted: it is the second thing to build, and
  including it would make event sourcing look cheaper than it is.
- **Atomic double entry.** The two halves of a transfer are appended one after
  the other. A real ledger appends them as a single record precisely so that a
  crash between them is impossible; here a crash between the two lines would
  leave the journal unbalanced. A publish that fails after the first half is
  parked rather than retried, because a retry would append that half again.
- **Retention.** The journal keeps an hour, because this is an example. A real
  one keeps them for as long as the law says. **If retention is shorter than
  "forever", the projection is the system of record after all** — and nobody
  wrote that down.

## Related

- [basic/06](../../basic/06-streams) — offsets and replay, one idea at a time
- [apps/02](../02-policy-administration) — the same discipline with a database as the system of record
