# advanced/03 — keeping a large payload off the broker

Two reports, one byte apart, on either side of the threshold. One travels on the
wire; the other goes to a store and the message carries the key.

## What it shows

- **The threshold**, from both sides in one run.
- **The three-byte framing** that tells a consumer which of the two it is
  holding.
- **A filesystem store**, so the payload outlives the process that wrote it —
  and outlives the queue.

## Running it

```bash
docker compose up -d
go run ./advanced/03-claim-check
```

## What to look for

```
the store is /tmp/acemq-claim-check-1790715065
offloading at 65536 bytes and above

            encoded   on the wire   framing
  small     65535         65538   AC 01 00  inline
  large     65536            39   AC 01 01  checked -> db764677-fec3-4c59-a0af-691ab38caf37

db764677-fec3-4c59-a0af-691ab38caf37 holds the 65536 bytes the broker never saw
```

**The two rows differ by one byte of payload and by two orders of magnitude on
the wire.** That boundary is the whole point, and it is invisible in an example
where every message is large.

**`65535` is inline and `65536` is checked**, because the comparison is strictly
less than — a payload exactly at the threshold is offloaded. Java, Python and
Ruby use the same comparison, and they have to: a payload sitting on the boundary
must not be inline from one library and checked from another. That kind of
disagreement shows up as one consumer in five failing on documents nobody can see
anything wrong with.

**`AC 01 00` and `AC 01 01`.** Three bytes — magic, version, kind — and the third
is how a consumer decides whether it is holding a payload or a reference to one.
Below the threshold the payload travels inline, byte for byte what the delegate
codec wrote, so a consumer handles both without being told which to expect. That
is what allows the threshold to be changed, or this codec to be introduced at
all, without a flag day: messages written before the change are still readable
after it, and a body that was never framed is read as the delegate would read it.

**`39` bytes on the wire** is the three-byte header plus a 36-character key. The
key is the store's key as bare UTF-8 — not a URI, not a scheme, nothing wrapped
around it — so a Go consumer pointed at the same store reads a document a Java,
Python or Ruby publisher checked in.

## Only when it is worth it

Offloading a two-hundred-byte message turns one broker round trip into a store
round trip and a broker round trip. An unconditional claim check makes the common
case slower to fix the rare one, which is why there is a threshold at all rather
than a codec that always offloads. `patterns.OffloadAbove(0)` offloads
everything, which is occasionally what a store-backed audit trail wants.

## The framing, and not a header

A header can be stripped by a shovel or a federation link; the body cannot. And a
header that is either present or absent cannot say *inline* about a message
written before the codec existed, which is what makes adding a claim check to a
live queue safe.

`x-acemq-claim` is reserved for an application that wants to say where a payload
went, so an operator reading a dead-letter queue can see it without decoding
anything. This codec does not write it. Python and Ruby reserve it the same way.

Use `patterns.ClaimKeyOf` to read the key out of a body without fetching it —
that is the question an operator holds in front of a dead-letter queue: which
object does this message need, and is it still in the store? Answering it from
the message alone is the difference between a five-minute check and restoring a
backup.

## Why the filesystem store and not the in-memory one

The library ships both. `NewInMemoryClaimCheckStore` holds payloads in the
publisher's own memory — which is where they were going to be anyway, so it takes
them off the broker and does nothing else. Every consumer in another process gets
*the claim check is not in the store*, and a restart turns every message still in
a queue into one that can never be read. It is genuinely useful in a test where
the publisher and the consumer are the same process and the thing being proved is
the framing.

This example uses two `FilesystemClaimCheckStore` values that share nothing but a
directory: one for the publisher, one for the consumer. Then it deletes the queue
and redeems the key from a third, built after everything else was closed. A claim
check that does not outlive the process that wrote it is a message nobody else
can read.

## Retention is the part that goes wrong

Nothing here deletes the payload, and `Delete` is never called by the codec.
Deleting on read would break a second consumer of the same message; deleting on
acknowledgement would break a replay. When a payload may be removed is a
retention decision, and retention decisions belong to whoever owns the data.

The store's retention has to exceed every retention that could bring a message
back: queue TTLs, dead-letter queues, and however long somebody might sit on a
message before replaying it by hand. A message replayed a month later carries a
key, and if the store expired that key the replay produces a message nobody can
read — worse than a lost message, because it still looks like a message and fails
deep inside a consumer rather than visibly. When in doubt, longer.

The example leaves its temporary directory behind for the same reason it exists.
