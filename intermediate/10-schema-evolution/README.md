# intermediate/10 — schema evolution

Two services on two versions of one schema, talking to each other anyway — and
the two ways to get it wrong, both shown failing.

## What it shows

- **Producer deployed first.** A consumer that has never heard of `currency`
  reads a message containing it, and the field is *skipped* rather than shifting
  every byte after it.
- **Consumer deployed first.** A consumer already on the new schema reads a
  message from a producer that is not, and the reader's default fills the gap.
- **A restart is not a new version.** The same definition registered again comes
  back with the same identifier.
- **Without `avro.ReaderSchema`, the default is not applied**, and **without a
  default, the read is refused.**

## Running it

```bash
docker compose up -d
go run ./intermediate/10-schema-evolution
```

## What to look for

```
reader  written with  decoded
v1      id 1          {OrderID:A-1 Total:42}
v1      id 2          {OrderID:B-2 Total:99.5}
v2      id 1          {OrderID:A-1 Total:42 Currency:EUR}
v2      id 2          {OrderID:B-2 Total:99.5 Currency:GBP}

versions of go.order.placed after a restart: 2
  id=1 version=1 fingerprint=63fdccfe3642…
  id=2 version=2 fingerprint=10be5eea9870…

no reader schema:  {OrderID:D-4 Total:7 Currency:}
no default:        acemq: schema 1 cannot be read as this consumer's schema: reader field currency is missing in writer schema and has no default
```

**Row two is the one that matters.** A producer already on the new schema, a
consumer still on the old one. Nobody had to redeploy the old consumer to make
the new producer safe to ship, and that is the only reason to run a registry.

**Row three is the other half.** A v2 reader meeting a v1 message fills
`currency` in from its own default, so the new code can be written as though the
field were always there.

**`written with id 1` / `id 2` is read off the wire**, from the five bytes framed
on the front of every registered message: one zero byte, then the schema
identifier, big-endian. That is Confluent's framing, and all five AceMQ
libraries write it. Avro resolves a *writer* schema onto a *reader* schema, and
without the identifier a reader has no idea what the writer used.

## The one option that matters

```go
avro.Registered(registry, subject, v2, avro.ReaderSchema(v2)) // resolves onto mine
avro.Registered(registry, subject, v2)                        // reads the writer's shape
```

The third argument is what this codec **writes**. `ReaderSchema` is what it
**reads as**, and it is what makes Avro resolve the two. Without it a consumer
decodes against the writer's schema: a field the writer added still comes
through, and a field the writer never wrote is simply absent — which in Go means
the zero value. That is `no reader schema: … Currency:` above: no error, no log
line, and an order with no currency where the schema promised `"EUR"`.

A zero value and a default are not the same thing, and only one of them is what
the schema said. Give every consumer a `ReaderSchema`; passing the schema it was
built with, as here, is the ordinary case.

[intermediate/07-binary-codecs](../07-binary-codecs) builds its consumer without
one, deliberately, to show the framing; this example is the one to copy.

## Always give a new field a default

```json
{"name": "currency", "type": "string", "default": "EUR"}
```

Without one there is nothing Avro can put there when an old producer omits the
field, so the read fails — the `no default` line above, which names both schemas
in full on the lines after the first. In a deployment that is **every consumer
breaking the moment it is rolled out ahead of the producers**, which, since
consumers are usually deployed first, is most of the time.

The safe changes are: add a field with a default, remove a field that had one,
and rename through an alias. Everything else — changing a type, making an
optional field required — is a new message type wearing the old one's name.

## A restart registers again, and that is fine

A codec registers its schema the first time it encodes, and a schema is
identified by a SHA-256 of its exact bytes, so a service registering on every
start gets the identifier it had before rather than a version per restart. Two
definitions that differ only in whitespace *are* two schemas: normalising first
would need a parser per format, and a registry that treated two schemas as one
because it mis-parsed them would be worse than one that is merely fussy.

What the codec registers is the schema as Avro prints it back, not the string
you passed. A codec built from the same definition always matches itself, which
is what the restart above relies on; a hand-written `registry.Register` with the
raw text is a different set of bytes and becomes a new version.

## No priming

A Go codec meeting an identifier it has not seen looks it up in the registry
itself, and remembers the answer — and the resolution onto the reader schema —
for every later message carrying it. There is no per-identifier step at start-up,
as Python's `learn_from` has.

## The registry here is not a registry

`patterns.NewInMemorySchemaRegistry` shares nothing between processes, so a
consumer cannot look up a schema a producer registered elsewhere — the entire
point of having one. `patterns.NewSQLSchemaRegistry` outlives the process, and
Confluent's works too, because the wire framing is theirs.

## Module

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/codec/avro
```
