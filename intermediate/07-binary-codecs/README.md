# intermediate/07 — Avro and protobuf

The two formats where the schema is not in the message, and the registry that
makes one of them survivable.

## What it shows

- **A schema registry, and a producer whose schema the consumer has never seen.**
  The identifier in front of the body is enough.
- **The Confluent framing**, asserted byte by byte: one zero byte, four bytes of
  schema identifier, big-endian.
- **Two Avro modes that must not be confused**, and the content-type gate that
  keeps them apart.
- **Protobuf without protoc**, and why a queue carrying two message shapes is
  read as bytes in Go.

## Running it

```bash
docker compose up -d
go run ./intermediate/07-binary-codecs
```

## What to look for

```
published avro v1 as application/vnd.acemq.avro
published avro v2 as application/vnd.acemq.avro
published protobuf as application/x-protobuf

content type                     bytes  read as
application/x-protobuf              42  map[celsius:17.75 sensor:roof-3]
application/vnd.acemq.avro          20  sensor=roof-1 celsius=21.50
application/vnd.acemq.avro          22  sensor=roof-2 celsius=19.25

schema versions registered: 2
  id=1 version=1 fingerprint=1dab018d4546…
  id=2 version=2 fingerprint=3dc894078ce0…
```

**`sensor=roof-2 celsius=19.25`** is the line the example exists for. That
message was written by a producer using a schema with a field this consumer has
never heard of, and the consumer read it anyway — because it looked the writer's
schema up by the identifier on the message rather than assuming its own.

Nothing was coordinated. The producer registered its schema when it started; the
consumer fetched it when a message turned up.

**Twenty bytes** for a reading. The schema is not in the message, which is the
whole trade: smaller messages, and a registry you now have to run.

## The framing is the contract

```
[0x00][4 bytes schema id, big-endian][avro body]
```

Confluent's clients write it, the Java library writes it, and this one writes it.
The example asserts the first byte and then looks the identifier up in the
registry, which is a stronger check than reading the decoded value: a consumer
that had ignored the framing entirely would still produce a plausible-looking
record.

## Two Avro modes, and why mixing them is silent

| | writes | claims |
| --- | --- | --- |
| `avro.Of(schema)` | `avro/binary` | `avro/binary` |
| `avro.Registered(registry, subject, schema)` | `application/vnd.acemq.avro` | `application/vnd.acemq.avro` |

A fixed-schema codec handed a registry-framed body would read the five framing
bytes as the start of the first field. **Avro does not object.** It returns a
record where every value is wrong, with no error and no log line.

Refusing the content type turns that into a message no codec claimed, which is
something a person can see. The example asserts both refusals.

Neither mode — nor protobuf — answers for a message whose sender set **no**
content type, for the same reason: arbitrary bytes parse as some protobuf message
more often than not, and a codec that volunteered there would report nonsense as
a success.

## What Go does differently, and it is worth knowing

The consumer decodes with **the writer's schema**, fetched by identifier. So:

- A field the consumer's schema does not have is **dropped**. That works, and it
  is what this example shows.
- A field the consumer's schema declares with a `default` that the writer never
  wrote is **not** filled in from that default. It arrives as the Go zero value.

Java and Ruby resolve the writer's schema onto the reader's and do apply the
default. If you are writing a Go consumer that depends on a defaulted field
arriving populated, populate it yourself after decoding — a zero value and a
default are not the same thing, and only one of them is what the schema promised.

## Protobuf without protoc

`structpb.Struct` is a generated protobuf type that ships inside
`google.golang.org/protobuf`, so this example is one file with no build step. A
real service imports what protoc wrote for its own `.proto`; the codec cannot
tell the difference, because there is none to tell.

Python and Ruby build a descriptor at run time to make the same point.

## Why the queue is read as bytes

`Consume[T]` has one `T`, and this queue carries two unrelated shapes — an Avro
record and a protobuf message. A `CompositeCodec` picks the codec but still hands
back one type, so it is the right tool for basic/05, where every message decodes
into the same struct, and the wrong one here.

So the consumer takes `Message[[]byte]` with `BytesCodec` and dispatches on
`CanDecode(m.ContentType)` — the same question the composite asks, asked out
loud. `Message.Body` is there for exactly this.

## The registry in this example is not a registry

`patterns.NewInMemorySchemaRegistry` shares nothing between processes, which is
the entire point of having a registry. It is here to show the shape.
`patterns.NewSQLSchemaRegistry` outlives a process, and a fleet that already runs
Confluent's should use that one — the wire framing is deliberately the same.

## Modules

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/codec/avro
go get github.com/AceMQ-Company/acemq-go-amqp/codec/protobuf
```

Each is a module of its own, so the core library keeps its single dependency and
a service that speaks neither resolves neither.
