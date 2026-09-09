# basic/05 — four formats on one queue

One consumer reads JSON, YAML, TOML and XML without being told which is which,
and the two codecs that do not interpret anything at all.

## What it shows

- **A `CompositeCodec` reading by content type.** Four producers, four formats,
  one queue, one consumer that was never configured for any of them
  individually.
- **A codec per publisher.** `PublishWith` is what writes; the connection's codec
  is what reads. That asymmetry is the shape of a real migration.
- **One struct, four tag sets.** The field names on the wire are not the Go field
  names, and no encoder reads another's tags.
- **`StringCodec` and `BytesCodec`**, which are for text that is text and bytes
  that are bytes.

## Running it

```bash
docker compose up -d
go run ./basic/05-codecs
```

## What to look for

```
published json as application/json
published yaml as application/yaml
published toml as application/toml
published xml  as application/xml

content type                bytes  decoded
application/json              51  A-7 4250 acme
application/toml              50  A-7 4250 acme
application/xml               87  A-7 4250 acme
application/yaml              43  A-7 4250 acme

text:  "order A-7 was placed by hand"
bytes: 89 50 4e 47 0d 0a 1a 0a ff fe 00 01
```

**The consumer was never told which format any of those messages was in.** It
holds a `CompositeCodec`, and a composite asks each codec in turn whether it can
read the content type that arrived. The first that says yes gets the body.

The byte counts are the other half of the check. Four distinct content types and
more than one body length is what proves these are four encodings rather than
four copies of the same one — a publisher that had quietly fallen back to the
connection's codec would still decode correctly and would show up here.

## Why one struct needs four tag sets

Because every encoder has its own opinion about what a Go field is called:

| encoder | `TotalCents` becomes |
| --- | --- |
| `encoding/json` | `TotalCents` |
| `gopkg.in/yaml.v3` | `totalcents` |
| `BurntSushi/toml` | `TotalCents` |
| `encoding/xml` | `TotalCents`, inside a root named after the Go type |

None of them reads another's tags, so the struct names the field in each. This
is not pedantry: a Go field and a Java field are the same field only when they
have the same name on the wire, and the default is four different messages for
one struct.

`XMLName` fixes the root element and is excluded from the other three, because it
is a marker for one encoder rather than a field of the message.

## The gate underneath it

A codec answers for the content types it can read and refuses the rest, and the
example asserts that: the TOML codec is asked whether it will take
`application/yaml` and says no.

Without that gate the first codec in the list would be handed every message. It
is also why **none of these claims a message whose sender set no content type**.
Only JSON does, because JSON is what the library writes when nobody says
otherwise; the rest would be guessing, and a guess that parses is worse than a
refusal.

`BytesCodec` is deliberately not in the composite. It answers for *every* content
type, so from anywhere in the list it wins every message — which is why it has to
be asked for by name.

## Modules, not extras

YAML and TOML are Go modules of their own:

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/codec/yaml
go get github.com/AceMQ-Company/acemq-go-amqp/codec/toml
```

The core library depends on one package and nothing else, so a service that
speaks JSON never resolves a YAML parser. XML is in the core because
`encoding/xml` is in the standard library and costs nothing to include.

Java and .NET split the same way for the same reason, and the content types are
shared across all five languages — `application/yaml` written here is
`application/yaml` read there.

## The two that do not interpret

`StringCodec` is for a message that really is a line of text: a log line, a
command somebody typed. Anything with fields wants JSON.

`BytesCodec` hands the body over exactly as it arrived. The payload here is not
valid UTF-8 on purpose — a codec that had decided it was text would have replaced
`ff fe` with U+FFFD, and the example asserts the bytes came back byte for byte.
It is also what replay uses: the bytes that were committed are the bytes that
should go back, and re-encoding through a Go type that has since changed would
produce something else.

## Avro and protobuf are elsewhere

They are in [intermediate/07-binary-codecs](../../intermediate/07-binary-codecs)
because neither is readable without the schema that wrote it, which is a
different problem from picking a parser by content type.
