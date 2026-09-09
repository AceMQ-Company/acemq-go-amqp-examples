# intermediate/06 — one rule, every message

A tenancy rule applied to every publish and every delivery on a connection,
without a single publisher or handler knowing about it.

## What it shows

- **A publish interceptor that stops a publish.** A message with no tenant is
  refused at the call site rather than sent and sorted out later.
- **A publish interceptor that rewrites the payload** before it is encoded, so a
  card number never reaches the broker.
- **A consume interceptor that refuses a delivery**, dead-lettering it with the
  reason instead of running the handler.
- **They run in the order they are added**, and the example asserts it rather
  than claiming it.

## Running it

```bash
docker compose up -d
go run ./intermediate/06-interceptors
```

## What to look for

```
published p-1 for acme
published p-2 for globex
publishing p-3 without a tenant: acemq-example: this message has no tenant, and every message must belong to one

handler saw    p-1  tenant=acme  card="•••• 4242"
on the wire    {"paymentId":"p-1","card":"•••• 4242","cents":4250}
dead-lettered  p-2  because: an interceptor refused it: acemq-example: this message belongs to tenant "globex", and this process serves [acme]

stamped [acme globex], redacted [p-1 p-2], refused [globex]
```

**`on the wire`** is the line that matters most. It is the body as it arrived —
what the broker wrote to its disk, what its management interface would show,
what ends up in a backup — and the card number is not in it. The example asserts
against those bytes rather than against the decoded payload, because the decoded
payload would look the same either way.

**`dead-lettered p-2`** is the other half. A message for a tenant this process
does not serve never reached a handler, and it did not vanish either: it is on
`go-interceptors.payments.dlq` with a reason naming the tenant and what this
process serves, which is what somebody draining that queue needs.

## Intercept, not observe

Returning an error is the difference:

| | returning an error |
| --- | --- |
| publish interceptor | the publish does not happen, and the caller gets the error |
| consume interceptor | the handler does not run, and the message is dead-lettered |

A message that must not go out can be stopped in one place rather than in every
publisher, and a message this process must not read can be refused in one place
rather than in every handler.

**Dead-lettered rather than retried**, deliberately. An interceptor that says no
will say no again to the same message, so retrying it would spend the whole retry
ladder to reach the same conclusion several minutes later.

## Order, and why it is checked

Interceptors run in the order they are added, and the second one here reads the
header the first one wrote. That is asserted — it returns an error if the tenant
header is missing — rather than being left as a comment somebody can invalidate.

The chain also stops at the first error, which is why `p-3` appears in neither
list: the tenant interceptor refused it and the redacting one was never reached.

## Before the payload is encoded

A publish interceptor sees `Payload` as the value, not as bytes. Redacting here
changes a Go field; redacting after encoding would mean rewriting JSON, and would
break the day somebody switches that publisher to YAML.

`Payload` is an `any`, so an interceptor that cares about a particular type
asserts it and returns without an opinion otherwise. Every message on the
connection passes through, including the ones from code this example never wrote.

## Why a connection rather than a wrapper

Registered on the connection, the rule covers publishers this file never
constructs: the one inside `patterns.Requester`, the one the outbox relay uses,
the one in a library added next quarter. A rule written into each publisher is
correct until somebody adds the eleventh.

## The header namespace

`x-tenant`, not `x-acemq-tenant`. The `x-acemq-` prefix is the engine's: a header
carrying it is materialised onto the envelope when this version knows it and
dropped from the application's headers either way. An application header put
there would go out on the wire and vanish before the handler saw it.

## What else an interceptor can do

`PublishContext` carries `Exchange` and `RoutingKey`, and changing them redirects
the message — a shadow queue for a tenant being migrated, a version-suffixed
exchange during a rollout. `ConsumeContext` carries the undecoded `Body` and
`Redelivered`, which is enough to refuse a schema version this process cannot
read before a codec has to fail on it.
