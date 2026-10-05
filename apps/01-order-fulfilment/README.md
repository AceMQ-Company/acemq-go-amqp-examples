# apps/01 — order fulfilment (microservices)

Five services, one broker, no shared database, and no service that knows another
exists.

Everything under `basic`, `intermediate` and `advanced` shows one idea at a time.
This is what they look like when they have to coexist: an outbox at the edge,
idempotency where double-charging is real harm, a retry ladder where a
downstream is flaky, and one correlation id that turns five services into one
story.

It is a port of the Java example of the same name. The exchange, the queues, the
routing keys, the message types and every JSON field are the same, character for
character.

## The flow

```mermaid
flowchart LR
    C["customer"] --> G["gateway<br/>orders + outbox<br/>one transaction"]
    G -->|order.placed| P["payments<br/>idempotent charge"]
    P -->|payment.captured| I["inventory<br/>retry ladder"]
    P -->|payment.declined| N
    I -->|stock.reserved| S["shipping"]
    I -->|stock.unavailable| N
    S -->|order.shipped| N["notifications<br/>fulfilment.#"]
```

Each service owns one decision and publishes what happened. None of them calls
another.

## What each service is here to show

| Service | The pattern | Why it lives there |
|---|---|---|
| [gateway](gateway/gateway.go) | Transactional outbox | The edge is where the dual-write problem lives: save the order *and* announce it, or a crash loses one of them |
| [payments](payments/payments.go) | Shared idempotency store | The only service where handling a message twice is real money. Claims before charging, confirms after publishing |
| [inventory](inventory/inventory.go) | Retry ladder | Tells "the warehouse timed out" (retry) from "there are three left and they want ten" (say so, never retry) |
| [shipping](shipping/shipping.go) | Nothing clever | The point: it reacts to one event, does one thing, publishes one event. Adding a service beside it changes nothing |
| [notifications](notifications/notifications.go) | Topic wildcard | Bound to `fulfilment.#`. Added without touching a single publisher, and the next one will be too |

[contracts](contracts/contracts.go) is what they agree on: the events, the
exchange, the queues and the routing keys, and nothing else.

## Running it

```bash
docker compose up -d
cd apps/01-order-fulfilment && go run .
```

It is a module of its own, so it is run from inside its directory. The gateway
and payments each get a real SQLite database, because an outbox and an
idempotency store only mean something when they share a transaction with the
work — and the pure-Go SQLite driver needs Go 1.25. Every other example still
builds on 1.23.

The run starts all five services in one process and puts five orders through,
each against a freshly started system:

```
--- an order travels through every service
ord-868374ff: [OrderPlaced PaymentCaptured StockReserved OrderShipped]
--- a flaky warehouse is retried rather than failed
--- an order over the limit stops at payments
ord-927a047a: [OrderPlaced PaymentDeclined]
--- there is not enough stock and retrying would not help
ord-25fa1c89: [OrderPlaced PaymentCaptured StockUnavailable]
--- an order delivered twice is charged once
ord-8b4d337f: [OrderPlaced PaymentCaptured StockReserved OrderShipped OrderPlaced]
all five orders ended where they should
```

The first four are the Java system test's four, with the same assertions. The
fifth checks the one claim that test leaves unchecked: the relay is
at-least-once, so payments gets the same order a second time with the same
message id, and refuses it at the claim. Any assertion that breaks ends the run
with a non-zero exit, which is how CI runs it.

It deletes the four service queues before it starts, so a message left behind by
an earlier run that was killed cannot be counted by this one.

### Give it a virtual host of its own

```bash
docker compose exec -e HOME=/var/lib/rabbitmq broker rabbitmqctl add_vhost fulfilment
docker compose exec -e HOME=/var/lib/rabbitmq broker rabbitmqctl set_permissions -p fulfilment guest ".*" ".*" ".*"
ACEMQ_FULFILMENT_URL=amqp://guest:guest@localhost:5672/fulfilment go run .
```

Not tidiness. The exchange is called `fulfilment` because the Java app's is, and
[intermediate/08](../../intermediate/08-a-declared-pipeline) declares a *direct*
exchange of the same name, after a different Java example. An exchange cannot be
redeclared as another kind, so on one virtual host whichever of the two runs
second fails with `PRECONDITION_FAILED - inequivalent arg 'type'`. Both names are
somebody's contract, so neither is renamed. CI found this the first time it ran
both, and runs this app on a virtual host of its own for the same reason.

`ACEMQ_FULFILMENT_URL` wins over `ACEMQ_URL`, so the other examples can keep
using the default virtual host beside it.

## It talks to the Java one

Checked rather than assumed, against one broker and one virtual host: the Java
gateway, shipping and notifications with the Go payments and inventory, and then
the other way round. Both runs ended with the same timelines as above, the same
counts on either side — two captured, one declined, one reserved, one rejected,
two retries scheduled — and nothing left in the outbox.

Two things had to be right for that to work, and both are easy to get wrong:

- **The queues are classic.** A durable queue declared by this library is quorum
  unless told otherwise, Java's are classic here, and a queue cannot be
  redeclared as the other type. Leaving the type to the default would make the
  two languages unable to share a broker at all.
- **Every publish names its message type.** A Go publisher that does not falls
  back to the routing key, so notifications would show
  `fulfilment.order.shipped` where Java shows `OrderShipped`. The timeline
  assertion is what catches it.

## Design decisions worth arguing with

**A database per service.** The moment two services read the same table, the
deployment boundary is fiction. The gateway and payments each get their own.

**Every service applies the whole topology on start-up.** Applying it five times
is safe, and it means no service depends on another having started first — there
is no deployment order to get wrong.

**Payments runs before inventory.** Reserving stock for an order that cannot be
paid for is how a warehouse fills with holds nobody releases.

**Money is taken before stock is confirmed available.** When stock runs out the
customer has already been charged, and the run asserts exactly that. A real
system triggers a refund here; the example leaves it visible rather than
pretending the problem does not exist. That compensation is what
[a saga](../../intermediate/04-saga) would add.

**Payments gives its claim back when it cannot publish.** Kept, the claim would
make the retry look like a duplicate, and the order would stop at payments with
nobody told. Released, the retry charges it properly.

## The correlation id is the whole observability story

```go
acemq.CorrelationID(m.Envelope.CorrelationID)
```

Every service copies it forward. Notifications rebuilds the customer's timeline
from nothing but that id:

```
OrderPlaced → PaymentCaptured → StockReserved → OrderShipped
```

Four services that never spoke to each other, assembled into one sequence. Drop
that one option in any service and the order vanishes from the timeline — which
is also what happens to your traces and your log correlation in production.

## A fan-in consumer needs the text codec

`notifications` subscribes to six event types on one queue, so it cannot ask for
a payload type. It asks for a `string` and says so with
`acemq.ConsumeWith(acemq.StringCodec{})`. Without that the JSON codec is asked to
turn an object into a string, cannot, and parks every message — none reaches the
handler, and nothing is lost either: they wait in `fulfilment.notifications.parked`.

## What is deliberately not here

No HTTP. The gateway exposes `PlaceOrder` as a method, because adding a web
framework would triple the code and demonstrate nothing about messaging. In a
real service that method body is the handler behind a POST.

No compensation. See above — the refund path is named and not implemented.

## Related

- [intermediate/03](../../intermediate/03-outbox) — the outbox on its own
- [intermediate/02](../../intermediate/02-idempotent-consumer) — one message delivered four times and charged once
- [basic/02](../../basic/02-retries-and-dead-letters) — the retry ladder
- [advanced/06](../../advanced/06-tracing) — the trace this correlation id enables
