# basic/01 — publishing a message and consuming it

The smallest AceMQ for Go program that is still honest: connect, declare a
durable queue, publish one order and wait for the broker to confirm it, then
consume it with a handler that has to say what it did with the message.

## What it shows

- **A confirmed publish.** `SendResult` returns the broker's answer, so
  "published" means the broker has the message rather than that the bytes left
  the process.
- **A handler that returns its decision.** `acemq.Accept()` is a return value,
  not a method call, so a handler that forgets to decide does not compile.
- **An envelope beside the payload** — the identifier, type, attempt and origin
  every AceMQ library reads and writes the same way.

## Running it

```bash
docker compose up -d
go run ./basic/01-publish-and-consume
```

## What to look for

```
published ef9e9b2c-0474-4fc3-b91c-64a6d23da490, confirmed by the broker: true
consumed  ef9e9b2c-0474-4fc3-b91c-64a6d23da490: order o-1 for 4250 cents
          type="orders" attempt=1 origin=examples@01-publish-and-consume
```

**The identifier is the same on both lines.** The library gives every message an
id before it is published, and the consumer reads the same one back. That id is
what an idempotent consumer keys on and what you search for when a message goes
missing — see [intermediate/02-idempotent-consumer](../../intermediate/02-idempotent-consumer).

**`confirmed by the broker: true`** is the line that a client without publisher
confirms cannot print. Without it, a publish that the broker dropped looks
exactly like one it kept.

**`origin=examples@01-publish-and-consume`** comes from `acemq.WithOrigin`. It is
also the name RabbitMQ's management interface shows for the connection, which
is worth having set before the day you need to know who is holding a message.

**`attempt=1`** is the counter [basic/02-retries-and-dead-letters](../02-retries-and-dead-letters)
watches move.

## The json tags are the contract

```go
type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}
```

Without them Go puts `OrderID` on the wire capitalised, and a Java, .NET,
Python or Ruby consumer reading `orderId` sees nothing at all. The tags are what
make a Go field and another language's field the same field.

## Declared, not assumed

`DeclareQueue` makes a durable quorum queue, and declaring one that already
exists with the same arguments is harmless. A service that declares what it
consumes from starts the same way on a fresh broker as on an old one.

## In the documentation

- [Tutorial: your first message](https://acemq-company.github.io/acemq-go-amqp/tutorial-first-message.html)
- [Publishing](https://acemq-company.github.io/acemq-go-amqp/publishing.html)
- [Consuming](https://acemq-company.github.io/acemq-go-amqp/consuming.html)
