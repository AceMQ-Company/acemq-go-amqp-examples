# intermediate/01 — asking a question and waiting for the answer

A price service answers requests over the broker: one caller asks for a single
price, ten concurrent requests each get their own answer back on one shared
reply queue, and a request the service cannot answer comes back as the
service's own error rather than as a timeout.

## What it shows

- **`patterns.Serve`** — a responder that is an ordinary function from a
  request to a response or an error, run four at a time.
- **`patterns.NewRequester`**, with a generated reply queue and a timeout on
  every call.
- **Correlation**: many requests in flight on one reply queue, and nobody gets
  somebody else's answer.
- **`patterns.ErrResponderFailed`**, which tells a responder's failure apart
  from a timeout or a broken connection.

## Running it

```bash
docker compose up -d
go run ./intermediate/01-request-reply
```

## What to look for

```
replies come back on acemq-reply-2196da4d-6b3a-4280-abcc-5ff9f541f145
widget costs 600 cents
ten concurrent requests, each got its own answer
the responder's reason came back: acemq: the responder failed: no such product
```

**`acemq-reply-…`** is generated per requester: an exclusive, auto-deleting
queue that belongs to this process and goes away with it. Name one with
`patterns.ReplyTo` only when replies must survive a restart. A generated reply
queue is classic, and has to be, because RabbitMQ does not allow a quorum queue
to be exclusive or auto-deleting.

**`ten concurrent requests, each got its own answer`** is printed only if no
goroutine received a wrong price. Each request carries a correlation id, the
reply carries it back, and the requester hands each reply to the call that is
waiting for that id. A line saying `WRONG ANSWER` would mean replies were being
matched by arrival order.

**`no such product` arrived at the caller.** The responder returned an error,
and the error went back over the broker. Without that the caller would wait out
its five-second timeout and learn only that nothing answered. `errors.Is(err,
patterns.ErrResponderFailed)` means the round trip worked and the answer was a
failure, which a caller usually handles differently from a responder that is
down.

## The cost of a synchronous shape

A caller waiting for a reply holds a goroutine and a deadline. If the request
queue backs up, every caller waits, and a slow responder becomes a service that
stops responding. Use request and reply where the caller cannot go on without
the answer, and set the timeout to what the caller can actually afford — the
default is thirty seconds.

## In the documentation

- [Request and reply](https://acemq-company.github.io/acemq-go-amqp/request-reply.html)
