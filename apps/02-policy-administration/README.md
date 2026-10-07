# apps/02 — policy administration (modular monolith)

One deployable, six modules, and no module that imports another.

[apps/01](../01-order-fulfilment) is five services that cannot call each other
because a network is in the way. This is the same discipline with the network
removed: the modules run in one process, share one connection and one database,
and still communicate only by publishing events. The boundary is the import
graph rather than a deployment, which is weaker against a determined engineer and
strong enough against a distracted one.

**A modular monolith is not a step towards microservices.** It is a different
answer to the same question, and for most organisations the better one: module
boundaries without distributed transactions, independent reasoning without
independent deployment, and one database you can actually join across.

It is a port of the Java example of the same name. The exchange, the queues, the
routing keys, the pipeline, the message types and every JSON field are the same,
character for character.

## The flow

```mermaid
flowchart LR
    B["broker submits"] --> P["policies<br/>applications + outbox<br/>one transaction"]
    P -->|application.submitted| U["underwriting<br/>pipeline: register → price → decide"]
    U -->|application.accepted| P
    U -->|application.declined| A
    P -->|policy.issued| BI["billing<br/>idempotent premium"]
    P -->|policy.issued| C["claims"]
    C -.->|"asks: is it in force?"| P
    D["documents<br/>claim check"] -->|document.stored| A["audit<br/>policy.#"]
    BI -->|premium.charged| A
```

The dotted line is the only one that is not an event: claims **asks** policies a
question and waits for the answer.

## What each module is here to show

| Module | The pattern | Why it lives there |
|---|---|---|
| [policies](policies/policies.go) | Transactional outbox, and a responder | One database does *not* remove the dual write. The two systems that must agree are this database and the broker, and no transaction spans both |
| [underwriting](underwriting/underwriting.go) | A declared pipeline with described steps | The one genuinely sequential part: check the register, price it, decide. A queue per stage, so a slow stage is a deep queue you can point at |
| [documents](documents/documents.go) | Claim check | A scanned medical report is tens of megabytes. The store gets the bytes; the message gets the key |
| [billing](billing/billing.go) | Shared idempotency store | The only module where handling a message twice is money |
| [claims](claims/claims.go) | Request and reply | Needs an answer *now*, before settling. Asks over the broker even though the callee is in the same process |
| audit | Topic wildcard | A queue bound to `policy.#`, and nothing else. Every event, including ones not invented yet |

[contracts](contracts/contracts.go) is the only package the modules share: the
events, the exchange, the queues and the routing keys.

## The outbox is still necessary

This surprises people, so it is the first thing to read:

```go
tx, err := m.db.BeginTx(ctx, nil)
write(tx)                                                     // this database
patterns.NewSQLOutboxStore(tx, patterns.SQLiteDialect).Add(ctx, record) // the same transaction
tx.Commit()                                                   // one decision, both writes
```

A monolith removes the distributed transaction *between modules*. It does nothing
about the one between a module and its broker. Save the application and publish
the event without an outbox, and a crash between them still loses one of the two
— and the application still exists with nobody told about it.

## Why claims asks instead of reading

Claims and policies are in the same process. A function call would work. It is
still the wrong choice:

```go
status, err := m.requester.Do(ctx, contracts.PolicyQuery{PolicyID: policyID})
```

The moment claims calls into policies directly, the two are one module and no
package structure will separate them again. Asking over the broker costs a
millisecond and keeps the seam that makes this arrangement worth having.

Note the timeout, and what happens when it expires: the claim is **neither
settled nor rejected**. A lookup that did not answer is not a "no", and treating
it as one would refuse valid claims whenever the application was busy.

## Running it

```bash
docker compose up -d
cd apps/02-policy-administration && go run .
```

A module of its own, like apps/01 and for the same reason: policies and billing
share a real SQLite database, and the pure-Go driver needs Go 1.25. The run starts
the whole application in one process and puts six scenarios through it, each
against a freshly started application and freshly emptied queues:

```
--- an ordinary application becomes a policy, and the premium is taken once
pipeline underwriting with 3 steps: register (look the applicant up on the shared industry register) | price (apply the rating table for the product and the applicant's age) | decide (accept, or refer anything a rule should not be deciding)
audited: 4 events
--- an application above the automatic limit is referred, and never becomes a policy
audited: 2 events
--- a claim is assessed against an answer from policies, not against a local copy
audited: 6 events
--- a large document travels as a claim check, not as a message
audited: 5 events
--- three copies of one event charge once; a genuinely different event still charges
audited: 8 events
--- a lookup nobody answers is neither a yes nor a no
refused to guess: could not establish whether POL-5f5993d5 is in force, so claim CLM-6c51946e was neither settled nor rejected; it must be retried: acemq: no reply arrived before the deadline after 5s (correlation a39cc4f5-…)
audited: 4 events
every application ended where it should
```

The first five are the Java system test's five, with the same assertions. Two
claims that test leaves unchecked are checked here as well:

- **The audit trail holds every event exactly once.** Each scenario counts the
  `policy.audit` queue, waits, and counts again, so a lost event and a duplicate
  are both a failure. Four for a policy — submitted, accepted, issued, charged —
  and in the idempotency scenario eight: the three copies are audited, because
  they really were published, and they make one charge, not three.
- **A lookup that does not answer is not a "no".** The sixth scenario stops
  policies and submits a claim. It must come back as an error naming the
  timeout, after the full five seconds, with nothing settled and nothing
  rejected.

Any assertion that breaks ends the run with a non-zero exit, which is how CI runs
it.

### Give it a virtual host of its own

```bash
docker compose exec -e HOME=/var/lib/rabbitmq broker rabbitmqctl add_vhost policy
docker compose exec -e HOME=/var/lib/rabbitmq broker rabbitmqctl set_permissions -p policy guest ".*" ".*" ".*"
ACEMQ_POLICY_URL=amqp://guest:guest@localhost:5672/policy go run .
```

The run deletes its queues before every scenario, so a message an earlier one
left behind cannot be counted by the next. Deletions like that belong on a
virtual host nothing else uses, and CI runs it on one. `ACEMQ_POLICY_URL` wins
over `ACEMQ_URL`.

## What writing this found

**In Java, the topology was wrong and the library said so.** `claim.settled` and
`document.stored` were published with nothing bound to them, and the Java library
refused the publish — "nothing is bound to exchange 'policy' for routing key
'policy.claim.settled'" — rather than letting the broker drop them. The `audit`
queue exists because of that failure, and a regulated insurer would have had one
anyway.

**In Go, the same mistake would have been silent.** Java's publishers are
mandatory by default. Go's are not: without `acemq.Mandatory`, a message nothing
is bound to is discarded by the broker and `Send` returns nil. Every event in
this port goes through `contracts.Event`, which asks for mandatory publishing, so
the failure Java gave its authors is the failure this port would give its own.
Leave the option out and delete the audit binding, and the run still passes the
Java test's assertions — only the audit count notices.

## Does it talk to the Java one?

Not checked, so not claimed. Everything on the wire is the Java app's — the
exchange, the queues and their classic type, the pipeline's exchange and its
`underwriting.{step}` queues, the routing keys, the message types and every JSON
field — but nobody has yet run the two side by side on one broker the way
[apps/01](../01-order-fulfilment#it-talks-to-the-java-one) was.

## What is not here yet

The claim check is a map in the `documents` module, as it is in Java.
`patterns.ClaimCheckCodec` does exist in this library now, and offloads a large
payload transparently, but it chooses its own keys; this module's keys name the
policy and the kind of document, and the scenario asserts that they do.

**Retention is the part to think about before you ship one.** The store and the
queue have different lifetimes. A message replayed a month later carries a key,
and if the store expired it the replay produces a message nobody can read — worse
than a lost message, because it looks like a message.

## Related

- [apps/01](../01-order-fulfilment) — the same patterns, across five processes
- [intermediate/08](../../intermediate/08-a-declared-pipeline) — a declared pipeline on its own
- [intermediate/01](../../intermediate/01-request-reply) — request and reply on its own
- [intermediate/03](../../intermediate/03-outbox) — the outbox on its own
