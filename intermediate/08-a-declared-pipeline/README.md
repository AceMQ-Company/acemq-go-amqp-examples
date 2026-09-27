# intermediate/08 — a three-step flow with a queue between every step

Two orders through one declared pipeline: a physical one that goes all the way,
and a digital one whose run ends at `reserve` because there is nothing to
reserve.

## What it shows

- **One declaration**, not three services agreeing by convention. `NewPipeline`
  declares the exchange, a queue per step and the bindings, and starts a consumer
  on each — so a pipeline that returns without an error is one that is running.
- **A step that stops the run**, by returning `false`. Nothing is published
  onwards and the message is accepted: a decision rather than a failure. `dispatch`
  never sees the digital order, and the output says so.
- **Per-step options** — `StepConsumers(4)` on the slow step only, its own
  `StepRetry` ladder, and `StepDescribedAs` for whoever reads the log.
- **A queue per step**: `fulfilment.validate`, `fulfilment.reserve`,
  `fulfilment.dispatch`. A deep queue names the stage that is behind, and you can
  scale that one alone.

## Running it

```bash
docker compose up -d
go run ./intermediate/08-a-declared-pipeline
```

## What to look for

The line the example logs from `pipeline.Describe()`:

```
pipeline fulfilment with 3 steps: validate (reject anything nobody can ship) |
reserve (hold stock for 15 minutes so payment cannot oversell) | dispatch (hand
the parcel to the courier)
```

Nothing in the library writes it. A library that picks a logger picks it for every
application that imports it, so `Describe()` returns the line and the application
decides where it goes.

Then the three steps' own accounts. `validate` and `reserve` saw both orders;
`dispatch` saw one.

## The types have to line up, and Go checks at start-up

`reserve` consumes an `Order` and produces a `Reservation`, so `dispatch` has to
consume a `Reservation`. Change one of them and the pipeline refuses to start:

```
acemq: pipeline "fulfilment" step "reserve" produces main.Reservation
but the next step "dispatch" consumes main.Order
```

Java's builder catches that at compile time. Go cannot — a method cannot introduce
type parameters, so a fluent chain cannot carry the previous step's output type
forward — so the check is made with reflection when the pipeline is assembled. It
fails before the first message rather than on it, and it names both steps and both
types.

## It is the same pipeline in every language

The exchange is the pipeline's name declared `direct`, the queue behind a step is
`{pipeline}.{step}`, the binding key is the step's name, and the route travels on
the message as `x-acemq-route` with a position. That is Java's topology to the
letter, and .NET's, Python's and Ruby's — so one step of this flow could be running
in any of them, and a message this example publishes would be picked up by a Java
consumer on `fulfilment.dispatch` without either side being told about the other.

See `patterns.AlongRoute` for the other direction: one Go step inside a pipeline
somebody else declared, with no `Pipeline` involved at all.
