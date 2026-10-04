// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Two services on two versions of one schema, talking to each other anyway.
//
//	docker compose up -d
//	go run ./intermediate/10-schema-evolution
//
// The question is never "can we add the field". It is "which of the twenty
// services that read this message do we have to deploy, and in what order". With
// a default on the new field and a reader schema on every consumer, the answer
// is: none of them, in any order.
//
// The old service has not been redeployed and writes orders without a currency.
// The new one writes them with one. Each message carries the identifier of the
// schema it was written with, the registry turns that back into a schema, and
// Avro resolves it onto the schema the reader holds: a field the writer added is
// skipped, and a field the writer never wrote is filled in from the reader's
// default.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/avro
package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	avrocodec "github.com/AceMQ-Company/acemq-go-amqp/codec/avro"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const (
	exchange = "go-schema-orders"
	oldQueue = "go-schema-old-reader"
	newQueue = "go-schema-new-reader"

	// subject groups the versions of one message type; conventionally the type.
	subject = "go.order.placed"
)

// What every producer and consumer agreed on last year.
const v1 = `{
  "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "total",    "type": "double"}
  ]}`

// A field added, with a default. The default is not decoration: without one this
// is not a backwards-compatible change, and a v2 reader meeting a v1 message has
// nothing to put in the field.
const v2 = `{
  "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "total",    "type": "double"},
    {"name": "currency", "type": "string", "default": "EUR"}
  ]}`

// The same addition done wrong.
const v2NoDefault = `{
  "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "total",    "type": "double"},
    {"name": "currency", "type": "string"}
  ]}`

// OrderV1 is the old service's idea of the message. It has no Currency, and it is
// about to be handed messages that have one.
type OrderV1 struct {
	OrderID string  `avro:"order_id"`
	Total   float64 `avro:"total"`
}

// OrderV2 is the new service's.
type OrderV2 struct {
	OrderID  string  `avro:"order_id"`
	Total    float64 `avro:"total"`
	Currency string  `avro:"currency"`
}

// seen is one delivery: what the reader decoded, and the identifier of the schema
// it was written with, read off the front of the body.
type seen[T any] struct {
	order       T
	writtenWith int
}

// schemaIDOn reads Confluent's framing, which every AceMQ library writes: one
// zero byte, then four bytes of schema identifier, big-endian, then the body.
func schemaIDOn(body []byte) int { return int(binary.BigEndian.Uint32(body[1:5])) }

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Not a registry in the sense that matters: nothing is shared between
	// processes, so a consumer cannot look up a schema a producer registered
	// somewhere else, which is the entire point of having one. It is here to show
	// the shape. patterns.NewSQLSchemaRegistry outlives a process, and the wire
	// framing is Confluent's, so theirs works too.
	registry := patterns.NewInMemorySchemaRegistry()

	// Two services, each holding the schema it was written against. The schema
	// is what each writes with; ReaderSchema is what each resolves every message
	// it reads onto. Leave ReaderSchema out and a consumer reads whatever shape
	// the writer used — see the end of this file for what that costs.
	oldService, err := avrocodec.Registered(registry, subject, v1, avrocodec.ReaderSchema(v1))
	if err != nil {
		log.Fatal(err)
	}
	newService, err := avrocodec.Registered(registry, subject, v2, avrocodec.ReaderSchema(v2))
	if err != nil {
		log.Fatal(err)
	}

	mq, err := acemq.Connect(ctx, brokerURL(), acemq.WithOrigin("examples@10-schema-evolution"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareExchange(ctx, exchange, "topic"); err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{oldQueue, newQueue} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
		if err := mq.Bind(ctx, q, exchange, "order.placed"); err != nil {
			log.Fatal(err)
		}
	}

	// Every order goes to both services, so each reads one message written by
	// itself and one written by the other.
	var mu sync.Mutex
	var oldRead []seen[OrderV1]
	var newRead []seen[OrderV2]
	var all sync.WaitGroup
	all.Add(4)

	oldConsumer, err := acemq.Consume(ctx, mq, oldQueue,
		func(_ context.Context, m acemq.Message[OrderV1]) acemq.Ack {
			mu.Lock()
			oldRead = append(oldRead, seen[OrderV1]{m.Payload, schemaIDOn(m.Body)})
			mu.Unlock()
			all.Done()
			return acemq.Accept()
		}, acemq.ConsumeWith(oldService))
	if err != nil {
		log.Fatal(err)
	}
	defer oldConsumer.Close()

	newConsumer, err := acemq.Consume(ctx, mq, newQueue,
		func(_ context.Context, m acemq.Message[OrderV2]) acemq.Ack {
			mu.Lock()
			newRead = append(newRead, seen[OrderV2]{m.Payload, schemaIDOn(m.Body)})
			mu.Unlock()
			all.Done()
			return acemq.Accept()
		}, acemq.ConsumeWith(newService))
	if err != nil {
		log.Fatal(err)
	}
	defer newConsumer.Close()

	// ---- the old service, which has never heard of a currency --------------

	if err := acemq.NewPublisher[OrderV1](mq, exchange, "order.placed",
		acemq.PublishWith[OrderV1](oldService)).
		Send(ctx, OrderV1{OrderID: "A-1", Total: 42}, acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}

	// ---- and the new one, which has been redeployed ------------------------

	if err := acemq.NewPublisher[OrderV2](mq, exchange, "order.placed",
		acemq.PublishWith[OrderV2](newService)).
		Send(ctx, OrderV2{OrderID: "B-2", Total: 99.5, Currency: "GBP"},
			acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}

	arrived := make(chan struct{})
	go func() { all.Wait(); close(arrived) }()
	select {
	case <-arrived:
	case <-ctx.Done():
		log.Fatalf("expected two orders on each reader, saw %d and %d", len(oldRead), len(newRead))
	}

	mu.Lock()
	byOrder1 := map[string]seen[OrderV1]{}
	for _, s := range oldRead {
		byOrder1[s.order.OrderID] = s
	}
	byOrder2 := map[string]seen[OrderV2]{}
	for _, s := range newRead {
		byOrder2[s.order.OrderID] = s
	}
	mu.Unlock()

	log.Printf("%-7s %-13s %s", "reader", "written with", "decoded")
	for _, id := range []string{"A-1", "B-2"} {
		log.Printf("%-7s id %-10d %+v", "v1", byOrder1[id].writtenWith, byOrder1[id].order)
	}
	for _, id := range []string{"A-1", "B-2"} {
		log.Printf("%-7s id %-10d %+v", "v2", byOrder2[id].writtenWith, byOrder2[id].order)
	}

	// ---- a restart registers again, and makes no new version ---------------
	//
	// A schema is identified by a hash of its exact bytes, so the same definition
	// registered again comes back with the same identifier. Without that, a
	// service that registers on every start adds a version per restart and the
	// version number stops meaning anything.
	restarted, err := avrocodec.Registered(registry, subject, v2, avrocodec.ReaderSchema(v2))
	if err != nil {
		log.Fatal(err)
	}
	againBody, err := restarted.Encode(OrderV2{OrderID: "C-3", Total: 1, Currency: "USD"})
	if err != nil {
		log.Fatal(err)
	}
	versions, err := registry.Versions(ctx, subject)
	if err != nil {
		log.Fatal(err)
	}
	log.Println()
	log.Printf("versions of %s after a restart: %d", subject, len(versions))
	for _, v := range versions {
		log.Printf("  id=%d version=%d fingerprint=%s…", v.ID, v.Version, v.Fingerprint[:12])
	}

	// ---- and the two ways to get it wrong ----------------------------------

	v1Body, err := oldService.Encode(OrderV1{OrderID: "D-4", Total: 7})
	if err != nil {
		log.Fatal(err)
	}

	// A consumer on the new code with no ReaderSchema. It decodes against the
	// writer's schema, which has no currency, so the field arrives as Go's zero
	// value — not the "EUR" the schema promised. Nothing fails; the order simply
	// has no currency.
	writersShape, err := avrocodec.Registered(registry, subject, v2)
	if err != nil {
		log.Fatal(err)
	}
	var unresolved OrderV2
	if err := writersShape.Decode(v1Body, &unresolved); err != nil {
		log.Fatal(err)
	}
	log.Println()
	log.Printf("no reader schema:  %+v", unresolved)

	// The same field added without a default. There is nothing Avro can put there
	// when an old producer omits it, so the read is refused — which in a
	// deployment means every consumer breaking the moment it is rolled out ahead
	// of the producers. Refused by name, with both schemas, rather than decoded
	// into a value that is wrong.
	strict, err := avrocodec.Registered(registry, subject, v2NoDefault,
		avrocodec.ReaderSchema(v2NoDefault))
	if err != nil {
		log.Fatal(err)
	}
	var never OrderV2
	refusal := strict.Decode(v1Body, &never)
	if refusal != nil {
		firstLine, _, _ := strings.Cut(refusal.Error(), "\n")
		log.Printf("no default:        %s", firstLine)
	}

	// ---- what all of that has to say, checked ------------------------------

	v1ID, v2ID := byOrder1["A-1"].writtenWith, byOrder1["B-2"].writtenWith

	// Every message is framed with the schema it was written with, whatever the
	// reader holds. That identifier is the only thing on the wire that makes any
	// of the rest work.
	if v1ID == v2ID || byOrder2["A-1"].writtenWith != v1ID || byOrder2["B-2"].writtenWith != v2ID {
		log.Fatalf("the messages are not framed with their writers' schemas: v1 read %d/%d, v2 read %d/%d",
			v1ID, v2ID, byOrder2["A-1"].writtenWith, byOrder2["B-2"].writtenWith)
	}

	// The direction that costs money to get wrong: a producer already on the new
	// schema, a consumer still on the old one. The field it does not know is
	// skipped, and the fields it does know are the right way round.
	if got := byOrder1["B-2"].order; got != (OrderV1{OrderID: "B-2", Total: 99.5}) {
		log.Fatalf("the v1 reader misread a v2 message: %+v", got)
	}
	if got := byOrder1["A-1"].order; got != (OrderV1{OrderID: "A-1", Total: 42}) {
		log.Fatalf("the v1 reader misread its own message: %+v", got)
	}

	// And the other way, which is what the default in v2 and the ReaderSchema on
	// the consumer are for.
	if got := byOrder2["A-1"].order; got != (OrderV2{OrderID: "A-1", Total: 42, Currency: "EUR"}) {
		log.Fatalf("the reader's default was not applied: %+v", got)
	}
	if got := byOrder2["B-2"].order; got.Currency != "GBP" {
		log.Fatalf("the v2 reader lost the currency the v2 writer wrote: %+v", got)
	}

	if len(versions) != 2 || versions[0].ID != v1ID || versions[1].ID != v2ID {
		log.Fatalf("the subject should hold exactly v1 and v2 after a restart: %+v", versions)
	}
	if schemaIDOn(againBody) != v2ID {
		log.Fatalf("the restarted service wrote id %d rather than reusing %d", schemaIDOn(againBody), v2ID)
	}

	if unresolved.Currency != "" {
		log.Fatalf("without a reader schema the default should not be applied, got %q", unresolved.Currency)
	}
	if refusal == nil {
		log.Fatalf("a field added without a default was read anyway: %+v", never)
	}

	if err := oldConsumer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := newConsumer.Close(); err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{oldQueue, newQueue} {
		for _, name := range []string{q, acemq.DeadLetterQueue(q), acemq.ParkedQueue(q)} {
			if err := mq.DeleteQueue(ctx, name); err != nil {
				log.Fatal(err)
			}
		}
	}
}

// brokerURL is the compose broker unless ACEMQ_URL names another.
//
// Repeated in every example rather than shared: each directory is meant to be
// readable on its own, and a helper somewhere else is one more thing to find.
func brokerURL() string {
	if url := os.Getenv("ACEMQ_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}
