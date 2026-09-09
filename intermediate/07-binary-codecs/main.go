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
// Avro and protobuf: the two formats where the schema is not in the message.
//
//	docker compose up -d
//	go run ./intermediate/07-binary-codecs
//
// JSON, YAML, TOML and XML carry their field names, so a consumer can read one
// knowing nothing beyond the content type — that is basic/05-codecs. These two
// do not. Protobuf's schema lives in a generated type; Avro's lives either in the
// consumer or, with a registry, behind a small identifier carried in front of the
// bytes.
//
// Which is why neither codec will answer for a message that arrived with no
// content type at all. Arbitrary bytes parse as some protobuf message more often
// than not, and Avro read against the wrong schema returns a record where every
// field is populated and wrong. A codec that volunteered there would report
// nonsense as a success.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/avro
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/protobuf
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	avrocodec "github.com/AceMQ-Company/acemq-go-amqp/codec/avro"
	pbcodec "github.com/AceMQ-Company/acemq-go-amqp/codec/protobuf"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
	"google.golang.org/protobuf/types/known/structpb"
)

const queue = "go-binary.readings"

// The schema a producer deployed last year writes.
const schemaV1 = `{
  "type": "record", "name": "Reading", "namespace": "acemq.examples",
  "fields": [
    {"name": "sensor",  "type": "string"},
    {"name": "celsius", "type": "double"}
  ]}`

// And the one a producer deployed this morning writes: the same record with a
// field added. Adding a field is the change that has to be survivable, because
// it is the change that always happens.
const schemaV2 = `{
  "type": "record", "name": "Reading", "namespace": "acemq.examples",
  "fields": [
    {"name": "sensor",  "type": "string"},
    {"name": "celsius", "type": "double"},
    {"name": "unit",    "type": "string", "default": "C"}
  ]}`

// Reading is the consumer's idea of the message, which is schemaV1's. It has no
// Unit field, and it is about to be handed a message that has one.
type Reading struct {
	Sensor  string  `avro:"sensor"`
	Celsius float64 `avro:"celsius"`
}

// arrival is one delivery, undecoded, plus what this process made of it.
type arrival struct {
	contentType string
	bytes       []byte
	described   string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A registry is shared state, and an in-memory one is shared with nobody —
	// which is the whole point of having a registry. It is here to show the
	// shape of the thing; patterns.NewSQLSchemaRegistry is the one that outlives
	// a process, and Confluent's is the one a fleet already has.
	registry := patterns.NewInMemorySchemaRegistry()

	// Registered rather than Of: each message carries a schema identifier, so a
	// consumer can read one written by a schema it has never seen. Of is the
	// smaller and more brittle option — the consumer must already hold the exact
	// schema the producer used, so changing it means deploying both ends
	// together.
	oldProducer, err := avrocodec.Registered(registry, "acemq.examples.reading", schemaV1)
	if err != nil {
		log.Fatal(err)
	}
	newProducer, err := avrocodec.Registered(registry, "acemq.examples.reading", schemaV2)
	if err != nil {
		log.Fatal(err)
	}
	consumerCodec, err := avrocodec.Registered(registry, "acemq.examples.reading", schemaV1)
	if err != nil {
		log.Fatal(err)
	}

	mq, err := acemq.Connect(ctx, brokerURL(),
		acemq.WithOrigin("examples@07-binary-codecs"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}

	// One queue carrying two formats, and one consumer taking the bytes.
	//
	// A composite codec picks a codec by content type and hands back a value, and
	// that works when every message on the queue decodes into the same Go type.
	// These two do not: one is a record, the other a protobuf message. Consume[T]
	// has one T, so the queue is read as bytes and the content type dispatches —
	// which is what Message.Body is for, and what CanDecode is for.
	arrived := make(chan arrival, 4)
	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[[]byte]) acemq.Ack {
			switch {
			case consumerCodec.CanDecode(m.ContentType):
				var reading Reading
				if err := consumerCodec.Decode(m.Body, &reading); err != nil {
					// Fatal errors are the codec saying the same bytes will fail
					// the same way for ever, so the message is parked rather
					// than retried until it ages out.
					return acemq.Park(err)
				}
				arrived <- arrival{m.ContentType, m.Body,
					fmt.Sprintf("sensor=%s celsius=%.2f", reading.Sensor, reading.Celsius)}

			case (pbcodec.Codec{}).CanDecode(m.ContentType):
				reading := &structpb.Struct{}
				if err := (pbcodec.Codec{}).Decode(m.Body, reading); err != nil {
					return acemq.Park(err)
				}
				arrived <- arrival{m.ContentType, m.Body,
					fmt.Sprintf("%v", reading.AsMap())}

			default:
				return acemq.Park(fmt.Errorf(
					"acemq-example: no codec here reads %q", m.ContentType))
			}
			return acemq.Accept()
		}, acemq.ConsumeWith(acemq.BytesCodec{}))
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// ---- the producer that has not been redeployed -------------------------

	if err := acemq.NewPublisher[Reading](mq, "", queue,
		acemq.PublishWith[Reading](oldProducer)).
		Send(ctx, Reading{Sensor: "roof-1", Celsius: 21.5},
			acemq.MessageType("reading.taken")); err != nil {
		log.Fatal(err)
	}
	log.Printf("published avro v1 as %s", oldProducer.ContentType())

	// ---- and the one that has ----------------------------------------------

	// A map rather than a struct, because this producer's shape is not this
	// process's shape: it is standing in for a service deployed from another
	// repository that has a field this one has never heard of.
	if err := acemq.NewPublisher[map[string]any](mq, "", queue,
		acemq.PublishWith[map[string]any](newProducer)).
		Send(ctx, map[string]any{"sensor": "roof-2", "celsius": 19.25, "unit": "C"},
			acemq.MessageType("reading.taken")); err != nil {
		log.Fatal(err)
	}
	log.Printf("published avro v2 as %s", newProducer.ContentType())

	// ---- and something protobuf --------------------------------------------

	// structpb.Struct is a generated protobuf type that ships with the protobuf
	// module, so this example needs no protoc and no build step. A real service
	// imports what protoc wrote for its own .proto file; the codec cannot tell
	// the difference, because there is none to tell.
	reading, err := structpb.NewStruct(map[string]any{"sensor": "roof-3", "celsius": 17.75})
	if err != nil {
		log.Fatal(err)
	}
	if err := acemq.NewPublisher[*structpb.Struct](mq, "", queue,
		acemq.PublishWith[*structpb.Struct](pbcodec.Codec{})).
		Send(ctx, reading, acemq.MessageType("reading.taken")); err != nil {
		log.Fatal(err)
	}
	log.Printf("published protobuf as %s", pbcodec.ContentType)

	seen := map[string]arrival{}
	for len(seen) < 3 {
		select {
		case a := <-arrived:
			seen[a.described] = a
		case <-ctx.Done():
			log.Fatalf("expected 3 messages, saw %d", len(seen))
		}
	}

	log.Println()
	log.Println("content type                     bytes  read as")
	for _, key := range sortedKeys(seen) {
		a := seen[key]
		log.Printf("%-30s %5d  %s", a.contentType, len(a.bytes), a.described)
	}

	versions, err := registry.Versions(ctx, "acemq.examples.reading")
	if err != nil {
		log.Fatal(err)
	}
	log.Println()
	log.Printf("schema versions registered: %d", len(versions))
	for _, v := range versions {
		log.Printf("  id=%d version=%d fingerprint=%s…", v.ID, v.Version, v.Fingerprint[:12])
	}

	// ---- what all of that has to say, checked ------------------------------

	// The consumer holds schemaV1 and read a message written with schemaV2,
	// dropping the field it has never heard of. Nothing about the producer was
	// coordinated with anything about the consumer: the identifier in front of
	// the body was enough.
	v2, ok := seen["sensor=roof-2 celsius=19.25"]
	if !ok {
		log.Fatalf("the message from the new producer did not arrive readable: %v", sortedKeys(seen))
	}
	if _, ok := seen["sensor=roof-1 celsius=21.50"]; !ok {
		log.Fatalf("the message from the old producer did not arrive readable: %v", sortedKeys(seen))
	}
	if _, ok := seen["map[celsius:17.75 sensor:roof-3]"]; !ok {
		log.Fatalf("the protobuf message did not arrive readable: %v", sortedKeys(seen))
	}

	// Two schemas, two versions of one subject, and the identifier on the wire
	// is the registry's. This is the framing Confluent's clients and the Java
	// library write: one zero byte, then four bytes of identifier, big-endian.
	if len(versions) != 2 {
		log.Fatalf("the registry holds %d versions of the subject, not 2", len(versions))
	}
	if v2.bytes[0] != 0x00 {
		log.Fatalf("the Avro body does not start with the framing byte: % x", v2.bytes[:5])
	}
	id := int(binary.BigEndian.Uint32(v2.bytes[1:5]))
	written, err := registry.ByID(ctx, id)
	if err != nil {
		log.Fatalf("schema %d is on the wire and not in the registry: %v", id, err)
	}
	if written.Version != 2 {
		log.Fatalf("the new producer's message names schema version %d", written.Version)
	}

	// And the gate that keeps the two Avro modes apart. A fixed-schema codec
	// handed a registry-framed body would read the five framing bytes as the
	// start of the first field, and Avro does not object: it returns a record
	// where every value is wrong, with no error and no log line. Refusing the
	// content type turns that into a message no codec claimed.
	fixed, err := avrocodec.Of(schemaV1)
	if err != nil {
		log.Fatal(err)
	}
	if fixed.CanDecode(avrocodec.RegisteredContentType) {
		log.Fatal("a fixed-schema Avro codec claimed a registry-framed message")
	}
	if consumerCodec.CanDecode(avrocodec.FixedContentType) {
		log.Fatal("a registered Avro codec claimed an unframed message")
	}

	// Neither format is self-describing, so neither guesses.
	if fixed.CanDecode("") || consumerCodec.CanDecode("") || (pbcodec.Codec{}).CanDecode("") {
		log.Fatal("a binary codec claimed a message whose sender set no content type")
	}

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := mq.DeleteQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
}

// sortedKeys keeps the printed table and the failure messages in one order.
func sortedKeys(m map[string]arrival) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
