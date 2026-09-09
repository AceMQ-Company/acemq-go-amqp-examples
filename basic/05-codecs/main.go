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
// Four text formats on one queue, read by one consumer, and the two codecs that
// do not interpret anything.
//
//	docker compose up -d
//	go run ./basic/05-codecs
//
// This is what a format migration actually looks like. The producers change one
// at a time, the queue carries several spellings at once, and the consumer has
// to read whatever turns up. A CompositeCodec is that consumer: it offers the
// message's content type to each codec in turn and the first that says it can
// read the body gets it.
//
// YAML and TOML are modules of their own — the core library depends on one
// package and nothing else, so a service that speaks JSON never resolves a YAML
// parser. XML is in the core because encoding/xml is in the standard library.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/yaml
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/toml
//
// Avro and protobuf are the other two the library ships, and they are somewhere
// else on purpose: neither is readable without the schema that wrote it. See
// intermediate/07-binary-codecs.
package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"log"
	"os"
	"sort"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	tomlcodec "github.com/AceMQ-Company/acemq-go-amqp/codec/toml"
	xmlcodec "github.com/AceMQ-Company/acemq-go-amqp/codec/xml"
	yamlcodec "github.com/AceMQ-Company/acemq-go-amqp/codec/yaml"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const (
	structured = "go-codecs.orders"
	lines      = "go-codecs.lines"
	blobs      = "go-codecs.blobs"
)

// OrderPlaced carries four tag sets, and that is the first thing worth noticing.
//
// Every encoder has its own opinion about what a Go field is called on the wire,
// and none of them reads another's tags: encoding/json would send TotalCents,
// gopkg.in/yaml.v3 would send totalcents, BurntSushi/toml would send TotalCents
// again and encoding/xml would send TotalCents inside a root element named after
// the type. Four different messages for one struct. Naming the field in each tag
// is how a Go producer and a Java consumer end up talking about the same field.
//
// XMLName is what fixes the root element; without it the document is named after
// the Go type, which no other language would guess. It is excluded from the
// other three, because it is a marker for one encoder rather than a field of the
// message.
type OrderPlaced struct {
	XMLName    xml.Name `json:"-" yaml:"-" toml:"-" xml:"order"`
	OrderID    string   `json:"orderId" yaml:"orderId" toml:"orderId" xml:"orderId"`
	TotalCents int64    `json:"totalCents" yaml:"totalCents" toml:"totalCents" xml:"totalCents"`
	Tenant     string   `json:"tenant" yaml:"tenant" toml:"tenant" xml:"tenant"`
}

// arrival is one decoded message, kept so the checks at the end can look at all
// of them together.
type arrival struct {
	contentType string
	bytes       int
	order       OrderPlaced
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The order matters only where two codecs would answer for the same content
	// type, and none of these do. JSON is first because the first codec is also
	// the one the connection writes when nothing else is asked for.
	//
	// acemq.BytesCodec is deliberately not in this list. It answers for every
	// content type there is, so from anywhere in a composite it wins every
	// message — which is why it has to be asked for by name, as it is further
	// down.
	reader := acemq.NewCompositeCodec(
		acemq.JSONCodec{}, yamlcodec.Codec{}, tomlcodec.Codec{}, xmlcodec.Codec{})

	mq, err := acemq.Connect(ctx, brokerURL(),
		acemq.WithCodec(reader),
		acemq.WithOrigin("examples@05-codecs"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	for _, queue := range []string{structured, lines, blobs} {
		if err := mq.DeclareQueue(ctx, queue); err != nil {
			log.Fatal(err)
		}
	}

	// ---- four formats, one queue, one consumer -----------------------------

	arrived := make(chan arrival, 4)
	orders, err := acemq.Consume(ctx, mq, structured,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- arrival{contentType: m.ContentType, bytes: len(m.Body), order: m.Payload}
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer orders.Close()

	order := OrderPlaced{OrderID: "A-7", TotalCents: 4250, Tenant: "acme"}

	// A codec per publisher, which is how a migration arrives: one producer at a
	// time changes what it writes, and none of them agree to do it on the same
	// day. The connection's codec is what reads; PublishWith is what writes.
	writers := []struct {
		name  string
		codec acemq.Codec
	}{
		{"json", acemq.JSONCodec{}},
		{"yaml", yamlcodec.Codec{}},
		{"toml", tomlcodec.Codec{}},
		{"xml", xmlcodec.Codec{}},
	}
	for _, w := range writers {
		publisher := acemq.NewPublisher[OrderPlaced](mq, "", structured,
			acemq.PublishWith[OrderPlaced](w.codec))
		if err := publisher.Send(ctx, order, acemq.MessageType("order.placed")); err != nil {
			log.Fatalf("publishing %s: %v", w.name, err)
		}
		log.Printf("published %-4s as %s", w.name, w.codec.ContentType())
	}

	seen := make([]arrival, 0, len(writers))
	for len(seen) < len(writers) {
		select {
		case a := <-arrived:
			seen = append(seen, a)
		case <-ctx.Done():
			log.Fatalf("expected %d messages, saw %d", len(writers), len(seen))
		}
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i].contentType < seen[j].contentType })

	log.Println()
	log.Println("content type                bytes  decoded")
	for _, a := range seen {
		log.Printf("%-26s %5d  %s %d %s",
			a.contentType, a.bytes, a.order.OrderID, a.order.TotalCents, a.order.Tenant)
	}

	// ---- text, which is text rather than a structure -----------------------

	// StringCodec is for a message that really is a line of text — a log line, a
	// command somebody typed. Anything with fields wants JSON.
	text := make(chan string, 1)
	notes, err := acemq.Consume(ctx, mq, lines,
		func(_ context.Context, m acemq.Message[string]) acemq.Ack {
			text <- m.Payload
			return acemq.Accept()
		}, acemq.ConsumeWith(acemq.StringCodec{}))
	if err != nil {
		log.Fatal(err)
	}
	defer notes.Close()

	const note = "order A-7 was placed by hand"
	if err := acemq.NewPublisher[string](mq, "", lines,
		acemq.PublishWith[string](acemq.StringCodec{})).Send(ctx, note); err != nil {
		log.Fatal(err)
	}

	var gotText string
	select {
	case gotText = <-text:
	case <-ctx.Done():
		log.Fatal("the text message never arrived")
	}

	// ---- bytes, which are not interpreted at all ---------------------------

	// BytesCodec hands the body over exactly as it arrived. For a payload
	// something else already encoded — an image, another system's serializer —
	// and for reading a message whose type this process does not have. The
	// payload below is not valid UTF-8, which is the point: nothing here is
	// allowed to decide it is text and repair it.
	raw := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0xff, 0xfe, 0x00, 0x01}

	blob := make(chan []byte, 1)
	files, err := acemq.Consume(ctx, mq, blobs,
		func(_ context.Context, m acemq.Message[[]byte]) acemq.Ack {
			blob <- m.Payload
			return acemq.Accept()
		}, acemq.ConsumeWith(acemq.BytesCodec{}))
	if err != nil {
		log.Fatal(err)
	}
	defer files.Close()

	if err := acemq.NewPublisher[[]byte](mq, "", blobs,
		acemq.PublishWith[[]byte](acemq.BytesCodec{})).Send(ctx, raw); err != nil {
		log.Fatal(err)
	}

	var gotBytes []byte
	select {
	case gotBytes = <-blob:
	case <-ctx.Done():
		log.Fatal("the byte message never arrived")
	}

	log.Println()
	log.Printf("text:  %q", gotText)
	log.Printf("bytes: % x", gotBytes)

	// ---- what all of that has to say, checked ------------------------------

	if len(seen) != len(writers) {
		log.Fatalf("expected %d structured messages, saw %d", len(writers), len(seen))
	}

	// Every format carried the same order. This is the claim: the consumer was
	// never told which format any of these messages was in, and read all four.
	types := make([]string, 0, len(seen))
	sizes := map[int]bool{}
	for _, a := range seen {
		if a.order.OrderID != order.OrderID ||
			a.order.TotalCents != order.TotalCents ||
			a.order.Tenant != order.Tenant {
			log.Fatalf("%s decoded to %+v, not %+v", a.contentType, a.order, order)
		}
		types = append(types, a.contentType)
		sizes[a.bytes] = true
	}

	// And they really were four different encodings rather than four copies of
	// the same one. Four distinct content types, and bodies of more than one
	// length — a check that would fail if a publisher had quietly fallen back to
	// the connection's codec.
	for i := 1; i < len(types); i++ {
		if types[i] == types[i-1] {
			log.Fatalf("two messages arrived as %s; a publisher did not use its own codec", types[i])
		}
	}
	if len(sizes) < 2 {
		log.Fatal("every body was the same length, so these are not four encodings")
	}

	if gotText != note {
		log.Fatalf("the text message came back as %q", gotText)
	}
	// Byte for byte, including the bytes that are not valid UTF-8. A codec that
	// had decided this was text would have replaced 0xff 0xfe with U+FFFD and
	// the length would differ.
	if !bytes.Equal(gotBytes, raw) {
		log.Fatalf("the bytes came back as % x, not % x", gotBytes, raw)
	}

	// The gate underneath all of it: a codec answers for the content types it can
	// actually read and refuses the rest. Without that, whichever codec came
	// first in the list would be handed every message, and a YAML body would
	// reach the TOML parser.
	if (tomlcodec.Codec{}).CanDecode(yamlcodec.ContentType) {
		log.Fatal("the TOML codec claimed a YAML message")
	}
	// And none of them claims a message whose sender set no content type. Only
	// JSON does that, because JSON is what the library writes when nobody says
	// otherwise — the others would be guessing.
	if (xmlcodec.Codec{}).CanDecode("") {
		log.Fatal("the XML codec claimed a message with no content type")
	}
	if !reader.CanDecode(tomlcodec.ContentType) {
		log.Fatal("the composite would not read TOML, so it is not reading by content type")
	}

	// The queues go, so a second run of this example starts where the first did.
	for _, closer := range []*acemq.Consumer{orders, notes, files} {
		if err := closer.Close(); err != nil {
			log.Fatal(err)
		}
	}
	for _, queue := range []string{structured, lines, blobs} {
		if err := mq.DeleteQueue(ctx, queue); err != nil {
			log.Fatal(err)
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
