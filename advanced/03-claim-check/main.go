// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Keeping a large payload off the broker, and only when it is worth it.
//
//	docker compose up -d
//	go run ./advanced/03-claim-check
//
// A scanned medical report is tens of megabytes. Putting it on a queue is
// possible and is a mistake: it fills the broker's memory, it is copied to every
// bound queue, it makes a dead-letter queue impossible to inspect, and it turns
// a broker into a filesystem with worse tools. What travels instead is a claim
// check — the payload goes to a store and the message carries the key.
//
// The interesting part is not the offloading, which is obvious. It is the
// threshold, which is why this example publishes two reports one byte apart. A
// small message still travels inline, byte for byte what the delegate codec
// wrote, because offloading a two-hundred-byte message turns one broker round
// trip into a store round trip and a broker round trip — an unconditional claim
// check makes the common case slower to fix the rare one.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-claim-check.reports"

type Report struct {
	ReportID string `json:"reportId"`
	Scan     string `json:"scan"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A directory rather than the in-memory store the library also ships. The
	// in-memory one holds payloads in the publisher's own memory, which is where
	// they were going to be anyway: it takes them off the broker and does
	// nothing else, and every consumer in another process gets "the claim check
	// is not in the store". It is for a test where the publisher and the
	// consumer are the same process. A claim check that does not outlive the
	// process that wrote it is a message nobody else can read.
	directory, err := os.MkdirTemp("", "acemq-claim-check-*")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("the store is %s", directory)

	// The codec wraps another one. What gets stored or inlined is whatever the
	// delegate wrote, so the content type on the wire stays application/json: a
	// claim-checked message is still a document, it is a document that is
	// somewhere else, and a consumer without the store gets a clear failure
	// rather than a parser error.
	writer := patterns.ClaimCheck(acemq.JSONCodec{}, patterns.NewFilesystemClaimCheckStore(directory))
	log.Printf("offloading at %d bytes and above", writer.Threshold())

	mq, err := acemq.Connect(ctx, brokerURL(), acemq.WithCodec(writer))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}

	// A second codec over a second store, pointed at the same directory. It
	// shares no object with the publisher's — only the directory — which is what
	// the pattern actually requires and what an in-memory store cannot give you.
	reader := patterns.ClaimCheck(acemq.JSONCodec{}, patterns.NewFilesystemClaimCheckStore(directory))

	arrived := make(chan acemq.Message[Report], 2)
	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[Report]) acemq.Ack {
			arrived <- m
			return acemq.Accept()
		},
		acemq.ConsumeWith(reader))
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// One byte below the threshold and exactly on it. The comparison is strictly
	// less than, in this library and in Java, Python and Ruby: a payload sitting
	// on the boundary must not be inline from one library and checked from
	// another, which is the sort of disagreement that shows up as one consumer
	// in five failing.
	small := sized("small", patterns.DefaultClaimCheckThreshold-1)
	large := sized("large", patterns.DefaultClaimCheckThreshold)

	reports := acemq.NewPublisher[Report](mq, "", queue)
	if err := reports.Send(ctx, small); err != nil {
		log.Fatal(err)
	}
	if err := reports.Send(ctx, large); err != nil {
		log.Fatal(err)
	}

	first := receive(ctx, arrived)
	second := receive(ctx, arrived)

	log.Println()
	log.Println("            encoded   on the wire   framing")
	for _, m := range []acemq.Message[Report]{first, second} {
		log.Printf("%7s   %7d   %11d   %s",
			m.Payload.ReportID, encodedSize(m.Payload), len(m.Body), framing(m.Body))
	}

	// ---- what the two rows have to say, checked ----------------------------

	if first.Payload.ReportID != "small" || second.Payload.ReportID != "large" {
		log.Fatalf("the reports arrived out of order: %s then %s",
			first.Payload.ReportID, second.Payload.ReportID)
	}

	// Below the threshold: three bytes of framing and then exactly what the
	// delegate wrote. Nothing was stored, and a consumer reading it needs no
	// store at all.
	if patterns.IsClaimChecked(first.Body) {
		log.Fatal("the small report was offloaded, and it is below the threshold")
	}
	if want := 3 + encodedSize(small); len(first.Body) != want {
		log.Fatalf("the small report put %d bytes on the wire, not %d", len(first.Body), want)
	}
	if first.Payload.Scan != small.Scan {
		log.Fatal("the small report did not survive the round trip")
	}

	// At the threshold: three bytes of framing and a key, and the payload is in
	// the store. The message is two orders of magnitude smaller than the one
	// before it and carries the same document.
	if !patterns.IsClaimChecked(second.Body) {
		log.Fatal("the large report travelled inline, and it is at the threshold")
	}
	key := patterns.ClaimKeyOf(second.Body)
	if key == "" {
		log.Fatal("a checked message with no key is a message nobody can redeem")
	}
	if want := 3 + len(key); len(second.Body) != want {
		log.Fatalf("the large report put %d bytes on the wire, not %d", len(second.Body), want)
	}
	if second.Payload.Scan != large.Scan {
		log.Fatal("the large report did not survive the round trip")
	}

	// ClaimKeyOf is for the operator in front of a dead-letter queue: which
	// object does this message need, and is it still in the store? Answering
	// that from the message alone is the difference between a five-minute check
	// and restoring a backup.
	stored, err := os.ReadFile(filepath.Join(directory, key))
	if err != nil {
		log.Fatal(err)
	}
	encoded, err := acemq.JSONCodec{}.Encode(large)
	if err != nil {
		log.Fatal(err)
	}
	if !bytes.Equal(stored, encoded) {
		log.Fatalf("the store holds %d bytes; the report encodes to %d", len(stored), len(encoded))
	}
	log.Println()
	log.Printf("%s holds the %d bytes the broker never saw", key, len(stored))

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := mq.DeleteQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
	}

	// The queue is gone and the payload is not, which is the whole arrangement.
	// A third store, built after everything above was closed, redeems the same
	// key — nothing but the directory was ever shared.
	later, found, err := patterns.NewFilesystemClaimCheckStore(directory).Get(key)
	if err != nil {
		log.Fatal(err)
	}
	if !found || len(later) != len(stored) {
		log.Fatalf("the payload did not outlive the queue: found=%t", found)
	}
	log.Println()
	log.Println("the queue is deleted and the payload is still there. Nothing here")
	log.Println("removes it: deleting on read breaks a second consumer and deleting")
	log.Println("on acknowledgement breaks a replay, so when a payload may go is a")
	log.Println("retention decision, and it belongs to whoever owns the data. The")
	log.Println("store has to outlast every queue, every dead-letter queue and any")
	log.Println("replay somebody might do by hand — a key whose object expired is")
	log.Println("worse than a lost message, because it still looks like a message.")
}

// sized builds a report whose JSON encoding is exactly size bytes.
//
// The point of the example is a payload on each side of the threshold, so the
// sizes have to be exact rather than approximately large and approximately
// small.
func sized(id string, size int) Report {
	empty, err := acemq.JSONCodec{}.Encode(Report{ReportID: id})
	if err != nil {
		log.Fatal(err)
	}
	report := Report{ReportID: id, Scan: strings.Repeat("x", size-len(empty))}
	if got := encodedSize(report); got != size {
		log.Fatalf("meant to build %d bytes and built %d", size, got)
	}
	return report
}

func encodedSize(report Report) int {
	encoded, err := acemq.JSONCodec{}.Encode(report)
	if err != nil {
		log.Fatal(err)
	}
	return len(encoded)
}

// framing renders the three bytes a consumer decides on.
//
// The framing rather than a header is the contract, because a header can be
// stripped by a shovel or a federation link and the body cannot — and because a
// header that is present or absent cannot say "inline" about a message written
// before this codec existed.
func framing(body []byte) string {
	if len(body) < 3 {
		return "not framed"
	}
	kind := "inline"
	if patterns.IsClaimChecked(body) {
		kind = "checked -> " + patterns.ClaimKeyOf(body)
	}
	return fmt.Sprintf("%02X %02X %02X  %s", body[0], body[1], body[2], kind)
}

func receive(ctx context.Context, arrived <-chan acemq.Message[Report]) acemq.Message[Report] {
	select {
	case m := <-arrived:
		return m
	case <-ctx.Done():
		log.Fatal("a report never arrived")
		return acemq.Message[Report]{}
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
