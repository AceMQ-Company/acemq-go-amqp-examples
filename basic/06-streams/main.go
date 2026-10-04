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

// A log that is read rather than emptied.
//
//	docker compose up -d
//	go run ./basic/06-streams
//
// A queue forgets a message the moment somebody acknowledges it. A stream does
// not: acknowledging moves this reader along and nothing else, so the messages
// are still there for the next reader, and for one written next month. Where a
// reader starts, and where it carries on after a restart, are therefore
// questions the application answers — the broker keeps nobody's place.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const stream = "go-stream-orders"

type OrderPlaced struct {
	ID    string  `json:"id"`
	Total float64 `json:"total"`
}

// reader is one run over the stream: what it read, in order, and the offset of
// the last message it handled — the number a real service would write down.
type reader struct {
	mu         sync.Mutex
	read       []string
	checkpoint uint64
}

func (r *reader) add(m acemq.Message[OrderPlaced]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.read = append(r.read, m.Payload.ID)
	if offset, ok := patterns.StreamOffsetOf(m.Envelope); ok {
		r.checkpoint = offset
	}
}

func (r *reader) snapshot() ([]string, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.read), r.checkpoint
}

// until waits for a reader to have seen n messages. A deadline rather than a
// sleep: a stream that stopped delivering should fail the example rather than
// pass it slowly.
func (r *reader) until(ctx context.Context, n int) []string {
	for {
		read, _ := r.snapshot()
		if len(read) >= n {
			return read
		}
		select {
		case <-ctx.Done():
			log.Fatalf("expected %d messages, the reader saw %d: %v", n, len(read), read)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	// A stream is declared, not converted. x-queue-type is part of a queue's
	// identity, so a leftover of another shape under this name would be a
	// PRECONDITION_FAILED that does not mention streams — and a leftover stream
	// would hand this run everything every previous run wrote.
	if exists, err := mq.QueueExists(ctx, stream); err != nil {
		log.Fatal(err)
	} else if exists {
		if err := mq.DeleteQueue(ctx, stream); err != nil {
			log.Fatal(err)
		}
	}

	// Retention is the argument that matters. A queue's messages leave when they
	// are handled; a stream's leave when this says so, and a stream declared
	// without it grows until the disk is full — which on RabbitMQ is not a stream
	// problem but a memory-and-disk alarm that blocks every publisher on the node.
	//
	// SegmentBytes is here because retention discards a whole segment file at a
	// time. With RabbitMQ's 500 MB default, a stream told to keep an hour keeps
	// everything until it has half a gigabyte to drop.
	if err := patterns.DeclareStream(ctx, mq, stream, patterns.StreamRetention{
		MaxAge:       time.Hour,
		MaxBytes:     20 << 20,
		SegmentBytes: 1 << 20,
	}); err != nil {
		log.Fatal(err)
	}

	publisher := acemq.NewPublisher[OrderPlaced](mq, "", stream)
	for i := range 10 {
		if err := publisher.Send(ctx, OrderPlaced{ID: fmt.Sprintf("o-%d", i), Total: float64(i)}); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("wrote      10 orders to %s", stream)

	// ---- a projection being built, and stopped half way --------------------
	//
	// It has to see history, so it says FromFirst. A reader not told where to
	// start reads FromNext — right for a consumer joining a live system, and
	// silently wrong here: the projection would come up empty and look healthy.
	//
	// Stopping is the library's own recipe: settle what arrives with Accept but
	// do not advance the checkpoint, then close. Whatever the broker had already
	// pushed past the fifth message is accepted and not counted, so the
	// checkpoint says exactly where the work stopped, whatever the prefetch.
	first := &reader{}
	var stopping bool
	firstSub, err := patterns.ReadStream(ctx, mq, stream,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			if stopping {
				return acemq.Accept()
			}
			first.add(m)
			read, _ := first.snapshot()
			stopping = len(read) == 5
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Name: "go-projection"})
	if err != nil {
		log.Fatal(err)
	}
	first.until(ctx, 5)
	if err := firstSub.Close(); err != nil {
		log.Fatal(err)
	}
	read, checkpoint := first.snapshot()
	log.Printf("read       %d, then the reader stopped", len(read))
	log.Printf("checkpoint offset %d, from the x-stream-offset on the last one handled", checkpoint)

	// ---- carrying on --------------------------------------------------------
	//
	// The broker does not remember anybody's position — that is what makes a
	// stream cheap to read — so the offset is the application's to store, beside
	// whatever the projection wrote, and to hand back one past itself.
	resumed := &reader{}
	resumedSub, err := patterns.ReadStream(ctx, mq, stream,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			resumed.add(m)
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: patterns.FromOffset(checkpoint + 1), Name: "go-projection"})
	if err != nil {
		log.Fatal(err)
	}
	carriedOn := resumed.until(ctx, 5)
	if err := resumedSub.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("resumed    %v", carriedOn)

	// ---- a handler that asks for a retry ------------------------------------
	//
	// On a queue, Retry republishes the message onto the queue it came from. On a
	// stream that would append a second copy to the log, for every reader to see
	// now and on every replay afterwards, so ReadStream refuses it: the message is
	// parked on {stream}.parked as a copy, the handler's error explains why, and
	// the stream itself is not touched.
	retrier := &reader{}
	retrierSub, err := patterns.ReadStream(ctx, mq, stream,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			retrier.add(m)
			if m.Payload.ID == "o-3" {
				return acemq.Retry(errors.New("the ledger is down"))
			}
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Name: "go-retrier"})
	if err != nil {
		log.Fatal(err)
	}
	retrier.until(ctx, 10)
	if err := retrierSub.Close(); err != nil {
		log.Fatal(err)
	}
	parked, err := mq.MessageCount(ctx, acemq.ParkedQueue(stream))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("retry      refused: %d parked on %s", parked, acemq.ParkedQueue(stream))

	// What the handler's Retry became, read back off the parked copy: the reason
	// travels on the envelope, so whoever drains the parked queue finds out why
	// without having to find this process's log.
	copyOf, order, found, err := acemq.PullInto[OrderPlaced](ctx, mq, acemq.ParkedQueue(stream))
	if err != nil || !found {
		log.Fatalf("nothing could be read back from %s: found=%v %v", acemq.ParkedQueue(stream), found, err)
	}
	if err := copyOf.Ack(); err != nil {
		log.Fatal(err)
	}
	// The full reason is a paragraph naming both honest alternatives; its last
	// sentence is the handler's own.
	_, handlers, _ := strings.Cut(copyOf.Envelope.Error, "The handler's reason was: ")
	log.Printf("parked     %s, with the handler's reason kept: %q", order.ID, handlers)

	// ---- and the property a queue does not have -----------------------------
	//
	// Three readers have acknowledged every message they read. A fourth, attached
	// now, still finds all ten — and only ten, so the refused retry did not add
	// a copy behind them.
	auditor := &reader{}
	auditorSub, err := patterns.ReadStream(ctx, mq, stream,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			auditor.add(m)
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Name: "go-auditor"})
	if err != nil {
		log.Fatal(err)
	}
	auditor.until(ctx, 10)
	// Long enough for an eleventh to arrive if there were one.
	time.Sleep(500 * time.Millisecond)
	audited, _ := auditor.snapshot()
	if err := auditorSub.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("a new reader still saw all %d", len(audited))

	// ---- what all of that has to say, checked ------------------------------

	want := make([]string, 10)
	for i := range want {
		want[i] = fmt.Sprintf("o-%d", i)
	}
	if !slices.Equal(read, want[:5]) {
		log.Fatalf("FromFirst did not read the stream from the start, in order: %v", read)
	}
	// The checkpoint is the offset of the last message handled, not a count. On a
	// fresh stream offsets start at zero, so five handled is offset four.
	if checkpoint != 4 {
		log.Fatalf("the checkpoint is offset %d, not 4", checkpoint)
	}
	// Resuming from checkpoint+1 picks up exactly what the first reader did not
	// handle: no gap, and nothing handled twice.
	if !slices.Equal(carriedOn, want[5:]) {
		log.Fatalf("FromOffset(%d) did not carry on where the projection stopped: %v",
			checkpoint+1, carriedOn)
	}
	if parked != 1 {
		log.Fatalf("a refused retry should park one copy, %d are parked", parked)
	}
	// The parked copy names the refusal and keeps the handler's own reason, so
	// the person draining it learns both what happened and why it was asked for.
	if order.ID != "o-3" ||
		!strings.Contains(copyOf.Envelope.Error, "returned acemq.Retry") ||
		!strings.Contains(copyOf.Envelope.Error, "the ledger is down") {
		log.Fatalf("the parked copy does not explain itself: %s %q", order.ID, copyOf.Envelope.Error)
	}
	// The point of a stream rather than a queue, and the proof a Retry did not
	// append: three readers acknowledged everything, and a fourth reads the same
	// ten in the same order.
	if !slices.Equal(audited, want) {
		log.Fatalf("the stream did not survive being read: %v", audited)
	}

	for _, queue := range []string{stream, acemq.DeadLetterQueue(stream), acemq.ParkedQueue(stream)} {
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
