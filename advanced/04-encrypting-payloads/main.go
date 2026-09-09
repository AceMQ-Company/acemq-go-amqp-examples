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
// Message bodies the broker cannot read, a key rotated without an outage, and
// what one altered byte does.
//
//	docker compose up -d
//	go run ./advanced/04-encrypting-payloads
//
// crypto.Codec wraps another codec: the payload is encoded as usual and the
// bytes are then encrypted, so the broker, its disk, its backups and anybody
// reading its management interface see ciphertext. Nothing outside the standard
// library is needed — AES-GCM is in it.
//
// # What this does not protect
//
// Headers travel in the clear, and this example prints one to make that
// concrete. The envelope is how the library routes, retries and correlates, so
// it cannot be encrypted without the broker losing the ability to do its job. Do
// not put anything secret in a header.
//
// It does not authenticate the sender either. Anybody holding the key can write
// a message this codec will happily decrypt, so a key is a shared secret between
// everyone who may publish and everyone who may read, and nothing more.
//
// # The bytes are the family's
//
//	[0xAE][1 byte version][1 byte key id length][key id][12 byte nonce][ciphertext+tag]
//
// Java, .NET, Python and Ruby write exactly that, so a message encrypted here
// opens there given the same key. This library wrote a framing of its own up to
// v0.3.0, which no other could read; it still reads those bodies so a queue
// filled before the change can be drained, and never writes them.
package main

import (
	"context"
	"encoding/hex"
	"log"
	"os"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/crypto"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-crypto.orders"

type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	Customer   string `json:"customer"`
	TotalCents int64  `json:"totalCents"`
}

// arrival is a delivery as the handler saw it: decrypted payload, and the body
// exactly as it came off the wire.
type arrival struct {
	order OrderPlaced
	body  []byte
	env   acemq.Envelope
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two keys, because rotation needs an overlap. A keyring with one key cannot
	// rotate without an outage: there is no moment at which both the messages
	// already in the queue and the ones about to be written can be read.
	//
	// The keys are drawn here because this is an example. Real ones come from
	// somewhere that can hand them to several processes and take them back — a
	// secret manager, a mounted file, a KMS — and never from source control.
	january, err := crypto.NewKey("2026-01")
	if err != nil {
		log.Fatal(err)
	}
	april, err := crypto.NewKey("2026-04")
	if err != nil {
		log.Fatal(err)
	}

	// The first key is the one new messages are written with.
	keyring, err := crypto.NewKeyring(january, april)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("keyring holds %v, writing with 2026-01", keyring.IDs())

	sealed := crypto.Wrap(acemq.JSONCodec{}, keyring)

	mq, err := acemq.Connect(ctx, brokerURL(),
		acemq.WithCodec(sealed),
		acemq.WithOrigin("examples@04-encrypting-payloads"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	// The parked queue is where a body that will not decode ends up, and the
	// tampered message below has to land somewhere visible.
	if err := acemq.NewTopology().
		Queue(queue).
		DeadLetters(queue).
		Apply(ctx, mq); err != nil {
		log.Fatal(err)
	}

	arrived := make(chan arrival, 4)
	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- arrival{order: m.Payload, body: m.Body, env: m.Envelope}
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	orders := acemq.NewPublisher[OrderPlaced](mq, "", queue)

	// ---- written with the old key ------------------------------------------

	if err := orders.Send(ctx,
		OrderPlaced{OrderID: "A-7", Customer: "Ada Lovelace", TotalCents: 4250},
		acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}
	first := next(ctx, arrived)

	// ---- rotate ------------------------------------------------------------

	// The order rotation happens in: the new key was added to every keyring
	// first — that is what NewKeyring did above — so every consumer could
	// already read it. Only then does one producer start writing with it. Doing
	// it the other way round produces messages nothing can open.
	if err := keyring.Use("2026-04"); err != nil {
		log.Fatal(err)
	}
	log.Println("rotated: new messages are written with 2026-04")

	if err := orders.Send(ctx,
		OrderPlaced{OrderID: "A-8", Customer: "Grace Hopper", TotalCents: 1999},
		acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}
	second := next(ctx, arrived)

	log.Println()
	for _, a := range []arrival{first, second} {
		keyID, err := crypto.KeyIDOf(a.body)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s  key=%-8s  type=%-13s  body=%s…",
			a.order.OrderID, keyID, a.env.Type, hex.EncodeToString(a.body[:16]))
	}

	// ---- one byte, changed -------------------------------------------------

	// The body of a real message, with the last byte of its authentication tag
	// flipped — a broker plugin that rewrote something, a disk that lied, or
	// somebody with access to the queue. GCM authenticates as well as encrypts,
	// so this does not decrypt into something slightly different: it does not
	// decrypt.
	tampered := make([]byte, len(first.body))
	copy(tampered, first.body)
	tampered[len(tampered)-1] ^= 0x01

	if err := acemq.NewPublisher[[]byte](mq, "", queue,
		acemq.PublishWith[[]byte](encryptedBytes{})).
		Send(ctx, tampered, acemq.MessageType("order.placed")); err != nil {
		log.Fatal(err)
	}
	log.Println()
	log.Println("published the same message with one byte of its tag flipped")

	parked, reason := waitForParked(ctx, mq)
	log.Printf("parked %d bytes, because: %s", len(parked), reason)

	// ---- and a consumer that does not hold the new key ----------------------

	// Not published: this is what the codec does when the key is missing, shown
	// without needing a second process. A keyring holding only the old key is
	// what a consumer that has not been redeployed has.
	behind, err := crypto.NewKeyring(january)
	if err != nil {
		log.Fatal(err)
	}
	var unreadable OrderPlaced
	missingKey := crypto.Wrap(acemq.JSONCodec{}, behind).Decode(second.body, &unreadable)
	log.Println()
	log.Printf("a consumer holding only 2026-01, reading A-8: %v", missingKey)

	// ---- what all of that has to say, checked ------------------------------

	if first.order.OrderID != "A-7" || second.order.OrderID != "A-8" {
		log.Fatalf("the wrong messages arrived: %s and %s",
			first.order.OrderID, second.order.OrderID)
	}
	if first.order.Customer != "Ada Lovelace" || second.order.TotalCents != 1999 {
		log.Fatal("a payload did not survive the round trip")
	}

	// The broker never saw a customer name. Checked against the body as it
	// arrived, which is what the broker wrote to its disk.
	for _, a := range []arrival{first, second} {
		body := string(a.body)
		if strings.Contains(body, a.order.Customer) || strings.Contains(body, "orderId") {
			log.Fatalf("%s is on the wire in the clear: %q", a.order.OrderID, body)
		}
		// The family framing: magic, then version 1.
		if len(a.body) < 3 || a.body[0] != 0xAE || a.body[1] != 0x01 {
			log.Fatalf("%s is not framed the way the other libraries read: % x",
				a.order.OrderID, a.body[:3])
		}
	}

	// The key each message was written with is readable without decrypting it,
	// which is what lets a tool route a message to whoever holds the key — and
	// what proves the rotation happened.
	firstKey, _ := crypto.KeyIDOf(first.body)
	secondKey, _ := crypto.KeyIDOf(second.body)
	if firstKey != "2026-01" || secondKey != "2026-04" {
		log.Fatalf("the messages name keys %q and %q", firstKey, secondKey)
	}

	// The envelope is in the clear, and that is the warning rather than a bug:
	// the broker routes and retries on it, so it cannot be encrypted.
	if first.env.Type != "order.placed" {
		log.Fatalf("the message type did not arrive readable: %q", first.env.Type)
	}

	// The altered message did not decrypt into anything. It is parked rather
	// than retried, because the same bytes fail the same way every time, and
	// parked rather than dead-lettered because a body nothing could read is a
	// different problem from a handler that failed five times.
	if !strings.Contains(reason, "did not decrypt") {
		log.Fatalf("the parked message's reason does not say what happened: %q", reason)
	}

	// A missing key is fatal too, and says which key and what the ring holds,
	// because retrying will not put a key on it.
	if missingKey == nil {
		log.Fatal("a message written with 2026-04 was read by a keyring without it")
	}
	if !acemq.IsFatal(missingKey) {
		log.Fatal("the missing key is not fatal, so the message would be retried for ever")
	}
	if !strings.Contains(missingKey.Error(), "2026-04") {
		log.Fatalf("the error does not name the key: %v", missingKey)
	}

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		if err := mq.DeleteQueue(ctx, q); err != nil {
			log.Fatal(err)
		}
	}
}

// encryptedBytes publishes bytes that are already encrypted, under the content
// type an encrypted message carries.
//
// It exists so the tampered message above goes onto the queue looking exactly
// like the real one it was copied from — same content type, same framing, one
// byte different. acemq.BytesCodec would have written
// application/octet-stream, and a message that announced itself as something
// else would be a weaker demonstration.
type encryptedBytes struct{ acemq.BytesCodec }

func (encryptedBytes) ContentType() string { return crypto.ContentType }

// next takes the next delivery, or gives up saying so.
func next(ctx context.Context, from <-chan arrival) arrival {
	select {
	case a := <-from:
		return a
	case <-ctx.Done():
		log.Fatal("a message never arrived")
		return arrival{}
	}
}

// waitForParked pulls the message that would not decode off the parked queue.
//
// Conn.Pull rather than PullInto, because decoding is the thing that failed: the
// connection's codec would fail on these bytes here exactly as it did in the
// consumer.
func waitForParked(ctx context.Context, mq *acemq.Conn) ([]byte, string) {
	deadline := time.After(30 * time.Second)
	for {
		pulled, found, err := mq.Pull(ctx, acemq.ParkedQueue(queue))
		if err != nil {
			log.Fatal(err)
		}
		if found {
			if err := pulled.Ack(); err != nil {
				log.Fatal(err)
			}
			return pulled.Body, pulled.Envelope.Error
		}
		select {
		case <-deadline:
			log.Fatal("the altered message did not reach the parked queue")
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
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
