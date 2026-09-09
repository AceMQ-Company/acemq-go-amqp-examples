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
// A TLS broker on a laptop, and the reason its certificates cannot reach
// production.
//
// This is the one example that needs a broker of its own, because it needs a TLS
// listener holding certificates generated here:
//
//	go run github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@v0.5.0 --out certs --broker localhost
//	chmod 644 certs/server.key
//	docker compose --profile tls up -d
//	go run ./advanced/05-development-certificates
//
// The chmod is worth understanding rather than copying. The generator writes
// private keys 0600, which is right for a key and wrong for a container running
// as another user; RabbitMQ reports an unreadable key as a listener that failed
// to start, which is a long way from what it is.
//
// # The refusal is the example
//
// Everything devcerts writes carries ACEMQ DEVELOPMENT ONLY - DO NOT TRUST in
// its subject, and the library refuses a certificate carrying that marker
// however trust is configured — including security.Insecure(), which checks
// nothing else at all. It is not a warning in a doc comment; it is the
// mechanism. A generated authority's private key sits next to its certificate
// and usually ends up in a repository, so a development certificate that could
// reach production would be an authority anybody who can read that repository
// can issue against, and the connection would succeed.
//
// Java, .NET, Python and Ruby stamp the same string and enforce it the same way.
package main

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/devcerts"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
	"github.com/AceMQ-Company/acemq-go-amqp/security"
)

const queue = "go-tls.orders"

type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ---- what the generator writes -----------------------------------------

	// A throwaway set, purely to show the files. The certificates this example
	// connects with are the ones the broker was started on: regenerating them
	// under a running broker is a handshake failure that takes a while to work
	// out, because nothing about the message says the certificate changed.
	shown, err := os.MkdirTemp("", "acemq-devcerts-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(shown)

	generated, err := devcerts.Generate(devcerts.Options{
		Directory:  shown,
		BrokerHost: "localhost",
		Validity:   24 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("acemq-certs wrote %d files, valid until %s",
		len(generated.Files), generated.Expiry.Format(time.RFC3339))
	for _, path := range generated.Files {
		info, err := os.Stat(path)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("  %-14s %v", filepath.Base(path), info.Mode().Perm())
	}
	log.Printf("marker: %q", generated.MarkerUsed)

	// ---- over TLS, against the broker --------------------------------------

	certs := certificateDir()
	authority := filepath.Join(certs, "ca.crt")

	// Naming an authority replaces the machine's trust store rather than adding
	// to it. That is the point: a broker holding a certificate from a public
	// authority is not your broker, and the several hundred authorities a
	// machine trusts by default are several hundred ways to be wrong.
	//
	// AllowDevelopmentCertificates is what gets past the marker, and is a named
	// method a reviewer will see — one more thing to grep for in a deployed
	// configuration.
	trusted := security.Required().
		TrustCertificateAuthorityFile(authority).
		AllowDevelopmentCertificates()

	mq, err := acemq.Connect(ctx, amqpsURL(), acemq.WithSecurity(trusted),
		acemq.WithOrigin("examples@05-development-certificates"))
	if err != nil {
		log.Fatalf("connecting to %s: %v\n\n%s", amqpsURL(), err, howToStart())
	}
	defer mq.Close()

	log.Println()
	log.Printf("connected to %s", amqpsURL())
	log.Printf("  %s", trusted)

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}

	arrived := make(chan OrderPlaced, 1)
	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "A-7"}); err != nil {
		log.Fatal(err)
	}

	var received OrderPlaced
	select {
	case received = <-arrived:
		log.Printf("  published and consumed %s over TLS", received.OrderID)
	case <-ctx.Done():
		log.Fatal("the message never arrived")
	}

	// ---- and with a client certificate -------------------------------------

	// A broker configured verify_peer validates one if it is presented. The
	// generated rabbitmq.conf sets fail_if_no_peer_cert = false, so it does not
	// insist — which suits a development broker that is also reached by
	// password, and means this proves the certificate was accepted rather than
	// that it was demanded.
	mutual := security.Required().
		TrustCertificateAuthorityFile(authority).
		WithClientCertificateFiles(
			filepath.Join(certs, "client.crt"), filepath.Join(certs, "client.key")).
		AllowDevelopmentCertificates()

	client, err := acemq.Connect(ctx, amqpsURL(), acemq.WithSecurity(mutual))
	if err != nil {
		log.Fatalf("connecting with a client certificate: %v", err)
	}
	log.Printf("  and again presenting a client certificate")
	if err := client.Close(); err != nil {
		log.Fatal(err)
	}

	// ---- the refusals ------------------------------------------------------

	log.Println()

	// The same configuration as the one that worked, without the flag.
	_, withoutFlag := acemq.Connect(ctx, amqpsURL(), acemq.WithSecurity(
		security.Required().TrustCertificateAuthorityFile(authority)))
	log.Printf("without the flag:       %v", firstLine(withoutFlag))

	// And the one that matters: Insecure accepts any certificate and verifies
	// no chain at all, and still refuses this one. A development certificate
	// reaching a production broker should be an error rather than a thing that
	// quietly works because somebody turned verification off to get past a
	// different problem.
	_, insecure := acemq.Connect(ctx, amqpsURL(), acemq.WithSecurity(security.Insecure()))
	log.Printf("even under Insecure:    %v", firstLine(insecure))

	// TLS settings against a plaintext URL are refused rather than ignored. A
	// service that was handed a certificate authority, connected in plaintext
	// and reported success is the failure this refusal exists to prevent.
	_, plaintext := acemq.Connect(ctx, plainURL(), acemq.WithSecurity(
		security.Required().TrustCertificateAuthorityFile(authority).
			AllowDevelopmentCertificates()))
	log.Printf("on an amqp:// URL:      %v", firstLine(plaintext))

	// ---- what all of that has to say, checked ------------------------------

	if received.OrderID != "A-7" {
		log.Fatalf("the message came back as %+v", received)
	}

	// Seven files: an authority, a broker certificate, a client certificate,
	// their keys, and a rabbitmq.conf pointing the broker at them. The same
	// names Python's generator writes, so either is a drop-in for the other.
	wanted := []string{
		"ca.crt", "ca.key", "client.crt", "client.key",
		"rabbitmq.conf", "server.crt", "server.key",
	}
	if got := baseNames(generated.Files); strings.Join(got, " ") != strings.Join(wanted, " ") {
		log.Fatalf("the generator wrote %v", got)
	}
	// Keys 0600. A development key is still a key, and one left world-readable
	// in a checkout is a habit worth not forming.
	for _, name := range []string{"ca.key", "server.key", "client.key"} {
		info, err := os.Stat(filepath.Join(shown, name))
		if err != nil {
			log.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			log.Fatalf("%s is %v", name, info.Mode().Perm())
		}
	}

	// Every certificate carries the marker, including the authority — which is
	// what makes the refusal reach a leaf issued by it.
	for name, cert := range map[string]*x509.Certificate{
		"authority": generated.Authority,
		"broker":    generated.Broker,
		"client":    generated.Client,
	} {
		if !security.IsDevelopmentCertificate(cert) {
			log.Fatalf("the %s certificate does not carry the marker: %s", name, cert.Subject)
		}
	}
	if generated.MarkerUsed != security.DevelopmentMarker {
		log.Fatalf("the generator stamped %q", generated.MarkerUsed)
	}

	// The three refusals happened, and each says which one it was.
	if withoutFlag == nil {
		log.Fatal("a development certificate was accepted without the flag")
	}
	if !strings.Contains(withoutFlag.Error(), security.DevelopmentMarker) {
		log.Fatalf("the refusal does not name the marker: %v", withoutFlag)
	}
	if insecure == nil {
		log.Fatal("a development certificate was accepted under Insecure, which is the whole point")
	}
	if !strings.Contains(insecure.Error(), security.DevelopmentMarker) {
		log.Fatalf("the Insecure refusal is about something else: %v", insecure)
	}
	if plaintext == nil {
		log.Fatal("TLS settings against an amqp:// URL were accepted")
	}
	if !strings.Contains(plaintext.Error(), "plaintext") {
		log.Fatalf("the amqp:// refusal is about something else: %v", plaintext)
	}

	// A configuration error is one this library recognised rather than something
	// the network did, and it says so in a way a caller can branch on.
	var configErr *security.ConfigurationError
	if !errors.As(withoutFlag, &configErr) {
		log.Fatalf("the refusal is not a security.ConfigurationError: %T", withoutFlag)
	}

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := mq.DeleteQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
}

// baseNames is the file names the generator wrote, sorted.
func baseNames(paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		out = append(out, filepath.Base(path))
	}
	sort.Strings(out)
	return out
}

// firstLine keeps a refusal to one line in the output. The whole message is
// worth reading, and is what a service would log.
func firstLine(err error) string {
	if err == nil {
		return "no error, which is a bug in this example"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	if len(line) > 150 {
		line = line[:150] + "…"
	}
	return line
}

func howToStart() string {
	return fmt.Sprintf(`This example needs a TLS broker holding certificates generated here:

    go run github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@v%s --out %s --broker localhost
    chmod 644 %s/server.key
    docker compose --profile tls up -d`, acemq.Version, certificateDir(), certificateDir())
}

// amqpsURL is the compose TLS broker unless ACEMQ_AMQPS_URL names another.
func amqpsURL() string {
	if url := os.Getenv("ACEMQ_AMQPS_URL"); url != "" {
		return url
	}
	return "amqps://guest:guest@localhost:5671/"
}

// plainURL is the plaintext broker, used here only to be refused.
func plainURL() string {
	if url := os.Getenv("ACEMQ_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}

// certificateDir holds ca.crt and the client certificate. It is gitignored:
// these are generated on the machine that runs them and belong nowhere else.
func certificateDir() string {
	if dir := os.Getenv("ACEMQ_CERTS"); dir != "" {
		return dir
	}
	return "certs"
}
