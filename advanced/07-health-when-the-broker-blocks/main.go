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

// What Health says on a connection the broker has really blocked.
//
//	docker compose --profile alarm up -d
//	go run ./advanced/07-health-when-the-broker-blocks
//
// RabbitMQ raises an alarm when memory or disk crosses a watermark, sends
// connection.blocked to the connections publishing on it, and stops reading
// them. Health answers up, with the broker's own reason, and answers without
// asking the broker anything — the round trip is skipped rather than shortened,
// because a broker that has stopped reading will not answer a probe either.
//
// Both halves are checked here against a real alarm, and the second is what
// gives the first any meaning: a health check that is fast on a broker that was
// never blocked has demonstrated nothing, so this also shows an ordinary round
// trip on the same connection failing to come back at all.
//
// # Its own broker, and the watermark put back
//
// An alarm is a property of the node, not of this connection. Every publisher
// on that broker stops being served for as long as it lasts, which is why this
// example has a broker of its own — pointed at the shared one it would block
// the other examples, and they would be the ones that failed.
//
// The watermark goes back before anything closes, whether this run passes or
// fails. Left down, the broker is unusable for whatever touches it next; and
// closing a blocked connection waits on a broker that is not reading.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

const queue = "go-blocked-health.orders"

// healthBudget is what Health has to answer within on a blocked connection.
//
// The measurement below is in microseconds and the release notes say "under a
// millisecond", so this is two orders of magnitude of slack. Loose on purpose:
// a threshold tight enough to be impressive is one that goes red on a busy
// runner for a reason that has nothing to do with the library, and what proves
// no round trip was made is the round trip further down that never comes back,
// not this number.
const healthBudget = 100 * time.Millisecond

// blockedPrefix is the fixed sentence Health puts in front of the broker's own
// words. Java, .NET, Python and Ruby all say it, and an alert rule matches on
// it, so it is worth an example noticing if it ever stops being said.
const blockedPrefix = "the broker has blocked this connection; publishing is paused"

// defaultWatermark is what the rabbitmq image ships with, and what this example
// puts back. Hard-coded rather than read from the broker first: the alarm has
// to be cleared even when the run is failing, and a restore that depends on the
// broker answering a question is a restore that does not happen when it is most
// needed.
const defaultWatermark = "0.4"

type Order struct {
	ID string `json:"id"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run returns its failures rather than calling log.Fatal, so that the deferred
// restore below runs however this ends. An example that leaves a broker in
// alarm because an assertion failed has broken the next thing to touch that
// broker, and the failure that then gets reported is not the one anybody reads.
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		return err
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		return err
	}

	// A healthy broker first, so the numbers below have something to be read
	// against.
	before := timed(ctx, mq)
	show("before", before)
	if before.report.Status != acemq.HealthUp {
		return fmt.Errorf("a reachable broker should be up, and this one reported %s", before.report.Status)
	}
	if before.report.Parts["blocked"] != false {
		return fmt.Errorf("nothing has blocked this connection yet, and health says blocked=%v", before.report.Parts["blocked"])
	}
	if _, timedIt := before.report.Parts["roundTripMillis"]; !timedIt {
		return errors.New("an unblocked check asks the broker something and should have timed it")
	}

	// Everything from here runs against a broker in alarm.
	log.Println()
	log.Println("dropping the memory high watermark to nothing, which puts the node in")
	log.Println("the state a production broker reaches under memory pressure")
	if err := rabbitmqctl("set_vm_memory_high_watermark", "0"); err != nil {
		return err
	}
	defer restore(mq)

	// The alarm on its own tells this connection nothing. RabbitMQ sends
	// connection.blocked to a connection that publishes under an alarm, so a
	// publisher finds out by publishing and an idle connection does not find
	// out at all. That is the whole reason a health check has to be able to
	// answer this question without publishing.
	if err := publishUntilBlocked(ctx, mq); err != nil {
		return err
	}

	during := timed(ctx, mq)
	show("during", during)

	// Up, not down and not degraded. Down would have an orchestrator restart
	// this process into the same blocked broker, having thrown away whatever it
	// was holding, and a fleet doing that together stops draining the queues at
	// the moment the broker most needs them drained. Degraded is for one
	// instance being worse at its job than its neighbours; a block is the
	// broker's state and identical across every replica, so an alert that fires
	// for all of them at once is one no deployment can act on.
	if during.report.Status != acemq.HealthUp {
		return fmt.Errorf("a blocked connection is up, and this one reported %s", during.report.Status)
	}
	if during.report.Parts["blocked"] != true {
		return fmt.Errorf("the broker has blocked this connection and health says blocked=%v", during.report.Parts["blocked"])
	}
	// The broker's own words, which is what belongs in the alert: "low on
	// memory" is actionable and "the publish failed" is not.
	reason, _ := during.report.Parts["blockedReason"].(string)
	if !strings.Contains(reason, "memory") {
		return fmt.Errorf("the reason should be the broker's own, and it reads %q", reason)
	}
	if !strings.HasPrefix(during.report.Detail, blockedPrefix) {
		return fmt.Errorf("an alert rule matches the sentence in front of the reason, and the detail reads %q", during.report.Detail)
	}
	// Nothing was timed because nothing was asked. The round trip being skipped
	// stated as a fact about the report, rather than as a duration that happened
	// to come out small.
	if _, timedIt := during.report.Parts["roundTripMillis"]; timedIt {
		return errors.New("a blocked check asks the broker nothing, so it has nothing to time")
	}
	if during.took > healthBudget {
		return fmt.Errorf("health took %s on a blocked connection, which is long enough to have been a round trip", during.took)
	}

	// The aggregate is what a readiness endpoint serves, and it keeps what the
	// parts said rather than summarising it away. Up with an empty line would be
	// the worst answer available here: the one fact the check went to the
	// trouble of finding, dropped from the line an operator reads first.
	aggregate := acemq.AggregateHealth(ctx, acemq.ConnHealth{Conn: mq, Label: "broker"})
	log.Printf("  aggregate  %s", aggregate)
	if !strings.Contains(aggregate.Detail, reason) {
		return fmt.Errorf("the aggregate dropped the reason and said %q", aggregate.Detail)
	}

	// Without this, everything above would pass just as happily against a broker
	// that was never blocked, and would prove nothing. A queue declare is the
	// cheapest question this library asks a broker, and it is the shape a health
	// check that probed first would have used: on a blocked connection it is not
	// refused, it goes unanswered.
	answered := declareOnItsOwnGoroutine(mq)
	select {
	case err := <-answered:
		return fmt.Errorf("a round trip answered on a blocked connection (%v), so nothing above was measured against a block", err)
	case <-time.After(5 * time.Second):
		log.Println()
		log.Println("  control    a queue declare on this same connection has still not come")
		log.Println("             back after 5s. That is what a health check that asked the")
		log.Println("             broker first would be sitting in, which is why this one")
		log.Println("             does not ask.")
	}

	// Putting the watermark back releases everything the alarm parked.
	log.Println()
	log.Printf("putting the memory high watermark back to %s", defaultWatermark)
	if err := rabbitmqctl("set_vm_memory_high_watermark", defaultWatermark); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return mq.BlockedReason() == "" }); err != nil {
		return fmt.Errorf("the alarm did not clear: %w", err)
	}
	select {
	case err := <-answered:
		log.Printf("  released   the same declare completed once the broker started reading: %v", err)
	case <-time.After(60 * time.Second):
		return errors.New("the declare never completed, so the alarm was not what had been holding it")
	}

	after := timed(ctx, mq)
	show("after", after)
	if after.report.Status != acemq.HealthUp || after.report.Parts["blocked"] != false {
		return fmt.Errorf("the alarm cleared and health still reads %s blocked=%v",
			after.report.Status, after.report.Parts["blocked"])
	}
	if _, timedIt := after.report.Parts["roundTripMillis"]; !timedIt {
		return errors.New("the connection is not blocked any more, so health should be asking the broker again")
	}

	log.Println()
	log.Println("a blocked broker is not an unhealthy application. The readiness probe")
	log.Println("stays green, the reason is on the line an operator reads first, and the")
	log.Println("consumers keep draining -- which is the only thing that clears the alarm.")
	return nil
}

// publishUntilBlocked publishes until the broker says it has blocked this
// connection, and then shows what publishing does once it has.
//
// Each send gets a short deadline of its own. The send that goes out as the
// broker stops reading is waiting on a confirm that will never arrive, and it
// is bounded here rather than left to the run's own deadline: without that it
// sits there for three minutes and the example looks hung rather than blocked.
func publishUntilBlocked(ctx context.Context, mq *acemq.Conn) error {
	publisher := acemq.NewPublisher[Order](mq, "", queue)

	for attempt := 0; attempt < 30; attempt++ {
		if reason := mq.BlockedReason(); reason != "" {
			log.Printf("  blocked    the broker sent connection.blocked: %q", reason)

			// Refused, immediately, rather than parked. A publish that hangs
			// for the length of an alarm is indistinguishable from a wedged
			// process, and a service told it is paused can shed load, buffer,
			// or fail the request instead.
			started := time.Now()
			err := publisher.Send(ctx, Order{ID: "refused"})
			var paused *acemq.PublishingPausedError
			if !errors.As(err, &paused) {
				return fmt.Errorf("a publish on a blocked connection should be refused, and it returned %v", err)
			}
			if !strings.Contains(paused.Reason, reason) {
				return fmt.Errorf("the refusal should carry the broker's reason, and it carried %q", paused.Reason)
			}
			log.Printf("  publish    refused in %s: %v", time.Since(started).Round(time.Microsecond), paused)
			return nil
		}

		send, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := publisher.Send(send, Order{ID: fmt.Sprintf("o-%d", attempt)})
		cancel()
		if err != nil {
			// Expected, once: the send that was in flight when the broker
			// stopped reading is waiting for a confirm that is not coming. The
			// loop goes round, connection.blocked has arrived by then, and the
			// next send is refused rather than left waiting.
			log.Printf("  publish    %d went unconfirmed, which is the broker having stopped reading mid-publish", attempt)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("the broker never blocked this connection, so there was nothing to measure")
}

// restore clears the alarm before the connection is closed.
//
// Deferred after the connection's own close so that it runs first: closing a
// blocked connection waits on a broker that has stopped reading.
func restore(mq *acemq.Conn) {
	if err := rabbitmqctl("set_vm_memory_high_watermark", defaultWatermark); err != nil {
		log.Printf("could not put the memory high watermark back: %v", err)
		log.Printf("the broker is still in alarm and every publisher on it is refused; run:")
		log.Printf("  docker exec %s rabbitmqctl set_vm_memory_high_watermark %s", alarmContainer(), defaultWatermark)
		return
	}
	deadline, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = waitFor(deadline, func() bool { return mq.BlockedReason() == "" })
}

type timing struct {
	report acemq.HealthReport
	took   time.Duration
}

func timed(ctx context.Context, mq *acemq.Conn) timing {
	started := time.Now()
	report := mq.Health(ctx)
	return timing{report: report, took: time.Since(started)}
}

func show(label string, t timing) {
	log.Println()
	log.Printf("  %-9s status %s in %s", label, t.report.Status, t.took.Round(time.Microsecond))
	log.Printf("             parts %v", t.report.Parts)
	if t.report.Detail != "" {
		log.Printf("             %s", t.report.Detail)
	}
}

// declareOnItsOwnGoroutine asks the broker the cheapest question there is, and
// reports when it is answered. On its own goroutine because on a blocked
// connection it is not answered at all, and the point is to still be here.
func declareOnItsOwnGoroutine(mq *acemq.Conn) <-chan error {
	answered := make(chan error, 1)
	go func() {
		// No deadline, deliberately. A deadline here would end the wait and
		// hide the thing being shown, which is that the wait does not end.
		answered <- mq.DeclareQueue(context.Background(), queue)
	}()
	return answered
}

func waitFor(ctx context.Context, condition func() bool) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if condition() {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("still not true after 30s")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// rabbitmqctl runs a command inside the broker's container.
//
// Through docker rather than over AMQP because there is no AMQP for this: an
// alarm is a property of the node, and the node is told about it by its own
// command line tool.
//
// HOME is named explicitly because rabbitmqctl looks for the Erlang cookie
// under it and refuses to talk to the node without the right one. The compose
// file points the container's HOME at /tmp, for the unrelated reason that the
// image's default is not writable on every Docker setup — and the server still
// keeps its cookie in its own data directory, so an exec that inherited /tmp
// reads a different cookie and fails with a wall of diagnostics that never
// mentions HOME.
func rabbitmqctl(args ...string) error {
	command := append([]string{
		"exec", "-e", "HOME=/var/lib/rabbitmq", alarmContainer(), "rabbitmqctl"}, args...)
	output, err := exec.Command("docker", command...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(command, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

// alarmContainer is the one broker container this example is allowed to put
// into alarm, which compose.yaml names. ACEMQ_ALARM_CONTAINER names another.
func alarmContainer() string {
	if name := os.Getenv("ACEMQ_ALARM_CONTAINER"); name != "" {
		return name
	}
	return "acemq-alarm-broker"
}

// brokerURL is this example's own broker rather than the one every other
// example shares. ACEMQ_ALARM_URL names another.
//
// Deliberately not ACEMQ_URL: pointed at the shared broker, this example would
// block every other publisher on it, and the failures would land on them.
func brokerURL() string {
	if url := os.Getenv("ACEMQ_ALARM_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5673/"
}
