// A load that keeps running and says what is happening to it, one line at a time.
//
// Every other example here finishes. This one does not: it publishes and consumes
// at a steady rate and prints one JSON object per second describing what it has
// seen. That makes it the thing a fault drill breaks the cluster underneath -- the
// drill kills a node or raises a memory alarm, reads these lines, and judges what
// the client did about it.
//
// Why a drill needs this rather than a probe of its own
// -----------------------------------------------------
// A probe that connects to the broker can answer "is the cluster usable". It
// cannot answer what an application saw: whether it was told the broker had
// stopped reading from it, whether it stopped publishing, whether it started
// again on its own or sat there waiting for a restart. Those are properties of a
// client library, they differ between libraries that are otherwise equivalent, and
// the only thing that can report them is a client.
//
// What a line has to contain
// --------------------------
// One JSON object per line, oldest first, on stdout. The fields below are the ones
// a watcher understands, and all of them are optional:
//
//	blocked      is the broker refusing to read from this connection now
//	published    sends attempted since the start
//	confirmed    sends the broker has acknowledged
//	consumed     deliveries handled
//	failed       sends that failed for a reason other than back-pressure
//	publishRate  confirms per second over the last interval
//	consumeRate  deliveries per second over the last interval
//
// Anything else on the line is ignored by a watcher and is there for a human --
// `refused' below is the interesting one for this library, and the section on it
// says why it is not counted as a failure.
//
// Running it
//
//	go run ./advanced/08-a-standing-load-something-else-can-watch \
//	    -broker amqp://guest:guest@localhost:5672 > readings.jsonl
//
// Then read the last few lines at any point to see what the client is seeing.
// Under a fault drill that file is the client's testimony; `tail -n 60` on it is
// how the drill asks.
//
// It exits on SIGINT or SIGTERM, or after -for if that is given. A drill campaign
// runs it for the length of the campaign, so the default is to run until stopped.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

	// The AMQP transport. Registered by importing it, so a program that talks to
	// a broker says so in its imports and one that only uses the in-memory
	// transport does not link a network stack it never calls.
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

// Order is the payload. Deliberately dull: this example is about what the client
// reports, and a realistic message would only make the lines longer.
type Order struct {
	ID string `json:"id"`
}

// reading is one line of the timeline.
//
// The field names are a contract with whatever is reading them, so they are
// spelled here exactly as a watcher expects and are not derived from anything.
// Renaming one to something tidier would leave a watcher reading a client that
// reports nothing, which is not distinguishable from a well-behaved client on a
// quiet cluster.
type reading struct {
	At        string `json:"at"`
	ElapsedMs int64  `json:"elapsedMs"`

	Blocked bool `json:"blocked"`

	Published int64 `json:"published"`
	Confirmed int64 `json:"confirmed"`
	Consumed  int64 `json:"consumed"`
	Failed    int64 `json:"failed"`

	PublishRate float64 `json:"publishRate"`
	ConsumeRate float64 `json:"consumeRate"`

	// Refused counts sends this library declined because the broker had blocked
	// the connection. Not part of what a watcher judges, and reported anyway:
	// it is the number that separates a library which refuses back-pressure
	// promptly from one that parks on it, and a human comparing two libraries
	// wants to see it.
	Refused int64 `json:"refused"`

	// Reason is what the broker said when it blocked the connection, while it is
	// blocked. Empty otherwise.
	Reason string `json:"reason,omitempty"`
}

// counters are shared between the publisher, the consumer and the sampler.
//
// Atomics rather than a mutex because every one of them is a single counter
// incremented from one place and read from another, and a reading that is a
// fraction of a second stale is not a reading anybody would act on differently.
type counters struct {
	published atomic.Int64
	confirmed atomic.Int64
	consumed  atomic.Int64
	failed    atomic.Int64
	refused   atomic.Int64
}

func main() {
	broker := flag.String("broker", envOr("ACEMQ_URL", "amqp://guest:guest@localhost:5672"),
		"the broker to run against")
	queue := flag.String("queue", "go-standing-load.orders", "the queue to publish to and consume from")
	rate := flag.Int("rate", 500, "messages per second to offer")
	interval := flag.Duration("interval", time.Second, "how often to print a reading")
	// Bounded in CI, where every example is run with no flags and nothing is
	// standing by to interrupt one. The same variable the recovery example uses,
	// for the same reason: unset it and this runs until stopped, which is what a
	// drill campaign needs and what it would do on anybody's machine.
	runFor := flag.Duration("for", exampleSeconds(),
		"stop after this long; zero runs until interrupted")
	flag.Parse()

	if err := run(*broker, *queue, *rate, *interval, *runFor); err != nil {
		log.Fatal(err)
	}
}

func run(broker, queue string, rate int, interval, runFor time.Duration) error {
	// Interrupted rather than killed, so that the connection closes and the last
	// reading is printed. A drill reads the tail of this output, and a load that
	// dies without a final line leaves its last second unaccounted for.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, runFor)
		defer cancel()
	}

	// Logs go to stderr so that stdout carries nothing but readings. A watcher
	// skips whatever is not a JSON object, so mixing them would work -- and it
	// would also mean every diagnostic line here had to stay un-JSON-like for
	// ever, which is not a property anybody would remember to preserve.
	log.SetOutput(os.Stderr)
	log.SetFlags(0)
	log.Printf("standing load: %s, %s at %d/s", broker, queue, rate)

	mq, err := acemq.Connect(ctx, broker, acemq.WithOrigin("examples@08-a-standing-load"))
	if err != nil {
		return err
	}
	defer mq.Close()

	// Quorum, because a drill's faults are about losing a node. A classic queue
	// lives on one node, and when that node is the one the drill stops, the load
	// stops with it -- which reports a client that gave up when the truth is that
	// the queue went away.
	if err := mq.DeclareQueue(ctx, queue, acemq.OfType(acemq.QueueQuorum)); err != nil {
		return err
	}

	var c counters

	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, _ acemq.Message[Order]) acemq.Ack {
			c.consumed.Add(1)
			return acemq.Accept()
		},
		acemq.Concurrency(4))
	if err != nil {
		return err
	}
	defer consumer.Close()

	go publish(ctx, mq, queue, rate, &c)

	sample(ctx, mq, interval, &c)
	return nil
}

// publish offers messages at a steady rate and records what became of each one.
func publish(ctx context.Context, mq *acemq.Conn, queue string, rate int, c *counters) {
	publisher := acemq.NewPublisher[Order](mq, "", queue)

	// One tick per message rather than a batch per second, so that a reading
	// taken mid-second reflects a rate rather than whatever part of a burst has
	// happened by then.
	if rate < 1 {
		rate = 1
	}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	for n := int64(0); ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		c.published.Add(1)
		// Bounded, because the point of a reading is that it arrives. A send
		// with no deadline on a broker that has stopped reading takes the
		// sampler's next line with it, and a client that went quiet looks the
		// same as a client that was never running.
		send, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := publisher.Send(send, Order{ID: fmt.Sprintf("o-%d", n)})
		cancel()

		switch {
		case err == nil:
			c.confirmed.Add(1)
		case isPaused(err):
			// Back-pressure, correctly reported, and not a failure: the broker
			// said it had stopped reading and this library declined rather than
			// waiting. Counting it as a failure would make a library that
			// handles back-pressure well look worse than one that hides it by
			// blocking a caller for the length of an alarm.
			c.refused.Add(1)
		case ctx.Err() != nil:
			return
		default:
			c.failed.Add(1)
		}
	}
}

// sample prints one reading per interval until the context ends.
func sample(ctx context.Context, mq *acemq.Conn, interval time.Duration, c *counters) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	started := time.Now()
	var lastConfirmed, lastConsumed int64
	last := started

	emit := func() {
		now := time.Now()
		elapsed := now.Sub(last).Seconds()
		confirmed := c.confirmed.Load()
		consumed := c.consumed.Load()

		r := reading{
			At:        now.UTC().Format(time.RFC3339Nano),
			ElapsedMs: now.Sub(started).Milliseconds(),
			Reason:    mq.BlockedReason(),
			Published: c.published.Load(),
			Confirmed: confirmed,
			Consumed:  consumed,
			Failed:    c.failed.Load(),
			Refused:   c.refused.Load(),
		}
		// Blocked is derived from the reason rather than tracked separately, so
		// the two can never disagree on one line -- and a line saying blocked
		// with no reason, or a reason with blocked false, is exactly the kind of
		// thing a reader would rightly refuse to believe.
		r.Blocked = r.Reason != ""
		if elapsed > 0 {
			r.PublishRate = float64(confirmed-lastConfirmed) / elapsed
			r.ConsumeRate = float64(consumed-lastConsumed) / elapsed
		}
		lastConfirmed, lastConsumed, last = confirmed, consumed, now

		line, err := json.Marshal(r)
		if err != nil {
			log.Printf("could not marshal a reading: %v", err)
			return
		}
		fmt.Println(string(line))
	}

	for {
		select {
		case <-ctx.Done():
			// One last line on the way out, so the final interval is accounted
			// for rather than being the one a reader has to guess about.
			emit()
			return
		case <-ticker.C:
			emit()
		}
	}
}

// isPaused reports whether a send was declined because the broker had blocked
// this connection.
func isPaused(err error) bool {
	var paused *acemq.PublishingPausedError
	return errors.As(err, &paused)
}

// exampleSeconds is how long to run when nobody said, which is zero -- until
// interrupted -- unless ACEMQ_EXAMPLE_SECONDS says otherwise.
//
// It exists for CI, which runs every example in this repository with no arguments
// and waits for each to finish. A standing load is the one example that has no
// reason to stop on its own, and without this it would not be a failing example:
// it would be a job that never ends.
func exampleSeconds() time.Duration {
	n, err := strconv.Atoi(os.Getenv("ACEMQ_EXAMPLE_SECONDS"))
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
