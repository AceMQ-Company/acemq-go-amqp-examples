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

// The whole system: five services, one broker, five orders.
//
//	docker compose up -d
//	cd apps/01-order-fulfilment && go run .
//
// In production these are five deployments. Here they run in one process
// against one real RabbitMQ, which exercises every queue, every hop and every
// failure path -- and fails, with a non-zero exit, if any service stops
// agreeing with the contracts. Each order gets a freshly started system, so
// nothing one scenario counted leaks into the next.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
	_ "modernc.org/sqlite"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/contracts"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/gateway"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/inventory"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/notifications"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/payments"
	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/shipping"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Println("all five orders ended where they should")
}

func run() error {
	dir, err := os.MkdirTemp("", "fulfilment-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	if err := cleanSlate(); err != nil {
		return err
	}

	scenarios := []struct {
		name string
		run  func(context.Context, *system) error
	}{
		{"an order travels through every service", anOrderTravelsThroughEveryService},
		{"a flaky warehouse is retried rather than failed", aFlakyWarehouseIsRetriedRatherThanFailed},
		{"an order over the limit stops at payments", anOrderOverTheLimitStopsAtPayments},
		{"there is not enough stock and retrying would not help", thereIsNotEnoughStock},
		{"an order delivered twice is charged once", anOrderDeliveredTwiceIsChargedOnce},
	}
	for i, scenario := range scenarios {
		log.Printf("--- %s", scenario.name)
		if err := runScenario(filepath.Join(dir, fmt.Sprint(i)), scenario.run); err != nil {
			return fmt.Errorf("%s: %w", scenario.name, err)
		}
	}
	return nil
}

func runScenario(dir string, scenario func(context.Context, *system) error) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	sys, err := start(ctx, dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, sys.stop()) }()
	return scenario(ctx, sys)
}

func anOrderTravelsThroughEveryService(ctx context.Context, s *system) error {
	orderID, err := s.gateway.PlaceOrder(ctx, "ada", "WIDGET", 2, 42.00)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.shipping.Shipped() == 1 }); err != nil {
		return err
	}

	// One order in at the gateway, and every service downstream acted exactly
	// once.
	if err := expect("captured", s.payments.Captured(), 1); err != nil {
		return err
	}
	if err := expect("reserved", s.inventory.Reserved(), 1); err != nil {
		return err
	}
	if err := expect("shipped", s.shipping.Shipped(), 1); err != nil {
		return err
	}

	// Stock actually moved. Without this the reservation is a log line.
	if err := expect("WIDGET left", s.inventory.StockOf("WIDGET"), 8); err != nil {
		return err
	}

	// The customer's view is the whole story, assembled from events published
	// by four services that never spoke to each other. The wait is for the final
	// count rather than an intermediate one: polling for three can miss the
	// moment the third arrives and the fourth follows.
	if err := expectTimeline(ctx, s, orderID,
		"OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped"); err != nil {
		return err
	}

	// The outbox is empty, so nothing is waiting to be published.
	pending, err := s.gateway.PendingInOutbox(ctx)
	if err != nil {
		return err
	}
	return expect("pending in the outbox", pending, 0)
}

func aFlakyWarehouseIsRetriedRatherThanFailed(ctx context.Context, s *system) error {
	s.inventory.WithFlakyWarehouse(2)

	orderID, err := s.gateway.PlaceOrder(ctx, "grace", "WIDGET", 1, 10.00)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.shipping.Shipped() == 1 }); err != nil {
		return err
	}

	// Two failures, then success. The order was never lost and no human was
	// involved.
	if retried := s.inventory.Retried(); retried < 2 {
		return fmt.Errorf("retried %d times; expected at least 2", retried)
	}
	if err := expect("reserved", s.inventory.Reserved(), 1); err != nil {
		return err
	}
	return waitFor(ctx, func() bool {
		return slices.Contains(s.notifications.TimelineOf(orderID), "OrderShipped")
	})
}

func anOrderOverTheLimitStopsAtPayments(ctx context.Context, s *system) error {
	orderID, err := s.gateway.PlaceOrder(ctx, "charles", "WIDGET", 1, 5_000.00)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.payments.Declined() == 1 }); err != nil {
		return err
	}

	// Nothing downstream ran, which is the point of declining before reserving.
	if err := expect("reserved", s.inventory.Reserved(), 0); err != nil {
		return err
	}
	if err := expect("shipped", s.shipping.Shipped(), 0); err != nil {
		return err
	}
	if err := expect("WIDGET left", s.inventory.StockOf("WIDGET"), 10); err != nil {
		return err
	}
	return expectTimeline(ctx, s, orderID, "OrderPlaced", "PaymentDeclined")
}

func thereIsNotEnoughStock(ctx context.Context, s *system) error {
	orderID, err := s.gateway.PlaceOrder(ctx, "alan", "WIDGET", 99, 99.00)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.inventory.Rejected() == 1 }); err != nil {
		return err
	}

	// The money was taken and the stock was not there. In a real system this is
	// where a refund is triggered. It is deliberately visible rather than
	// swallowed.
	if err := expect("captured", s.payments.Captured(), 1); err != nil {
		return err
	}
	if err := expect("shipped", s.shipping.Shipped(), 0); err != nil {
		return err
	}
	return expectTimeline(ctx, s, orderID, "OrderPlaced", "PaymentCaptured", "StockUnavailable")
}

// The one claim the Java system test leaves unchecked: that payments is
// idempotent. The relay is at-least-once by design -- it removes a record only
// after the broker confirmed it, so a crash in between publishes it again -- and
// this is that second copy, same message id and all, arriving after the first
// has been charged.
func anOrderDeliveredTwiceIsChargedOnce(ctx context.Context, s *system) error {
	order := contracts.OrderPlaced{Customer: "edsger", SKU: "WIDGET", Quantity: 1, Total: 10.00}
	var err error
	if order.OrderID, err = s.gateway.PlaceOrder(ctx, order.Customer, order.SKU, order.Quantity, order.Total); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.shipping.Shipped() == 1 }); err != nil {
		return err
	}

	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		return err
	}
	defer mq.Close()
	if err := acemq.NewPublisher[contracts.OrderPlaced](mq, contracts.Exchange, contracts.OrderPlacedKey).
		Send(ctx, order, acemq.MessageType("OrderPlaced"),
			acemq.MessageID(order.OrderID), acemq.CorrelationID(order.OrderID)); err != nil {
		return err
	}
	if err := waitFor(ctx, func() bool { return s.payments.DuplicatesRefused() == 1 }); err != nil {
		return err
	}

	// Refused at the claim, so nothing was charged and nothing downstream moved.
	if err := expect("captured", s.payments.Captured(), 1); err != nil {
		return err
	}
	if err := expect("reserved", s.inventory.Reserved(), 1); err != nil {
		return err
	}
	if err := expect("shipped", s.shipping.Shipped(), 1); err != nil {
		return err
	}
	// Notifications is not idempotent and does not need to be: it saw the
	// order placed twice, which is the truth about the wire.
	return expectTimeline(ctx, s, order.OrderID,
		"OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped", "OrderPlaced")
}

// system is the five services, each with a database of its own where it needs
// one. The moment two services read the same table, the deployment boundary is
// a fiction.
type system struct {
	gateway       *gateway.Service
	payments      *payments.Service
	inventory     *inventory.Service
	shipping      *shipping.Service
	notifications *notifications.Service
	closers       []func() error
}

func start(ctx context.Context, dir string) (s *system, err error) {
	s = &system{}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.stop())
		}
	}()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	url := brokerURL()

	gatewayDB, err := database(s, filepath.Join(dir, "gateway.db"))
	if err != nil {
		return nil, err
	}
	if s.gateway, err = gateway.Start(ctx, url, gatewayDB); err != nil {
		return nil, err
	}
	s.closers = append(s.closers, s.gateway.Close)

	paymentsDB, err := database(s, filepath.Join(dir, "payments.db"))
	if err != nil {
		return nil, err
	}
	if s.payments, err = payments.Start(ctx, url, paymentsDB); err != nil {
		return nil, err
	}
	s.closers = append(s.closers, s.payments.Close)

	if s.inventory, err = inventory.Start(ctx, url); err != nil {
		return nil, err
	}
	s.inventory.WithStock("WIDGET", 10)
	s.closers = append(s.closers, s.inventory.Close)

	if s.shipping, err = shipping.Start(ctx, url); err != nil {
		return nil, err
	}
	s.closers = append(s.closers, s.shipping.Close)

	if s.notifications, err = notifications.Start(ctx, url); err != nil {
		return nil, err
	}
	s.closers = append(s.closers, s.notifications.Close)
	return s, nil
}

// stop closes everything in reverse: the services before their databases.
func (s *system) stop() error {
	var errs []error
	for i := len(s.closers) - 1; i >= 0; i-- {
		errs = append(errs, s.closers[i]())
	}
	s.closers = nil
	return errors.Join(errs...)
}

// database is a fresh SQLite file for one service.
//
// One connection, because SQLite takes one writer at a time and a pool would
// turn that into "database is locked" rather than into waiting.
func database(s *system, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s.closers = append(s.closers, db.Close)
	return db, nil
}

// cleanSlate deletes the four service queues, so messages a previous run left
// behind cannot be counted by this one. Topology recreates them.
func cleanSlate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mq, err := acemq.Connect(ctx, brokerURL())
	if err != nil {
		return err
	}
	defer mq.Close()
	for _, queue := range []string{contracts.PaymentsQueue, contracts.InventoryQueue,
		contracts.ShippingQueue, contracts.NotificationsQueue} {
		if err := mq.DeleteQueue(ctx, queue); err != nil {
			return err
		}
	}
	return nil
}

func expectTimeline(ctx context.Context, s *system, orderID string, want ...string) error {
	if err := waitFor(ctx, func() bool { return len(s.notifications.TimelineOf(orderID)) >= len(want) }); err != nil {
		return fmt.Errorf("timeline of %s is %v: %w", orderID, s.notifications.TimelineOf(orderID), err)
	}
	// Built from the correlation id alone.
	if got := s.notifications.TimelineOf(orderID); !slices.Equal(got, want) {
		return fmt.Errorf("timeline of %s is %v; expected %v", orderID, got, want)
	}
	log.Printf("%s: %v", orderID, want)
	return nil
}

func expect[N int | int64](what string, got N, want N) error {
	if got != want {
		return fmt.Errorf("%s is %d; expected %d", what, got, want)
	}
	return nil
}

func waitFor(ctx context.Context, done func() bool) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !done() {
		select {
		case <-ctx.Done():
			return errors.New("the system did not reach the expected state in time")
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}

// brokerURL is ACEMQ_FULFILMENT_URL, then ACEMQ_URL, then the compose broker.
//
// A variable of its own because this app wants a virtual host of its own: see
// the README for the exchange it would otherwise fight over.
func brokerURL() string {
	for _, name := range []string{"ACEMQ_FULFILMENT_URL", "ACEMQ_URL"} {
		if url := os.Getenv(name); url != "" {
			return url
		}
	}
	return "amqp://guest:guest@localhost:5672/"
}
