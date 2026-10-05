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

// Package contracts is what every service in this system agrees on, and
// nothing else.
//
// The events, the exchange, the queue each service reads, and the routing keys
// that connect them. In a larger estate this is what a schema registry holds.
//
// What is deliberately not here: any service's domain model, any database
// access, any shared helper. A contracts package that grows those stops being a
// contract and becomes a shared library, which is how five services turn back
// into one deployable that happens to have five entry points.
//
// Every name and every JSON field is the Java example's, character for
// character. The wire is the contract, not the language either side of it.
package contracts

import acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

// Exchange is the one topic exchange every event in the system is published to.
const Exchange = "fulfilment"

// The events. Each carries the order id, because that is the only identifier
// every service shares -- and correlation across five services is otherwise
// guesswork.

// OrderPlaced is someone placing an order. Published by the gateway, from its
// outbox.
type OrderPlaced struct {
	OrderID  string  `json:"orderId"`
	Customer string  `json:"customer"`
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Total    float64 `json:"total"`
}

// PaymentCaptured is the money being ours. Published by payments.
type PaymentCaptured struct {
	OrderID  string  `json:"orderId"`
	Customer string  `json:"customer"`
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Amount   float64 `json:"amount"`
}

// PaymentDeclined is the money not being ours, and not going to be. Published by
// payments; nothing downstream proceeds.
type PaymentDeclined struct {
	OrderID  string `json:"orderId"`
	Customer string `json:"customer"`
	Reason   string `json:"reason"`
}

// StockReserved is stock held for this order. Published by inventory.
type StockReserved struct {
	OrderID  string `json:"orderId"`
	Customer string `json:"customer"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// StockUnavailable is there not being enough. Published by inventory; the money
// must be given back.
type StockUnavailable struct {
	OrderID  string `json:"orderId"`
	Customer string `json:"customer"`
	SKU      string `json:"sku"`
	Reason   string `json:"reason"`
}

// OrderShipped is the order on its way. Published by shipping.
type OrderShipped struct {
	OrderID  string `json:"orderId"`
	Customer string `json:"customer"`
	Tracking string `json:"tracking"`
}

// Routing keys: "fulfilment.<aggregate>.<past-tense-verb>". The aggregate in the
// middle is what lets a service subscribe to everything about orders without
// naming each event, and lets notifications subscribe to everything at all.
const (
	OrderPlacedKey      = "fulfilment.order.placed"
	PaymentCapturedKey  = "fulfilment.payment.captured"
	PaymentDeclinedKey  = "fulfilment.payment.declined"
	StockReservedKey    = "fulfilment.stock.reserved"
	StockUnavailableKey = "fulfilment.stock.unavailable"
	OrderShippedKey     = "fulfilment.order.shipped"
)

// Queues: one per service, named after the service rather than after the event.
// Two services wanting the same event each get their own copy, and neither can
// starve the other.
const (
	PaymentsQueue      = "fulfilment.payments"
	InventoryQueue     = "fulfilment.inventory"
	ShippingQueue      = "fulfilment.shipping"
	NotificationsQueue = "fulfilment.notifications"
)

// Topology is the whole system's topology, as one value.
//
// Every service applies it on start-up. Applying the same topology from five
// places is safe and is the point: no service depends on another having started
// first, and there is no deployment order to get wrong.
//
// Classic queues, as in Java. Go's default for a durable queue is quorum, and a
// queue cannot be redeclared as another type, so leaving the type to the default
// would make a Go service and a Java service unable to share a broker.
func Topology() *acemq.Topology {
	classic := acemq.OfType(acemq.QueueClassic)
	return acemq.NewTopology().
		Exchange(Exchange, "topic").

		// Payments acts on new orders.
		Queue(PaymentsQueue, classic).
		Binding(PaymentsQueue, Exchange, OrderPlacedKey).

		// Inventory acts once the money is taken, not before. Reserving stock
		// for an order that cannot be paid for is how a warehouse fills with
		// holds nobody releases.
		Queue(InventoryQueue, classic).
		Binding(InventoryQueue, Exchange, PaymentCapturedKey).

		// Shipping needs stock held.
		Queue(ShippingQueue, classic).
		Binding(ShippingQueue, Exchange, StockReservedKey).

		// Notifications wants everything, which is what a wildcard is for.
		Queue(NotificationsQueue, classic).
		Binding(NotificationsQueue, Exchange, "fulfilment.#")
}
