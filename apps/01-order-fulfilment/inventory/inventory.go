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

// Package inventory holds stock for orders that have been paid for.
//
// The service that talks to something unreliable. A warehouse system that times
// out is the ordinary case, not the exception, and the two failures have to be
// told apart:
//
//   - the warehouse did not answer — retry, it will probably work in a moment;
//   - there are three left and the order wants ten — retrying changes nothing.
//
// The first goes up the retry ladder. The second is an outcome, not a failure:
// it is published as StockUnavailable straight away, because four more attempts
// would only delay telling the customer.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/contracts"
)

// Service is inventory.
type Service struct {
	mq          *acemq.Conn
	metrics     *acemq.Metrics
	consumer    *acemq.Consumer
	reservedPub *acemq.Publisher[contracts.StockReserved]
	missingPub  *acemq.Publisher[contracts.StockUnavailable]

	mu    sync.Mutex
	stock map[string]int

	reserved, rejected, warehouseCalls atomic.Int64

	// failuresToSimulate is how many warehouse calls fail before it starts
	// working. Set by the system run.
	failuresToSimulate atomic.Int64
}

// Start connects, applies the topology and starts consuming.
func Start(ctx context.Context, url string) (*Service, error) {
	// The library's own counters, kept in memory, so the retries can be read
	// back as the library counted them rather than as this service believes.
	metrics := acemq.NewMetrics()
	mq, err := acemq.Connect(ctx, url, acemq.WithObserver(metrics))
	if err != nil {
		return nil, err
	}
	s := &Service{
		mq:          mq,
		metrics:     metrics,
		stock:       map[string]int{},
		reservedPub: acemq.NewPublisher[contracts.StockReserved](mq, contracts.Exchange, contracts.StockReservedKey),
		missingPub:  acemq.NewPublisher[contracts.StockUnavailable](mq, contracts.Exchange, contracts.StockUnavailableKey),
	}
	if err := contracts.Topology().Apply(ctx, mq); err != nil {
		mq.Close()
		return nil, err
	}

	// The ladder waits inside the broker rather than on this goroutine: a
	// failed message sits in a delay queue and comes back, instead of occupying
	// a consumer that could be handling the orders behind it.
	s.consumer, err = acemq.Consume(ctx, mq, contracts.InventoryQueue, s.reserve,
		acemq.Prefetch(20),
		acemq.RetryWith(acemq.ExponentialRetry(4, 200*time.Millisecond, 5*time.Second)))
	if err != nil {
		mq.Close()
		return nil, err
	}
	return s, nil
}

// WithStock puts stock on the shelf.
func (s *Service) WithStock(sku string, quantity int) *Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stock[sku] = quantity
	return s
}

// WithFlakyWarehouse makes the next count warehouse calls fail, the way a real
// one does.
func (s *Service) WithFlakyWarehouse(count int) *Service {
	s.failuresToSimulate.Store(int64(count))
	return s
}

func (s *Service) reserve(ctx context.Context, m acemq.Message[contracts.PaymentCaptured]) acemq.Ack {
	payment := m.Payload

	// The transient failure. Nothing is wrong with the message, so it goes back
	// on the ladder and arrives again shortly.
	if s.warehouseCalls.Add(1) <= s.failuresToSimulate.Load() {
		return acemq.Retry(errors.New("warehouse did not respond"))
	}

	correlation := acemq.CorrelationID(m.Envelope.CorrelationID)

	s.mu.Lock()
	available, known := s.stock[payment.SKU]
	if !known || available < payment.Quantity {
		s.mu.Unlock()
		// The permanent one. Retrying will not conjure stock.
		if err := s.missingPub.Send(ctx,
			contracts.StockUnavailable{OrderID: payment.OrderID, Customer: payment.Customer,
				SKU: payment.SKU, Reason: fmt.Sprintf("only %d left", available)},
			acemq.MessageType("StockUnavailable"), correlation); err != nil {
			return acemq.Retry(err)
		}
		s.rejected.Add(1)
		return acemq.Accept()
	}
	s.stock[payment.SKU] = available - payment.Quantity
	s.mu.Unlock()

	if err := s.reservedPub.Send(ctx,
		contracts.StockReserved{OrderID: payment.OrderID, Customer: payment.Customer,
			SKU: payment.SKU, Quantity: payment.Quantity},
		acemq.MessageType("StockReserved"), correlation); err != nil {
		// Put it back: the retry will take it again.
		s.mu.Lock()
		s.stock[payment.SKU] += payment.Quantity
		s.mu.Unlock()
		return acemq.Retry(err)
	}
	s.reserved.Add(1)
	return acemq.Accept()
}

// Reserved is how many orders had stock held.
func (s *Service) Reserved() int64 { return s.reserved.Load() }

// Rejected is how many orders found too little stock.
func (s *Service) Rejected() int64 { return s.rejected.Load() }

// StockOf is what is left of a SKU.
func (s *Service) StockOf(sku string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stock[sku]
}

// Retried is how many messages the library actually scheduled for another
// attempt, read from its own counter.
func (s *Service) Retried() int64 {
	var n int64
	for key, count := range s.metrics.Counts() {
		if strings.HasPrefix(key, acemq.MetricRetriedTotal) {
			n += count
		}
	}
	return n
}

// Close drains the consumer, then closes the connection.
func (s *Service) Close() error {
	return errors.Join(s.consumer.Close(), s.mq.Close())
}
