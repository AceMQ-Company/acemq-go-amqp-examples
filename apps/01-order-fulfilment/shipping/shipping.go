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

// Package shipping dispatches what has been paid for and reserved.
//
// The simplest service in the system, and it is worth noticing why: it reacts
// to one event, does one thing, and publishes one event. It knows nothing about
// payments, nothing about stock levels, and nothing about who else cares that an
// order shipped.
//
// That is the property the whole architecture is buying. Adding a service that
// also reacts to stock.reserved requires no change here at all.
package shipping

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/contracts"
)

// Service is shipping.
type Service struct {
	mq       *acemq.Conn
	consumer *acemq.Consumer
	shipped  *acemq.Publisher[contracts.OrderShipped]
	count    atomic.Int64
}

// Start connects, applies the topology and starts consuming.
func Start(ctx context.Context, url string) (*Service, error) {
	mq, err := acemq.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	s := &Service{
		mq:      mq,
		shipped: acemq.NewPublisher[contracts.OrderShipped](mq, contracts.Exchange, contracts.OrderShippedKey),
	}
	if err := contracts.Topology().Apply(ctx, mq); err != nil {
		mq.Close()
		return nil, err
	}
	s.consumer, err = acemq.Consume(ctx, mq, contracts.ShippingQueue, s.dispatch, acemq.Prefetch(10))
	if err != nil {
		mq.Close()
		return nil, err
	}
	return s, nil
}

func (s *Service) dispatch(ctx context.Context, m acemq.Message[contracts.StockReserved]) acemq.Ack {
	reservation := m.Payload
	tracking := "TRK-" + strings.ToUpper(strings.TrimPrefix(reservation.OrderID, "ord-"))
	if err := s.shipped.Send(ctx,
		contracts.OrderShipped{OrderID: reservation.OrderID, Customer: reservation.Customer, Tracking: tracking},
		acemq.MessageType("OrderShipped"),
		acemq.CorrelationID(m.Envelope.CorrelationID)); err != nil {
		return acemq.Retry(err)
	}
	s.count.Add(1)
	return acemq.Accept()
}

// Shipped is how many orders went out.
func (s *Service) Shipped() int64 { return s.count.Load() }

// Close drains the consumer, then closes the connection.
func (s *Service) Close() error {
	return errors.Join(s.consumer.Close(), s.mq.Close())
}
