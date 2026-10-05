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

// Package notifications tells the customer what happened.
//
// Bound to fulfilment.# — everything. This is the service that shows why a
// topic exchange is worth more than a queue per pair of services: it was added
// without a single change to any publisher, and the next one will be too.
//
// It cannot ask for a typed payload, because it subscribes to six event types
// on one queue and their shapes differ. So it takes the body as text and reads
// the envelope, which carries the type and the correlation id -- everything
// this service actually needs.
package notifications

import (
	"context"
	"errors"
	"slices"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment/contracts"
)

// Service is notifications.
type Service struct {
	mq       *acemq.Conn
	consumer *acemq.Consumer

	mu       sync.Mutex
	timeline map[string][]string
}

// Start connects, applies the topology and starts consuming.
func Start(ctx context.Context, url string) (*Service, error) {
	mq, err := acemq.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	s := &Service{mq: mq, timeline: map[string][]string{}}
	if err := contracts.Topology().Apply(ctx, mq); err != nil {
		mq.Close()
		return nil, err
	}

	// The text codec is the part worth copying: a fan-in consumer reads bodies
	// it has no single type for, and the JSON codec would be asked to turn an
	// object into a string.
	s.consumer, err = acemq.Consume(ctx, mq, contracts.NotificationsQueue,
		func(_ context.Context, m acemq.Message[string]) acemq.Ack {
			// The correlation id is the order it belongs to, set by whichever
			// service published it and carried forward by all of them.
			s.mu.Lock()
			defer s.mu.Unlock()
			order := m.Envelope.CorrelationID
			s.timeline[order] = append(s.timeline[order], m.Envelope.Type)
			return acemq.Accept()
		},
		acemq.Prefetch(50), acemq.ConsumeWith(acemq.StringCodec{}))
	if err != nil {
		mq.Close()
		return nil, err
	}
	return s, nil
}

// TimelineOf is what a customer looking at "where is my order" would be shown.
func (s *Service) TimelineOf(orderID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.timeline[orderID])
}

// OrdersSeen is how many orders have a timeline.
func (s *Service) OrdersSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.timeline)
}

// Close drains the consumer, then closes the connection.
func (s *Service) Close() error {
	return errors.Join(s.consumer.Close(), s.mq.Close())
}
