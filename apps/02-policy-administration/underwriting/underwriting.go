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

// Package underwriting decides whether to accept an application, and at what
// price.
//
// The one part of this application that is genuinely a sequence: check the
// applicant against the register, price the risk, then decide. Each stage fails
// for its own reasons and is slow for its own reasons, which is what a declared
// pipeline is for -- a queue per stage, so a slow stage shows up as a deep queue
// you can point at, and can be retried and scaled without touching the others.
// Written as one consumer doing three things in order, all of that disappears
// into one number that says "underwriting is slow".
//
// Each step is described as well as named. The name is the routing key and the
// queue suffix, so it stays short and stable; the description is free text, and
// is what the start-up log line shows somebody who does not know this system.
package underwriting

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/contracts"
)

// ReferralThreshold is the sum assured above which a human decides, not a rule.
const ReferralThreshold = 500_000

// Checked is what the register stage produces.
type Checked struct {
	Application     contracts.ApplicationSubmitted `json:"application"`
	KnownToRegister bool                           `json:"knownToRegister"`
}

// Priced is what the pricing stage produces.
type Priced struct {
	Application   contracts.ApplicationSubmitted `json:"application"`
	AnnualPremium int                            `json:"annualPremium"`
	Refer         bool                           `json:"refer"`
}

// Module is underwriting.
type Module struct {
	pipeline    *patterns.Pipeline[contracts.ApplicationSubmitted]
	submissions *acemq.Consumer
	acceptedPub *acemq.Publisher[contracts.ApplicationAccepted]
	declinedPub *acemq.Publisher[contracts.ApplicationDeclined]

	accepted, declined atomic.Int64
}

// Start declares the pipeline and feeds it from the module's own queue.
func Start(ctx context.Context, mq *acemq.Conn) (m *Module, err error) {
	m = &Module{
		acceptedPub: contracts.Event[contracts.ApplicationAccepted](mq, contracts.ApplicationAcceptedKey),
		declinedPub: contracts.Event[contracts.ApplicationDeclined](mq, contracts.ApplicationDeclinedKey),
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, m.Close())
		}
	}()

	m.pipeline, err = patterns.NewPipeline[contracts.ApplicationSubmitted](ctx, mq, "underwriting",
		patterns.PipelineStep("register", checkRegister,
			patterns.StepDescribedAs("look the applicant up on the shared industry register"),
			// The register is somebody else's service and is periodically
			// unavailable, which is a wait rather than a decline.
			patterns.StepRetry(acemq.ExponentialRetry(4, 200*time.Millisecond, 5*time.Second))),
		patterns.PipelineStep("price", price,
			patterns.StepDescribedAs("apply the rating table for the product and the applicant's age")),
		patterns.PipelineStep("decide", m.decide,
			patterns.StepDescribedAs("accept, or refer anything a rule should not be deciding")))
	if err != nil {
		return nil, err
	}
	// Returned rather than logged by the library, so it is logged here.
	log.Print(m.pipeline.Describe())

	// The pipeline is fed from this module's own queue rather than being bound
	// to the exchange itself: the pipeline owns its stages' queues, and what
	// enters it is this module's decision.
	m.submissions, err = acemq.Consume(ctx, mq, contracts.UnderwritingQueue,
		func(ctx context.Context, msg acemq.Message[contracts.ApplicationSubmitted]) acemq.Ack {
			if _, err := m.pipeline.Send(ctx, msg.Payload,
				acemq.CorrelationID(msg.Envelope.CorrelationID)); err != nil {
				return acemq.Retry(err)
			}
			return acemq.Accept()
		})
	if err != nil {
		return nil, err
	}
	return m, nil
}

func checkRegister(_ context.Context, msg acemq.Message[contracts.ApplicationSubmitted]) (Checked, bool, error) {
	// A real one calls an industry service. What matters here is that it is the
	// stage most likely to be slow, and it has its own queue to prove it.
	application := msg.Payload
	return Checked{
		Application:     application,
		KnownToRegister: strings.Contains(strings.ToLower(application.Applicant), "known"),
	}, true, nil
}

func price(_ context.Context, msg acemq.Message[Checked]) (Priced, bool, error) {
	checked := msg.Payload
	application := checked.Application

	// A rating table, compressed to one line. Older applicants and larger sums
	// cost more.
	base := application.SumAssured / 1000
	ageLoading := max(0, application.AgeOfApplicant-30) * 2
	registerLoading := 0
	if checked.KnownToRegister {
		registerLoading = base / 2
	}
	return Priced{
		Application:   application,
		AnnualPremium: base + ageLoading + registerLoading,
		Refer:         application.SumAssured > ReferralThreshold,
	}, true, nil
}

func (m *Module) decide(ctx context.Context, msg acemq.Message[Priced]) (struct{}, bool, error) {
	priced := msg.Payload
	application := priced.Application
	opts := []acemq.EnvelopeOption{
		acemq.MessageType("UnderwritingDecision"), acemq.CorrelationID(application.ApplicationID),
	}

	if priced.Refer {
		// Declined rather than parked: "a human must look at this" is a real
		// outcome of underwriting, not a failure of it. Modelled as an error it
		// would sit in a dead-letter queue looking like something broke.
		err := m.declinedPub.Send(ctx, contracts.ApplicationDeclined{
			ApplicationID: application.ApplicationID,
			Applicant:     application.Applicant,
			Reason:        fmt.Sprintf("sum assured of %d is above the automatic limit", application.SumAssured),
		}, opts...)
		if err != nil {
			return struct{}{}, false, err
		}
		// Counted after the publish, so a publish that fails and is retried is
		// not counted twice.
		m.declined.Add(1)
		return struct{}{}, true, nil
	}

	err := m.acceptedPub.Send(ctx, contracts.ApplicationAccepted{
		ApplicationID: application.ApplicationID,
		Applicant:     application.Applicant,
		Product:       application.Product,
		SumAssured:    application.SumAssured,
		AnnualPremium: priced.AnnualPremium,
	}, opts...)
	if err != nil {
		return struct{}{}, false, err
	}
	m.accepted.Add(1)
	return struct{}{}, true, nil
}

// Accepted is how many applications underwriting accepted.
func (m *Module) Accepted() int64 { return m.accepted.Load() }

// Declined is how many it referred or refused.
func (m *Module) Declined() int64 { return m.declined.Load() }

// Close stops taking submissions, then the pipeline.
func (m *Module) Close() error {
	var errs []error
	if m.submissions != nil {
		errs = append(errs, m.submissions.Close())
	}
	if m.pipeline != nil {
		errs = append(errs, m.pipeline.Close())
	}
	return errors.Join(errs...)
}
