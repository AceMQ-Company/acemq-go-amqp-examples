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

// Package claims is the module that has to ask another module a question.
//
// Everything else here reacts to events, which is the right default. Claims
// cannot: before settling it must know whether the policy is in force, and it
// needs the answer now, in the middle of a decision. An event cannot answer a
// question, so it asks, over the broker.
//
// The callee is in the same process, and a function call would work today. It
// would also be the wrong choice: the moment claims calls into policies, the two
// are one module and the boundary that makes this a modular monolith is gone.
// Asking over the broker costs a millisecond and keeps the seam.
//
// The timeout is the part not to skip. A request that waits forever is how one
// slow module stops the whole application, monolith or not.
package claims

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	"github.com/AceMQ-Company/acemq-go-amqp-examples/apps/02-policy-administration/contracts"
)

// LookupTimeout is generous for an in-process hop, and still bounded.
const LookupTimeout = 5 * time.Second

// Module is claims.
type Module struct {
	requester  *patterns.Requester[contracts.PolicyQuery, contracts.PolicyStatus]
	issued     *acemq.Consumer
	settledPub *acemq.Publisher[contracts.ClaimSettled]
	rejectPub  *acemq.Publisher[contracts.ClaimRejected]

	settled, rejected atomic.Int64
}

// Start opens the requester and starts listening for issued policies.
func Start(ctx context.Context, mq *acemq.Conn) (m *Module, err error) {
	m = &Module{
		settledPub: contracts.Event[contracts.ClaimSettled](mq, contracts.ClaimSettledKey),
		rejectPub:  contracts.Event[contracts.ClaimRejected](mq, contracts.ClaimRejectedKey),
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, m.Close())
		}
	}()

	// The default exchange and the queue's name: a request is addressed, not
	// routed.
	if m.requester, err = patterns.NewRequester[contracts.PolicyQuery, contracts.PolicyStatus](
		ctx, mq, "", contracts.PolicyLookupQueue, patterns.Timeout(LookupTimeout)); err != nil {
		return nil, err
	}

	// Claims listens for issued policies only to know they exist at all. The
	// authoritative answer still comes from the lookup, because a policy can be
	// cancelled after issue and this module deliberately keeps no copy of
	// another module's state.
	m.issued, err = acemq.Consume(ctx, mq, contracts.ClaimsQueue,
		func(context.Context, acemq.Message[contracts.PolicyIssued]) acemq.Ack { return acemq.Accept() })
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Submit assesses a claim against a policy and returns the claim id.
func (m *Module) Submit(ctx context.Context, policyID string, amount int, description string) (string, error) {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	claimID := "CLM-" + hex.EncodeToString(b)
	opts := []acemq.EnvelopeOption{acemq.MessageType("Claim"), acemq.CorrelationID(policyID)}

	status, err := m.requester.Do(ctx, contracts.PolicyQuery{PolicyID: policyID})
	if err != nil {
		// Not an answer, and must not be treated as "no". Refusing a valid claim
		// because a lookup was slow is the failure worth being explicit about.
		return "", fmt.Errorf("could not establish whether %s is in force, so claim %s was neither"+
			" settled nor rejected; it must be retried: %w", policyID, claimID, err)
	}

	if !status.InForce {
		if err := m.rejectPub.Send(ctx,
			contracts.ClaimRejected{ClaimID: claimID, PolicyID: policyID, Reason: "no policy in force"},
			opts...); err != nil {
			return "", err
		}
		m.rejected.Add(1)
		return claimID, nil
	}

	// A real assessment is a great deal more than this. What matters is that it
	// happened after an authoritative answer rather than after a guess.
	if err := m.settledPub.Send(ctx,
		contracts.ClaimSettled{ClaimID: claimID, PolicyID: policyID, Paid: amount}, opts...); err != nil {
		return "", err
	}
	m.settled.Add(1)
	return claimID, nil
}

// Settled is how many claims were settled.
func (m *Module) Settled() int64 { return m.settled.Load() }

// Rejected is how many were rejected because no policy was in force.
func (m *Module) Rejected() int64 { return m.rejected.Load() }

// Close stops listening, then the requester.
func (m *Module) Close() error {
	var errs []error
	if m.issued != nil {
		errs = append(errs, m.issued.Close())
	}
	if m.requester != nil {
		errs = append(errs, m.requester.Close())
	}
	return errors.Join(errs...)
}
