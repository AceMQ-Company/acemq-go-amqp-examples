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

// Package contracts is what every module in this monolith agrees on, and
// nothing else.
//
// The same role contracts plays in apps/01, and the reason is sharper here: the
// modules run in one process, so nothing but the import graph stops one from
// reaching into another. This package is the only thing they share. Every
// other package imports it and none of its siblings, which is what makes the
// monolith modular rather than merely large -- a module that only ever
// received events can be lifted into a process of its own by changing where it
// connects.
//
// Every name and every JSON field is the Java example's, character for
// character.
package contracts

import acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"

// Exchange is the one topic exchange for the whole application.
const Exchange = "policy"

// ApplicationSubmitted is a broker submitting an application. Published by
// policies, from its outbox.
type ApplicationSubmitted struct {
	ApplicationID  string `json:"applicationId"`
	Applicant      string `json:"applicant"`
	Product        string `json:"product"`
	SumAssured     int    `json:"sumAssured"`
	AgeOfApplicant int    `json:"ageOfApplicant"`
}

// ApplicationAccepted is underwriting reaching a decision and pricing it.
type ApplicationAccepted struct {
	ApplicationID string `json:"applicationId"`
	Applicant     string `json:"applicant"`
	Product       string `json:"product"`
	SumAssured    int    `json:"sumAssured"`
	AnnualPremium int    `json:"annualPremium"`
}

// ApplicationDeclined is underwriting refusing it, with a reason a human can act
// on.
type ApplicationDeclined struct {
	ApplicationID string `json:"applicationId"`
	Applicant     string `json:"applicant"`
	Reason        string `json:"reason"`
}

// PolicyIssued is a policy existing. Published by policies once underwriting
// accepted.
type PolicyIssued struct {
	PolicyID      string `json:"policyId"`
	ApplicationID string `json:"applicationId"`
	Applicant     string `json:"applicant"`
	Product       string `json:"product"`
	AnnualPremium int    `json:"annualPremium"`
}

// DocumentStored is a document belonging to a policy.
//
// The document itself is not here. This carries a claim check -- the key it
// was stored under and how big it is -- because a medical report scanned at
// 300 dpi is tens of megabytes and a broker is not a filesystem.
type DocumentStored struct {
	PolicyID    string `json:"policyId"`
	DocumentKey string `json:"documentKey"`
	Kind        string `json:"kind"`
	Bytes       int    `json:"bytes"`
}

// PremiumCharged is the first premium being taken. Published by billing.
type PremiumCharged struct {
	PolicyID  string `json:"policyId"`
	Applicant string `json:"applicant"`
	Amount    int    `json:"amount"`
}

// ClaimSettled is a claim assessed and paid.
type ClaimSettled struct {
	ClaimID  string `json:"claimId"`
	PolicyID string `json:"policyId"`
	Paid     int    `json:"paid"`
}

// ClaimRejected is a claim refused, and why.
type ClaimRejected struct {
	ClaimID  string `json:"claimId"`
	PolicyID string `json:"policyId"`
	Reason   string `json:"reason"`
}

// PolicyQuery is what claims asks policies.
type PolicyQuery struct {
	PolicyID string `json:"policyId"`
}

// PolicyStatus is what policies answers. A struct rather than a bool, so it can
// grow a reason.
type PolicyStatus struct {
	PolicyID      string `json:"policyId"`
	InForce       bool   `json:"inForce"`
	AnnualPremium int    `json:"annualPremium"`
}

// Routing keys.
const (
	ApplicationSubmittedKey = "policy.application.submitted"
	ApplicationAcceptedKey  = "policy.application.accepted"
	ApplicationDeclinedKey  = "policy.application.declined"
	PolicyIssuedKey         = "policy.policy.issued"
	DocumentStoredKey       = "policy.document.stored"
	PremiumChargedKey       = "policy.premium.charged"
	ClaimSubmittedKey       = "policy.claim.submitted"
	ClaimSettledKey         = "policy.claim.settled"
	ClaimRejectedKey        = "policy.claim.rejected"
)

// Queues: one per module, named for the module, exactly as in apps/01. That
// these happen to be served by goroutines in one process is an operational
// detail, not an architectural one.
const (
	UnderwritingQueue = "policy.underwriting"
	PoliciesQueue     = "policy.policies"
	BillingQueue      = "policy.billing"
	ClaimsQueue       = "policy.claims"

	// AuditQueue is everything, for the audit trail.
	//
	// Not decoration. Writing the Java example without it produced a real
	// failure: claims and documents published events nothing was bound to, and
	// the library refused the publish rather than dropping it. A regulated
	// insurer has this queue whatever else it has.
	AuditQueue = "policy.audit"

	// PolicyLookupQueue is where claims asks policies whether a policy is in
	// force. Request and reply, not an event.
	PolicyLookupQueue = "policy.lookup"
)

// Topology is the whole application's topology, as one value.
//
// Applied once at start-up, because there is one process. It is still declared
// here rather than assembled from each module's fragment: a module that
// declares its own queue is a module that can be started against an exchange
// nobody created.
//
// Classic queues, as in Java. A durable queue this library declares is quorum
// unless told otherwise, and a queue cannot be redeclared as another type.
func Topology() *acemq.Topology {
	classic := acemq.OfType(acemq.QueueClassic)
	return acemq.NewTopology().
		Exchange(Exchange, "topic").

		// Underwriting acts on submissions.
		Queue(UnderwritingQueue, classic).
		Binding(UnderwritingQueue, Exchange, ApplicationSubmittedKey).

		// Policies issues once underwriting has accepted.
		Queue(PoliciesQueue, classic).
		Binding(PoliciesQueue, Exchange, ApplicationAcceptedKey).

		// Billing charges once a policy exists, never before: charging for a
		// policy that was never issued is a refund and an apology.
		Queue(BillingQueue, classic).
		Binding(BillingQueue, Exchange, PolicyIssuedKey).

		// Claims needs to know which policies exist.
		Queue(ClaimsQueue, classic).
		Binding(ClaimsQueue, Exchange, PolicyIssuedKey).

		// Everything, for as long as the regulator asks. A wildcard also means a
		// new event type is audited the day it is introduced.
		Queue(AuditQueue, classic).
		Binding(AuditQueue, Exchange, "policy.#").

		// Not bound: a request is addressed to a queue, not routed to whoever
		// happens to be listening.
		Queue(PolicyLookupQueue, classic)
}

// Event is a publisher for one of the events above.
//
// Mandatory, which is the one place this port says something Java does not
// have to. Java's publishers are mandatory by default, and that default is what
// turned the missing audit binding into a failure rather than into lost
// messages. Go's are not: without this option an event nothing is bound to is
// dropped by the broker and the publish succeeds.
func Event[T any](mq *acemq.Conn, routingKey string) *acemq.Publisher[T] {
	return acemq.NewPublisher[T](mq, Exchange, routingKey, acemq.Mandatory[T]())
}
