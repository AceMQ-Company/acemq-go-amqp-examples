// A module of its own, and the only example that is one.
//
// telemetry/otel moved its Go floor to 1.25 when it took OpenTelemetry 1.46, and
// every other example here still builds on 1.23 — which is the oldest toolchain
// the library supports and the thing the CI job exists to prove. Keeping this
// example in the root module would have dragged all eighteen up to 1.25 to suit
// one of them, and an example somebody cannot run is the failure this repository
// was built to prevent.
//
// So the floor stays where it is for everything else, and tracing pays for
// itself. Anyone using telemetry/otel already needs 1.25, so this asks nothing
// of them that the module they are importing does not ask first.
module github.com/AceMQ-Company/acemq-go-amqp-examples/advanced/06-tracing

go 1.25.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.7.2
	github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel v0.7.2
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/rabbitmq/amqp091-go v1.14.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
