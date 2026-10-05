// A module of its own, like advanced/06-tracing and for the same kind of reason.
//
// The gateway's outbox and payments' idempotency store only mean anything when
// they share a transaction with a real database, and the pure-Go SQLite driver
// needs Go 1.25. Every other example still builds on 1.23; this one pays for its
// database itself rather than raising the floor for the rest.
module github.com/AceMQ-Company/acemq-go-amqp-examples/apps/01-order-fulfilment

go 1.25.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.9.4
	modernc.org/sqlite v1.58.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.14.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.47.0 // indirect
	modernc.org/libc v1.75.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)
