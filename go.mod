// Examples for AceMQ for Go.
//
// They depend on the released version rather than a replace directive pointing
// at a checkout, so they resolve exactly what the documentation tells you to
// depend on — and so an example that stops compiling against a release is a red
// build here rather than a surprise for whoever copies it.
module github.com/AceMQ-Company/acemq-go-amqp-examples

go 1.23

require github.com/AceMQ-Company/acemq-go-amqp v0.1.0

require github.com/rabbitmq/amqp091-go v1.14.0 // indirect
