// Examples for AceMQ for Go.
//
// They depend on the released version rather than a replace directive pointing
// at a checkout, so they resolve exactly what the documentation tells you to
// depend on — and so an example that stops compiling against a release is a red
// build here rather than a surprise for whoever copies it.
module github.com/AceMQ-Company/acemq-go-amqp-examples

go 1.23.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.7.2
	github.com/AceMQ-Company/acemq-go-amqp/codec/avro v0.7.2
	github.com/AceMQ-Company/acemq-go-amqp/codec/protobuf v0.7.2
	github.com/AceMQ-Company/acemq-go-amqp/codec/toml v0.7.2
	github.com/AceMQ-Company/acemq-go-amqp/codec/yaml v0.7.2
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/hamba/avro/v2 v2.30.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rabbitmq/amqp091-go v1.14.0 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
