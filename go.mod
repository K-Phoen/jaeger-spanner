module github.com/voi-oss/jaeger-spanner

go 1.16

require (
	cloud.google.com/go/spanner v1.23.0
	github.com/grpc-ecosystem/grpc-opentracing v0.0.0-20180507213350-8e809c8a8645
	github.com/hashicorp/go-hclog v0.16.2
	github.com/hashicorp/go-plugin v1.4.2
	github.com/jaegertracing/jaeger v1.24.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/spf13/viper v1.8.1
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	google.golang.org/genproto v0.0.0-20210707141755-0f065b0b1eb9
	google.golang.org/grpc v1.39.0
)
