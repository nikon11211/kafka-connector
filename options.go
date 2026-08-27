package kafka

import "go.opentelemetry.io/otel/trace"

type ClientOpts func(*ClientOptions)

type ClientOptions struct {
	TracerProvider trace.TracerProvider
}

func WithTracerProvider(tp trace.TracerProvider) ClientOpts {
	return func(o *ClientOptions) {
		o.TracerProvider = tp
	}
}
