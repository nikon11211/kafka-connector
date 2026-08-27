package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestWithTracerProvider(t *testing.T) {
	opts := &ClientOptions{}
	tp := noop.NewTracerProvider()

	opt := WithTracerProvider(tp)
	opt(opts)

	assert.NotNil(t, opts.TracerProvider)
	assert.Equal(t, tp, opts.TracerProvider)
}

func TestWithTracerProviderNil(t *testing.T) {
	opts := &ClientOptions{}

	opt := WithTracerProvider(nil)
	opt(opts)

	assert.Nil(t, opts.TracerProvider)
}

func TestClientOptsMultiple(t *testing.T) {
	opts := &ClientOptions{}
	tp1 := noop.NewTracerProvider()
	tp2 := noop.NewTracerProvider()

	WithTracerProvider(tp1)(opts)
	assert.Equal(t, tp1, opts.TracerProvider)

	WithTracerProvider(tp2)(opts)
	assert.Equal(t, tp2, opts.TracerProvider)
}
