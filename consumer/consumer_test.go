package consumer

import (
	"errors"
	"testing"

	"github.com/nikon11211/kafka-connector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"
)

type testLogger struct{}

func (t *testLogger) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

func (t *testLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {}

type failingRegisterer struct{}

func (failingRegisterer) Register(prometheus.Collector) error {
	return errors.New("registration failed")
}

func (failingRegisterer) MustRegister(...prometheus.Collector) {}

func (failingRegisterer) Unregister(prometheus.Collector) bool { return true }

func TestNewConsumer(t *testing.T) {
	t.Run("with default config", func(t *testing.T) {
		cfg := kafka.DefaultConsumerConfig()
		cfg.Brokers = []string{"localhost:9092"}
		cfg.Topics = []string{"test-topic"}
		cfg.Metrics.EnabledHTTP = false

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, c.Client)
		c.Close()
	})

	t.Run("with nil config", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(nil, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, c)
		c.Close()
	})

	t.Run("failing registerer panics", func(t *testing.T) {
		assert.Panics(t, func() {
			registerMetrics(failingRegisterer{})
		})
	})

	t.Run("shared registerer does not panic on second construction", func(t *testing.T) {
		logger := &testLogger{}
		reg := prometheus.NewRegistry()

		first, err := New(kafka.DefaultConsumerConfig(), logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, first)
		first.Close()

		second, err := New(kafka.DefaultConsumerConfig(), logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, second)
		second.Close()
	})

	t.Run("with custom config", func(t *testing.T) {
		cfg := &kafka.ConsumerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			ConsumerGroup:        "custom-group",
			DisableAutocommit:    true,
			BlockRebalanceOnPoll: true,
			FetchMinBytes:        100,
			FetchMaxMB:           100,
			HeartbeatInterval:    5000,
			Balancers:            []kafka.Balancer{kafka.CooperativeStickyBalancer, kafka.RangeBalancer},
			OffsetReset:          kafka.EndOffset,
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, c)
		c.Close()
	})

	t.Run("with tracing", func(t *testing.T) {
		cfg := kafka.DefaultConsumerConfig()
		cfg.Brokers = []string{"localhost:9092"}
		cfg.Topics = []string{"test-topic"}
		cfg.Metrics.EnabledHTTP = false

		reg := prometheus.NewRegistry()
		logger := &testLogger{}
		tp := noop.NewTracerProvider()

		c, err := New(cfg, logger, reg, kafka.WithTracerProvider(tp))
		require.NoError(t, err)
		assert.NotNil(t, c)
		c.Close()
	})

	t.Run("with multiple topics", func(t *testing.T) {
		cfg := &kafka.ConsumerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"topic1", "topic2"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			ConsumerGroup: "multi-topic-group",
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, c)
		c.Close()
	})

	t.Run("with unsupported balancer", func(t *testing.T) {
		cfg := &kafka.ConsumerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			ConsumerGroup: "bad-group",
			Balancers:     []kafka.Balancer{"invalid"},
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, c)
		assert.ErrorIs(t, err, kafka.ErrUnsupportedGroupBalancer)
	})

	t.Run("with start offset", func(t *testing.T) {
		cfg := kafka.DefaultConsumerConfig()
		cfg.Brokers = []string{"localhost:9092"}
		cfg.Topics = []string{"test-topic"}
		cfg.Metrics.EnabledHTTP = false
		cfg.OffsetReset = kafka.StartOffset

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, c)
		c.Close()
	})

	t.Run("with end offset", func(t *testing.T) {
		cfg := kafka.DefaultConsumerConfig()
		cfg.Brokers = []string{"localhost:9092"}
		cfg.Topics = []string{"test-topic"}
		cfg.Metrics.EnabledHTTP = false
		cfg.OffsetReset = kafka.EndOffset

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, c)
		c.Close()
	})
}

func TestNewConsumerErrors(t *testing.T) {
	t.Run("with invalid TLS config", func(t *testing.T) {
		cfg := &kafka.ConsumerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
				TLS: kafka.TLS{
					Enabled:  true,
					CertFile: "/nonexistent/cert.pem",
					KeyFile:  "/nonexistent/key.pem",
				},
			},
			ConsumerGroup: "tls-error-group",
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "failed to parse config")
	})

	t.Run("with no brokers", func(t *testing.T) {
		cfg := &kafka.ConsumerConfig{
			Config: kafka.Config{
				Topics: []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			ConsumerGroup: "no-brokers-group",
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		c, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "failed to create kafka client")
	})
}

func TestParseBalancers(t *testing.T) {
	t.Run("empty balancers returns default", func(t *testing.T) {
		b, err := parseBalancers(nil)
		assert.NoError(t, err)
		assert.Len(t, b, 1)
	})

	t.Run("cooperative sticky", func(t *testing.T) {
		b, err := parseBalancers([]kafka.Balancer{kafka.CooperativeStickyBalancer})
		assert.NoError(t, err)
		assert.Len(t, b, 1)
	})

	t.Run("sticky", func(t *testing.T) {
		b, err := parseBalancers([]kafka.Balancer{kafka.StickyBalancer})
		assert.NoError(t, err)
		assert.Len(t, b, 1)
	})

	t.Run("range", func(t *testing.T) {
		b, err := parseBalancers([]kafka.Balancer{kafka.RangeBalancer})
		assert.NoError(t, err)
		assert.Len(t, b, 1)
	})

	t.Run("round robin", func(t *testing.T) {
		b, err := parseBalancers([]kafka.Balancer{kafka.RoundRobinBalancer})
		assert.NoError(t, err)
		assert.Len(t, b, 1)
	})

	t.Run("multiple balancers", func(t *testing.T) {
		b, err := parseBalancers([]kafka.Balancer{
			kafka.CooperativeStickyBalancer,
			kafka.StickyBalancer,
			kafka.RangeBalancer,
			kafka.RoundRobinBalancer,
		})
		assert.NoError(t, err)
		assert.Len(t, b, 4)
	})

	t.Run("invalid balancer", func(t *testing.T) {
		b, err := parseBalancers([]kafka.Balancer{"invalid"})
		assert.Error(t, err)
		assert.Nil(t, b)
		assert.ErrorIs(t, err, kafka.ErrUnsupportedGroupBalancer)
	})
}
