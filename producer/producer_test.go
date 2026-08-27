package producer

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

func TestNewProducer(t *testing.T) {
	t.Run("with nil config", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(nil, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, p)
		p.Close()
	})

	t.Run("shared registerer does not panic on second construction", func(t *testing.T) {
		logger := &testLogger{}
		reg := prometheus.NewRegistry()

		first, err := New(kafka.DefaultProducerConfig(), logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, first)
		first.Close()

		second, err := New(kafka.DefaultProducerConfig(), logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, second)
		second.Close()
	})

	t.Run("failing registerer panics", func(t *testing.T) {
		assert.Panics(t, func() {
			registerMetrics(failingRegisterer{})
		})
	})

	t.Run("with custom config leader ack", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			ProducerPartitioner:   kafka.RoundRobinPartitioner,
			RequireAcks:           kafka.LeaderAck,
			Compression:           []kafka.Compression{kafka.GzipCompression},
			RecordRetries:         5,
			ProducerBatchMaxBytes: 2048,
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, p)
		p.Close()
	})

	t.Run("with tracing", func(t *testing.T) {
		cfg := kafka.DefaultProducerConfig()
		cfg.Brokers = []string{"localhost:9092"}
		cfg.Topics = []string{"test-topic"}
		cfg.Metrics.EnabledHTTP = false

		reg := prometheus.NewRegistry()
		logger := &testLogger{}
		tp := noop.NewTracerProvider()

		p, err := New(cfg, logger, reg, kafka.WithTracerProvider(tp))
		require.NoError(t, err)
		assert.NotNil(t, p)
		p.Close()
	})

	t.Run("with unsupported partitioner", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			ProducerPartitioner: "invalid",
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, p)
		assert.ErrorIs(t, err, kafka.ErrUnsupportedPartitioner)
	})

	t.Run("with multiple topics", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"topic1", "topic2"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, p)
		p.Close()
	})

	t.Run("with all compression types", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			Compression: []kafka.Compression{
				kafka.NoneCompression,
				kafka.SnappyCompression,
				kafka.ZstdCompression,
				kafka.GzipCompression,
				kafka.Lz4Compression,
			},
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, p)
		p.Close()
	})

	t.Run("with no ack disables idempotency", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			RequireAcks: kafka.NoAck,
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotNil(t, p)
		p.Close()
	})
}

func TestNewProducerErrors(t *testing.T) {
	t.Run("with invalid TLS config", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
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
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, p)
		assert.Contains(t, err.Error(), "failed to parse config")
	})

	t.Run("with invalid ack", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
			RequireAcks: "invalid",
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, p)
		assert.Contains(t, err.Error(), "unknown ack type")
	})

	t.Run("with no brokers", func(t *testing.T) {
		cfg := &kafka.ProducerConfig{
			Config: kafka.Config{
				Topics: []string{"test-topic"},
				Metrics: kafka.Metrics{
					Namespace:   "test",
					EnabledHTTP: false,
				},
			},
		}

		reg := prometheus.NewRegistry()
		logger := &testLogger{}

		p, err := New(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, p)
		assert.Contains(t, err.Error(), "failed to create kafka client")
	})
}

func TestParsePartitioner(t *testing.T) {
	tests := []struct {
		name        string
		partitioner kafka.Partitioner
		wantErr     bool
	}{
		{"uniform bytes", kafka.UniformBytesPartitioner, false},
		{"least backup", kafka.LeastBackupPartitioner, false},
		{"manual", kafka.ManualPartitioner, false},
		{"round robin", kafka.RoundRobinPartitioner, false},
		{"sticky key", kafka.StickyKeyPartitioner, false},
		{"sticky", kafka.StickyPartitioner, false},
		{"invalid", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := parsePartitioner(tt.partitioner)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, p)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, p)
			}
		})
	}
}

func TestParseAck(t *testing.T) {
	tests := []struct {
		name    string
		ack     kafka.Ack
		wantErr bool
	}{
		{"no ack", kafka.NoAck, false},
		{"leader ack", kafka.LeaderAck, false},
		{"all ack", kafka.AllAck, false},
		{"empty ack defaults to all", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := parseAck(tt.ack)
			assert.NoError(t, err)
			assert.NotNil(t, a)
		})
	}
}

func TestParseCompression(t *testing.T) {
	t.Run("all compressions", func(t *testing.T) {
		comps := []kafka.Compression{
			kafka.NoneCompression,
			kafka.SnappyCompression,
			kafka.ZstdCompression,
			kafka.GzipCompression,
			kafka.Lz4Compression,
		}
		result := parseCompression(comps)
		assert.Len(t, result, 5)
	})

	t.Run("invalid compression skipped", func(t *testing.T) {
		comps := []kafka.Compression{"invalid"}
		result := parseCompression(comps)
		assert.Empty(t, result)
	})

	t.Run("empty compression", func(t *testing.T) {
		result := parseCompression(nil)
		assert.Empty(t, result)
	})
}
