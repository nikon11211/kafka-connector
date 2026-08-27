package parser

import (
	"testing"
	"time"

	"github.com/nikon11211/kafka-connector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type benchLogger struct {
	level kgo.LogLevel
}

func (benchLogger) Level() kgo.LogLevel { return kgo.LogLevelNone }

func (benchLogger) Log(kgo.LogLevel, string, ...any) {}

var (
	parserBenchOpts []kgo.Opt
	parserBenchErr  error
)

func benchmarkConfig() *kafka.Config {
	return &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Topics:  []string{"orders"},
		Metrics: kafka.Metrics{Namespace: "kafka"},
		Timeout: kafka.Timeout{
			Dial:               30 * time.Second,
			ConnIdle:           300 * time.Second,
			RequestOverhead:    10 * time.Second,
			Session:            45 * time.Second,
			ProduceRequest:     30 * time.Second,
			RecordDelivery:     120 * time.Second,
			TransactionTimeout: 60 * time.Second,
		},
	}
}

func BenchmarkParseConfig(b *testing.B) {
	cfg := benchmarkConfig()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parserBenchOpts, parserBenchErr = ParseConfig(cfg, &benchLogger{}, prometheus.NewRegistry())
	}
}

func BenchmarkParseTimeouts(b *testing.B) {
	cfg := benchmarkConfig()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parserBenchOpts = parseTimeouts(&cfg.Timeout)
	}
}

func BenchmarkParseConfigNilSASL(b *testing.B) {
	cfg := benchmarkConfig()
	cfg.SASL = kafka.SASL{Username: "user", Password: "pass"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parserBenchOpts, parserBenchErr = ParseConfig(cfg, &benchLogger{}, prometheus.NewRegistry())
	}
}
