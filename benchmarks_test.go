package kafka

import (
	"testing"
)

var (
	kcBenchCfg any
	kcBenchErr error
)

func BenchmarkConfigValidate(b *testing.B) {
	cfg := DefaultProducerConfig().Config
	cfg.Topics = []string{"orders"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kcBenchErr = cfg.Validate()
	}
}

func BenchmarkProducerConfigValidate(b *testing.B) {
	cfg := DefaultProducerConfig()
	cfg.Topics = []string{"orders"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kcBenchErr = cfg.Config.Validate()
	}
}

func BenchmarkConsumerConfigValidate(b *testing.B) {
	cfg := DefaultConsumerConfig()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kcBenchErr = cfg.Config.Validate()
	}
}

func BenchmarkDefaultProducerConfig(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kcBenchCfg = DefaultProducerConfig()
	}
}

func BenchmarkDefaultConsumerConfig(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kcBenchCfg = DefaultConsumerConfig()
	}
}
