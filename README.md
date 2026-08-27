<p align="center">
  <h1 align="center">Kafka-Connector — Production-Grade Kafka Client for Go</h1>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/nikon11211/kafka-connector">
    <img src="https://pkg.go.dev/badge/github.com/nikon11211/kafka-connector.svg" alt="Go Reference"/>
  </a>
  <a href="https://goreportcard.com/report/github.com/nikon11211/kafka-connector">
    <img src="https://goreportcard.com/badge/github.com/nikon11211/kafka-connector" alt="Go Report Card"/>
  </a>
  <a href="https://github.com/nikon11211/kafka-connector/actions/workflows/test.yaml">
    <img src="https://github.com/nikon11211/kafka-connector/actions/workflows/test.yaml/badge.svg" alt="Tests"/>
  </a>
  <a href="https://codecov.io/gh/nikon11211/kafka-connector">
    <img src="https://codecov.io/gh/nikon11211/kafka-connector/branch/main/graph/badge.svg" alt="Coverage"/>
  </a>
  <a href="https://sonarcloud.io/summary/overall?id=nikon11211_kafka-connector">
    <img src="https://sonarcloud.io/api/project_badges/measure?project=nikon11211_kafka-connector&metric=coverage" alt="SonarCloud Coverage"/>
  </a>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"/>
  </a>
  <a href="https://golang.org/">
    <img src="https://img.shields.io/badge/Go-%3E%3D%201.26-blue" alt="Go Version"/>
  </a>
</p>

<p align="center">
  <b>A high-performance Kafka producer & consumer library for Go microservices</b><br/>
  <i>franz-go • OpenTelemetry tracing • Prometheus metrics • SASL/TLS • consumer groups</i>
</p>

---

## Overview

`kafka-connector` wraps [franz-go](https://github.com/twmb/franz-go), the
most feature-complete and efficient Kafka client for Go, and layers on the
operational features microservices need out of the box:

- **producer & consumer packages** — ready-made `producer.Producer` and
  `consumer.Consumer` built from declarative config structs;
- **Prometheus metrics** — client, record, request and lag metrics registered
  automatically;
- **OpenTelemetry tracing** — client-side spans via the franz-go `kotel`
  plugin (only when a tracer provider is configured);
- **SASL / TLS** — PLAIN and SCRAM authentication, TLS with cert/CA files;
- **consumer groups** — group, partition balancers, offset reset and
  auto-commit controls;
- **idempotent writes** — required acks of `All` enable idempotence
  automatically;
- **strict validation** — `Config.Validate()` catches misconfiguration early.

## Install

```bash
go get github.com/nikon11211/kafka-connector
```

## Quick Start — Producer

```go
package main

import (
	"context"
	"time"

	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/producer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := kafka.DefaultProducerConfig()
	cfg.Brokers = []string{"localhost:9092"}

	prod, err := producer.New(cfg, kafka.NoopLogger{}, prometheus.DefaultRegisterer)
	if err != nil {
		panic(err)
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	record := &kgo.Record{Topic: "events", Key: []byte("key"), Value: []byte("hello")}
	if err := prod.ProduceSync(ctx, record).FirstErr(); err != nil {
		panic(err)
	}
}
```

## Quick Start — Consumer

```go
import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/consumer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := kafka.DefaultConsumerConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"events"}
	cfg.ConsumerGroup = "events-service"

	cons, err := consumer.New(cfg, kafka.NoopLogger{}, prometheus.DefaultRegisterer)
	if err != nil {
		panic(err)
	}
	defer cons.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := cons.PollFetches(ctx)
			fetches.EachRecord(func(record *kgo.Record) {
				log.Printf("Received: %s", record.Value)
			})
		}
	}
}
```

Complete runnable examples live in [`examples`](examples/).

## Configuration

The library ships three configurable structs, all `mapstructure`-ready:

- `kafka.Config` — shared client settings: brokers, timeouts (`Timeout`),
  SASL, TLS, metrics (`Metrics`).
- `kafka.ProducerConfig` — default topic, batch size, record retries,
  partitioner, required acks, compression.
- `kafka.ConsumerConfig` — consumer group, topics, balancers, offset reset,
  fetch sizing, heartbeat, auto-commit.

Defaults come from `kafka.DefaultProducerConfig()` /
`kafka.DefaultConsumerConfig()`; passing `nil` to `New` also falls back to
them.

### Enums

| Type          | Values                                                                 |
|---------------|------------------------------------------------------------------------|
| `Partitioner` | `uniform-bytes`, `least-backup`, `manual`, `round-robin`, `sticky-key`, `sticky` |
| `Ack`         | `No`, `Leader`, `All`                                                  |
| `Compression` | `none`, `snappy`, `zstd`, `gzip`, `lz4`                                |
| `Balancer`    | `cooperative-sticky`, `sticky`, `range`, `round-robin`                 |
| `OffsetReset` | `start`, `end`                                                         |

## Prometheus Metrics

- **Producer**: `messages_sent`, `errors_occurred`, `record_queue_time_avg`,
  `request_rate`, `request_latency_avg`, `batch_size_avg`, `record_size_avg`,
  `buffer_available_bytes`, `requests_in_flight`, `request_size`,
  `compression_rate`, `buffer_memory`, `max_in_flight_requests`.
- **Consumer**: `messages_consumed`, `errors_occurred`, `batch_size_avg`,
  `fetch_rate`, `fetch_latency_avg`, `offset_lag`, `messages_processed`.

Metrics are registered on the `prometheus.Registerer` you pass to `New`.

## Tracing

```go
import (
	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/producer"
	"go.opentelemetry.io/otel"
)

prod, err := producer.New(
	cfg,
	kafka.NoopLogger{},
	prometheus.DefaultRegisterer,
	kafka.WithTracerProvider(otel.GetTracerProvider()),
)
```

When a tracer provider is supplied, producer/consumer operations are traced
with the franz-go `kotel` plugin and the `TraceContext` propagator — spans
cross process boundaries automatically.

## Errors

All user-facing errors are sentinels, tested and documented:

- `ErrUnsupportedPartitioner`
- `ErrUnsupportedGroupBalancer`
- `ErrInvalidTopicConfig`
- `ErrTLSConfiguration`
- `ErrSASLConfiguration`

## Options

| Option                    | Description                                        |
|---------------------------|----------------------------------------------------|
| `WithTracerProvider(tp)`  | Enable OpenTelemetry tracing via the kotel plugin  |

## Testing & Benchmarks

Unit tests reach 100% statement coverage (excluding `examples/`):

```bash
go test -race -coverprofile=coverage.txt -covermode=atomic $(go list ./... | grep -v /examples)
```

Run benchmarks across the library and the internal config parser:

```bash
go test -bench=. -benchmem -run '^$' . ./internal/parser
```

| Benchmark                          | What it measures                        |
|------------------------------------|-----------------------------------------|
| `BenchmarkConfigValidate`          | Shared client config validation         |
| `BenchmarkProducerConfigValidate`  | Producer config validation              |
| `BenchmarkConsumerConfigValidate`  | Consumer config validation              |
| `BenchmarkDefaultProducerConfig`   | Producer defaults reset                 |
| `BenchmarkDefaultConsumerConfig`   | Consumer defaults reset                 |
| `BenchmarkParseConfig`             | franz-go options parsing (parser pkg)   |
| `BenchmarkParseTimeouts`           | Timeouts parsing (parser pkg)           |
| `BenchmarkParseConfigNilSASL`      | Option parsing with no SASL (parser pkg)|

CI enforces the 100% gate, runs `go vet`, benchmarks and
[golangci-lint](https://golangci-lint.run), and publishes coverage to Codecov
and SonarCloud.

## License

[MIT](LICENSE)
