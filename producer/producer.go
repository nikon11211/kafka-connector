package producer

import (
	"fmt"

	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/internal/parser"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/propagation"
)

type Producer struct {
	*kgo.Client
}

func New(cfg *kafka.ProducerConfig, logger kafka.Logger, registerer prometheus.Registerer, opts ...kafka.ClientOpts) (*Producer, error) {
	if cfg == nil {
		cfg = kafka.DefaultProducerConfig()
	}

	options := &kafka.ClientOptions{}
	for _, opt := range opts {
		opt(options)
	}

	kafkaOpts, err := parser.ParseConfig(&cfg.Config, logger, registerer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if options.TracerProvider != nil {
		tracerOpts := []kotel.TracerOpt{
			kotel.TracerProvider(options.TracerProvider),
			kotel.TracerPropagator(propagation.TraceContext{}),
		}
		tracer := kotel.NewTracer(tracerOpts...)
		kotelClient := kotel.NewKotel(kotel.WithTracer(tracer))
		kafkaOpts = append(kafkaOpts, kgo.WithHooks(kotelClient.Hooks()...))
	}

	registerMetrics(registerer)

	if len(cfg.Topics) > 0 {
		kafkaOpts = append(kafkaOpts, kgo.DefaultProduceTopic(cfg.Topics[0]))
	}

	if cfg.ProducerBatchMaxBytes > 0 {
		kafkaOpts = append(kafkaOpts, kgo.ProducerBatchMaxBytes(cfg.ProducerBatchMaxBytes))
	}

	if cfg.RecordRetries > 0 {
		kafkaOpts = append(kafkaOpts, kgo.RecordRetries(cfg.RecordRetries))
	}

	if cfg.ProducerPartitioner != "" {
		partitioner, err := parsePartitioner(cfg.ProducerPartitioner)
		if err != nil {
			return nil, err
		}
		kafkaOpts = append(kafkaOpts, kgo.RecordPartitioner(partitioner))
	}

	ack, err := parseAck(cfg.RequireAcks)
	if err != nil {
		return nil, err
	}
	kafkaOpts = append(kafkaOpts, kgo.RequiredAcks(ack))

	if ack != kgo.AllISRAcks() {
		kafkaOpts = append(kafkaOpts, kgo.DisableIdempotentWrite())
	}

	if len(cfg.Compression) > 0 {
		codecs := parseCompression(cfg.Compression)
		if len(codecs) > 0 {
			kafkaOpts = append(kafkaOpts, kgo.ProducerBatchCompression(codecs...))
		}
	}

	client, err := kgo.NewClient(kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Producer{Client: client}, nil
}

func parsePartitioner(p kafka.Partitioner) (kgo.Partitioner, error) {
	switch p {
	case kafka.UniformBytesPartitioner:
		return kgo.UniformBytesPartitioner(32<<10, true, true, nil), nil
	case kafka.LeastBackupPartitioner:
		return kgo.LeastBackupPartitioner(), nil
	case kafka.ManualPartitioner:
		return kgo.ManualPartitioner(), nil
	case kafka.RoundRobinPartitioner:
		return kgo.RoundRobinPartitioner(), nil
	case kafka.StickyKeyPartitioner:
		return kgo.StickyKeyPartitioner(nil), nil
	case kafka.StickyPartitioner:
		return kgo.StickyPartitioner(), nil
	default:
		return nil, kafka.ErrUnsupportedPartitioner
	}
}

func parseAck(a kafka.Ack) (kgo.Acks, error) {
	switch a {
	case kafka.NoAck:
		return kgo.NoAck(), nil
	case kafka.LeaderAck:
		return kgo.LeaderAck(), nil
	case kafka.AllAck, "":
		return kgo.AllISRAcks(), nil
	default:
		return kgo.AllISRAcks(), fmt.Errorf("unknown ack type: %s, using default AllISRAcks", a)
	}
}

func parseCompression(compressions []kafka.Compression) []kgo.CompressionCodec {
	var codecs []kgo.CompressionCodec
	for _, c := range compressions {
		switch c {
		case kafka.NoneCompression:
			codecs = append(codecs, kgo.NoCompression())
		case kafka.SnappyCompression:
			codecs = append(codecs, kgo.SnappyCompression())
		case kafka.ZstdCompression:
			codecs = append(codecs, kgo.ZstdCompression())
		case kafka.GzipCompression:
			codecs = append(codecs, kgo.GzipCompression())
		case kafka.Lz4Compression:
			codecs = append(codecs, kgo.Lz4Compression())
		}
	}
	return codecs
}
