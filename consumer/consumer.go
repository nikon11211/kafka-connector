package consumer

import (
	"fmt"

	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/internal/parser"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/propagation"
)

type Consumer struct {
	*kgo.Client
}

func New(cfg *kafka.ConsumerConfig, logger kafka.Logger, registerer prometheus.Registerer, opts ...kafka.ClientOpts) (*Consumer, error) {
	if cfg == nil {
		cfg = kafka.DefaultConsumerConfig()
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

	kafkaOpts = append(kafkaOpts, kgo.ConsumerGroup(cfg.ConsumerGroup))

	offset := kgo.NewOffset().AtStart()
	if cfg.OffsetReset == kafka.EndOffset {
		offset = kgo.NewOffset().AtEnd()
	}
	kafkaOpts = append(kafkaOpts, kgo.ConsumeResetOffset(offset))

	if len(cfg.Topics) > 0 {
		kafkaOpts = append(kafkaOpts, kgo.ConsumeTopics(cfg.Topics...))
	}

	balancers, err := parseBalancers(cfg.Balancers)
	if err != nil {
		return nil, err
	}
	kafkaOpts = append(kafkaOpts, kgo.Balancers(balancers...))

	if cfg.FetchMinBytes > 0 {
		kafkaOpts = append(kafkaOpts, kgo.FetchMinBytes(cfg.FetchMinBytes))
	}

	if cfg.FetchMaxMB > 0 {
		kafkaOpts = append(kafkaOpts, kgo.FetchMaxBytes(cfg.FetchMaxMB<<20))
	}

	if cfg.HeartbeatInterval > 0 {
		kafkaOpts = append(kafkaOpts, kgo.HeartbeatInterval(cfg.HeartbeatInterval))
	}

	if cfg.DisableAutocommit {
		kafkaOpts = append(kafkaOpts, kgo.DisableAutoCommit())
	}

	if cfg.BlockRebalanceOnPoll {
		kafkaOpts = append(kafkaOpts, kgo.BlockRebalanceOnPoll())
	}

	client, err := kgo.NewClient(kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Consumer{Client: client}, nil
}

func parseBalancers(balancers []kafka.Balancer) ([]kgo.GroupBalancer, error) {
	if len(balancers) == 0 {
		return []kgo.GroupBalancer{kgo.StickyBalancer()}, nil
	}

	result := make([]kgo.GroupBalancer, len(balancers))
	for i, b := range balancers {
		switch b {
		case kafka.CooperativeStickyBalancer:
			result[i] = kgo.CooperativeStickyBalancer()
		case kafka.StickyBalancer:
			result[i] = kgo.StickyBalancer()
		case kafka.RangeBalancer:
			result[i] = kgo.RangeBalancer()
		case kafka.RoundRobinBalancer:
			result[i] = kgo.RoundRobinBalancer()
		default:
			return nil, kafka.ErrUnsupportedGroupBalancer
		}
	}
	return result, nil
}
