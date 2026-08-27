package parser

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/nikon11211/kafka-connector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kprom"
)

func ParseConfig(cfg *kafka.Config, logger kafka.Logger, registerer prometheus.Registerer) ([]kgo.Opt, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.WithLogger(logger),
	}

	if cfg.TLS.Enabled {
		tlsOpts, err := parseTLS(&cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("TLS configuration error: %w", err)
		}
		opts = append(opts, tlsOpts...)
	}

	if cfg.SASL.Username != "" && cfg.SASL.Password != "" {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.SASL.Username,
			Pass: cfg.SASL.Password,
		}.AsSha512Mechanism()))
	}

	metrics := kprom.NewMetrics(
		cfg.Metrics.Namespace,
		kprom.Registerer(registerer),
		kprom.FetchAndProduceDetail(
			kprom.ByNode,
			kprom.ByTopic,
			kprom.Batches,
			kprom.Records,
			kprom.UncompressedBytes,
		),
	)
	opts = append(opts, kgo.WithHooks(metrics))

	timeoutOpts := parseTimeouts(&cfg.Timeout)
	opts = append(opts, timeoutOpts...)

	return opts, nil
}

func parseTLS(tlsCfg *kafka.TLS) ([]kgo.Opt, error) {
	if tlsCfg == nil {
		return nil, errors.New("TLS config is nil")
	}

	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if tlsCfg.CAFile != "" {
		caCert, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: tlsCfg.InsecureSkipVerify, // #nosec G402 -- TLS verification is intentionally configurable
		RootCAs:            caCertPool,
		Certificates:       []tls.Certificate{cert},
	}

	tlsDialer := &tls.Dialer{Config: tlsConfig}

	return []kgo.Opt{kgo.Dialer(tlsDialer.DialContext)}, nil
}

func parseTimeouts(timeoutCfg *kafka.Timeout) []kgo.Opt {
	if timeoutCfg == nil {
		return nil
	}

	opts := make([]kgo.Opt, 0)

	if timeoutCfg.Dial > 0 {
		opts = append(opts, kgo.DialTimeout(timeoutCfg.Dial))
	}
	if timeoutCfg.ConnIdle > 0 {
		opts = append(opts, kgo.ConnIdleTimeout(timeoutCfg.ConnIdle))
	}
	if timeoutCfg.RequestOverhead > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(timeoutCfg.RequestOverhead))
	}
	if timeoutCfg.Rebalance > 0 {
		opts = append(opts, kgo.RebalanceTimeout(timeoutCfg.Rebalance))
	}
	if timeoutCfg.Retry > 0 {
		opts = append(opts, kgo.RetryTimeout(timeoutCfg.Retry))
	}
	if timeoutCfg.Session > 0 {
		opts = append(opts, kgo.SessionTimeout(timeoutCfg.Session))
	}
	if timeoutCfg.ProduceRequest > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(timeoutCfg.ProduceRequest))
	}
	if timeoutCfg.RecordDelivery > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(timeoutCfg.RecordDelivery))
	}
	if timeoutCfg.TransactionTimeout > 0 {
		opts = append(opts, kgo.TransactionTimeout(timeoutCfg.TransactionTimeout))
	}

	return opts
}
