// Package natsexporter implements an exporter for OpenTelemetry Collector
// that sends telemetry data to NATS and NATS JetStream.
package natsexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/natsexporter"

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

const (
	typeStr   = "nats"
	stability = component.StabilityLevelBeta
)

// Config defines configuration for the NATS exporter.
type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	Queue                        configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	configretry.BackOffConfig    `mapstructure:"retry_on_failure"`

	// Servers is a list of NATS server URLs.
	Servers []string `mapstructure:"servers"`

	// Auth contains authentication configuration.
	Auth AuthConfig `mapstructure:"auth"`

	// Connection contains connection-level configuration.
	Connection ConnectionConfig `mapstructure:"connection"`

	// Traces contains traces-specific publisher configuration.
	Traces *PublisherConfig `mapstructure:"traces"`

	// Metrics contains metrics-specific publisher configuration.
	Metrics *PublisherConfig `mapstructure:"metrics"`

	// Logs contains logs-specific publisher configuration.
	Logs *PublisherConfig `mapstructure:"logs"`
}

// AuthConfig contains authentication configuration.
type AuthConfig struct {
	Token        string    `mapstructure:"token"`
	Username     string    `mapstructure:"username"`
	Password     string    `mapstructure:"password"`
	NKeySeedFile string    `mapstructure:"nkey_seed_file"`
	JWTFile      string    `mapstructure:"jwt_file"`
	SeedFile     string    `mapstructure:"seed_file"`
	TLS          TLSConfig `mapstructure:"tls"`
}

// TLSConfig contains TLS configuration.
type TLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// ConnectionConfig contains connection-level configuration.
type ConnectionConfig struct {
	Name            string        `mapstructure:"name"`
	MaxReconnects   int           `mapstructure:"max_reconnects"`
	ReconnectWait   time.Duration `mapstructure:"reconnect_wait"`
	ReconnectJitter time.Duration `mapstructure:"reconnect_jitter"`
	PingInterval    time.Duration `mapstructure:"ping_interval"`
	MaxPingsOut     int           `mapstructure:"max_pings_out"`
	DrainTimeout    time.Duration `mapstructure:"drain_timeout"`
}

// PublisherConfig defines configuration for a signal-specific publisher.
type PublisherConfig struct {
	Subject   string            `mapstructure:"subject"`
	Encoding  string            `mapstructure:"encoding"`
	JetStream *JetStreamConfig  `mapstructure:"jetstream"`
	Headers   map[string]string `mapstructure:"headers"`
}

// JetStreamConfig contains JetStream-specific configuration.
type JetStreamConfig struct {
	Enabled             bool          `mapstructure:"enabled"`
	Stream              string        `mapstructure:"stream"`
	MaxWait             time.Duration `mapstructure:"max_wait"`
	DeduplicationWindow time.Duration `mapstructure:"deduplication_window"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if len(cfg.Servers) == 0 {
		return errors.New("at least one NATS server must be specified")
	}

	if cfg.Traces == nil && cfg.Metrics == nil && cfg.Logs == nil {
		return errors.New("at least one signal publisher must be configured")
	}

	for name, pub := range map[string]*PublisherConfig{
		"traces":  cfg.Traces,
		"metrics": cfg.Metrics,
		"logs":    cfg.Logs,
	} {
		if pub != nil {
			if err := pub.validate(); err != nil {
				return fmt.Errorf("%s publisher: %w", name, err)
			}
		}
	}

	return nil
}

func (p *PublisherConfig) validate() error {
	if p.Subject == "" {
		return errors.New("subject must be specified")
	}
	if p.Encoding == "" {
		return errors.New("encoding must be specified")
	}
	if p.Encoding != "otlp_proto" && p.Encoding != "otlp_json" {
		return fmt.Errorf("unsupported encoding: %s (must be otlp_proto or otlp_json)", p.Encoding)
	}
	if p.JetStream != nil && p.JetStream.Enabled && p.JetStream.Stream == "" {
		return errors.New("stream name required when JetStream is enabled")
	}
	return nil
}

// NewFactory creates a factory for the NATS exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutConfig: exporterhelper.NewDefaultTimeoutConfig(),
		Queue:         configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		Servers:       []string{"nats://localhost:4222"},
		Connection: ConnectionConfig{
			Name:            "otel-collector-exporter",
			MaxReconnects:   -1,
			ReconnectWait:   2 * time.Second,
			ReconnectJitter: 100 * time.Millisecond,
			PingInterval:    2 * time.Minute,
			MaxPingsOut:     2,
			DrainTimeout:    30 * time.Second,
		},
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	if c.Traces == nil {
		return nil, errors.New("traces publisher not configured")
	}
	exp, err := newExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(
		ctx, set, cfg,
		exp.pushTraces,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.Queue),
		exporterhelper.WithRetry(c.BackOffConfig),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	if c.Metrics == nil {
		return nil, errors.New("metrics publisher not configured")
	}
	exp, err := newExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(
		ctx, set, cfg,
		exp.pushMetrics,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.Queue),
		exporterhelper.WithRetry(c.BackOffConfig),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	if c.Logs == nil {
		return nil, errors.New("logs publisher not configured")
	}
	exp, err := newExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx, set, cfg,
		exp.pushLogs,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.Queue),
		exporterhelper.WithRetry(c.BackOffConfig),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

// natsExporter implements the NATS exporter.
type natsExporter struct {
	config *Config
	logger *zap.Logger

	mu   sync.RWMutex
	conn *nats.Conn
	js   jetstream.JetStream
}

func newExporter(cfg *Config, set exporter.Settings) (*natsExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return &natsExporter{
		config: cfg,
		logger: set.Logger,
	}, nil
}

func (e *natsExporter) start(ctx context.Context, _ component.Host) error {
	conn, err := e.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	e.mu.Lock()
	e.conn = conn
	e.mu.Unlock()

	if e.needsJetStream() {
		js, err := jetstream.New(conn)
		if err != nil {
			return fmt.Errorf("failed to initialize JetStream: %w", err)
		}
		e.mu.Lock()
		e.js = js
		e.mu.Unlock()

		if err := e.ensureStreams(ctx); err != nil {
			return fmt.Errorf("failed to ensure JetStream streams: %w", err)
		}
	}

	e.logger.Info("NATS exporter started",
		zap.Strings("servers", e.config.Servers),
		zap.String("connection_name", e.config.Connection.Name),
	)
	return nil
}

func (e *natsExporter) shutdown(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.conn != nil {
		if err := e.conn.Drain(); err != nil {
			e.logger.Warn("Failed to drain connection", zap.Error(err))
		}
		e.conn.Close()
		e.conn = nil
		e.js = nil
		e.logger.Info("NATS exporter shutdown complete")
	}
	return nil
}

func (e *natsExporter) connect() (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(e.config.Connection.Name),
		nats.MaxReconnects(e.config.Connection.MaxReconnects),
		nats.ReconnectWait(e.config.Connection.ReconnectWait),
		nats.ReconnectJitter(e.config.Connection.ReconnectJitter, e.config.Connection.ReconnectJitter),
		nats.PingInterval(e.config.Connection.PingInterval),
		nats.MaxPingsOutstanding(e.config.Connection.MaxPingsOut),
		nats.DrainTimeout(e.config.Connection.DrainTimeout),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				e.logger.Warn("Disconnected from NATS", zap.Error(err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			e.logger.Info("Reconnected to NATS", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			e.logger.Error("NATS error", zap.Error(err))
		}),
	}

	if err := e.configureAuth(&opts); err != nil {
		return nil, fmt.Errorf("failed to configure authentication: %w", err)
	}

	return nats.Connect(strings.Join(e.config.Servers, ","), opts...)
}

func (e *natsExporter) configureAuth(opts *[]nats.Option) error {
	auth := &e.config.Auth

	switch {
	case auth.Token != "":
		*opts = append(*opts, nats.Token(auth.Token))
	case auth.Username != "" && auth.Password != "":
		*opts = append(*opts, nats.UserInfo(auth.Username, auth.Password))
	case auth.NKeySeedFile != "":
		opt, err := nats.NkeyOptionFromSeed(auth.NKeySeedFile)
		if err != nil {
			return fmt.Errorf("failed to load NKey: %w", err)
		}
		*opts = append(*opts, opt)
	case auth.JWTFile != "" && auth.SeedFile != "":
		*opts = append(*opts, nats.UserCredentials(auth.JWTFile, auth.SeedFile))
	case auth.TLS.CertFile != "" && auth.TLS.KeyFile != "":
		tlsConfig, err := e.buildTLSConfig()
		if err != nil {
			return err
		}
		*opts = append(*opts, nats.Secure(tlsConfig))
	}

	return nil
}

func (e *natsExporter) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: e.config.Auth.TLS.InsecureSkipVerify,
	}

	if e.config.Auth.TLS.CAFile != "" {
		caCert, err := os.ReadFile(e.config.Auth.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if e.config.Auth.TLS.CertFile != "" && e.config.Auth.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(e.config.Auth.TLS.CertFile, e.config.Auth.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func (e *natsExporter) needsJetStream() bool {
	for _, pub := range []*PublisherConfig{e.config.Traces, e.config.Metrics, e.config.Logs} {
		if pub != nil && pub.JetStream != nil && pub.JetStream.Enabled {
			return true
		}
	}
	return false
}

func (e *natsExporter) ensureStreams(ctx context.Context) error {
	streams := make(map[string][]string)
	for _, pub := range []*PublisherConfig{e.config.Traces, e.config.Metrics, e.config.Logs} {
		if pub != nil && pub.JetStream != nil && pub.JetStream.Enabled {
			streams[pub.JetStream.Stream] = append(streams[pub.JetStream.Stream], pub.Subject)
		}
	}

	for streamName, subjects := range streams {
		if _, err := e.js.Stream(ctx, streamName); err != nil {
			_, err = e.js.CreateStream(ctx, jetstream.StreamConfig{
				Name:      streamName,
				Subjects:  subjects,
				Retention: jetstream.LimitsPolicy,
				Storage:   jetstream.FileStorage,
			})
			if err != nil {
				return fmt.Errorf("failed to create stream %s: %w", streamName, err)
			}
			e.logger.Info("Created JetStream stream",
				zap.String("stream", streamName),
				zap.Strings("subjects", subjects),
			)
		}
	}

	return nil
}

func (e *natsExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	marshaler, err := newTracesMarshaler(e.config.Traces.Encoding)
	if err != nil {
		return err
	}
	data, err := marshaler.MarshalTraces(td)
	if err != nil {
		return fmt.Errorf("failed to marshal traces: %w", err)
	}
	return e.publish(ctx, e.config.Traces, data, "traces")
}

func (e *natsExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	marshaler, err := newMetricsMarshaler(e.config.Metrics.Encoding)
	if err != nil {
		return err
	}
	data, err := marshaler.MarshalMetrics(md)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}
	return e.publish(ctx, e.config.Metrics, data, "metrics")
}

func (e *natsExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	marshaler, err := newLogsMarshaler(e.config.Logs.Encoding)
	if err != nil {
		return err
	}
	data, err := marshaler.MarshalLogs(ld)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}
	return e.publish(ctx, e.config.Logs, data, "logs")
}

func (e *natsExporter) publish(ctx context.Context, pub *PublisherConfig, data []byte, signalType string) error {
	e.mu.RLock()
	conn := e.conn
	js := e.js
	e.mu.RUnlock()

	if conn == nil {
		return errors.New("not connected to NATS")
	}

	msg := &nats.Msg{
		Subject: pub.Subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	msg.Header.Set("otel-signal-type", signalType)
	for key, value := range pub.Headers {
		msg.Header.Set(key, value)
	}

	if pub.JetStream != nil && pub.JetStream.Enabled {
		if js == nil {
			return errors.New("JetStream not initialized")
		}

		publishOpts := []jetstream.PublishOpt{
			jetstream.WithExpectStream(pub.JetStream.Stream),
		}

		if pub.JetStream.MaxWait > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, pub.JetStream.MaxWait)
			defer cancel()
		}

		if _, err := js.PublishMsg(ctx, msg, publishOpts...); err != nil {
			return fmt.Errorf("failed to publish to JetStream: %w", err)
		}

		e.logger.Debug("Published to JetStream",
			zap.String("subject", pub.Subject),
			zap.String("stream", pub.JetStream.Stream),
			zap.String("signal_type", signalType),
			zap.Int("bytes", len(data)),
		)
	} else {
		if err := conn.PublishMsg(msg); err != nil {
			return fmt.Errorf("failed to publish to NATS: %w", err)
		}

		e.logger.Debug("Published to NATS",
			zap.String("subject", pub.Subject),
			zap.String("signal_type", signalType),
			zap.Int("bytes", len(data)),
		)
	}

	return nil
}

// Marshalers
type tracesMarshaler interface {
	MarshalTraces(ptrace.Traces) ([]byte, error)
}

type metricsMarshaler interface {
	MarshalMetrics(pmetric.Metrics) ([]byte, error)
}

type logsMarshaler interface {
	MarshalLogs(plog.Logs) ([]byte, error)
}

type otlpProtoTracesMarshaler struct{}

func (m *otlpProtoTracesMarshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
}

type otlpJSONTracesMarshaler struct{}

func (m *otlpJSONTracesMarshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return ptraceotlp.NewExportRequestFromTraces(td).MarshalJSON()
}

type otlpProtoMetricsMarshaler struct{}

func (m *otlpProtoMetricsMarshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	return pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
}

type otlpJSONMetricsMarshaler struct{}

func (m *otlpJSONMetricsMarshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	return pmetricotlp.NewExportRequestFromMetrics(md).MarshalJSON()
}

type otlpProtoLogsMarshaler struct{}

func (m *otlpProtoLogsMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	return plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
}

type otlpJSONLogsMarshaler struct{}

func (m *otlpJSONLogsMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	return plogotlp.NewExportRequestFromLogs(ld).MarshalJSON()
}

func newTracesMarshaler(encoding string) (tracesMarshaler, error) {
	switch encoding {
	case "otlp_proto":
		return &otlpProtoTracesMarshaler{}, nil
	case "otlp_json":
		return &otlpJSONTracesMarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

func newMetricsMarshaler(encoding string) (metricsMarshaler, error) {
	switch encoding {
	case "otlp_proto":
		return &otlpProtoMetricsMarshaler{}, nil
	case "otlp_json":
		return &otlpJSONMetricsMarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

func newLogsMarshaler(encoding string) (logsMarshaler, error) {
	switch encoding {
	case "otlp_proto":
		return &otlpProtoLogsMarshaler{}, nil
	case "otlp_json":
		return &otlpJSONLogsMarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}
