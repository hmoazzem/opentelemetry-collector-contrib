// Package natsreceiver implements a receiver for OpenTelemetry Collector
// that consumes telemetry data from NATS and NATS JetStream.
package natsreceiver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// Config defines configuration for the NATS receiver
type Config struct {
	configretry.BackOffConfig `mapstructure:"backoff_config"`

	// NATS server URLs
	Servers []string `mapstructure:"servers"`

	// Authentication
	Auth AuthConfig `mapstructure:"auth"`

	// Connection settings
	Connection ConnectionConfig `mapstructure:"connection"`

	// Signal-specific consumers
	Traces   *ConsumerConfig `mapstructure:"traces"`
	Metrics  *ConsumerConfig `mapstructure:"metrics"`
	Logs     *ConsumerConfig `mapstructure:"logs"`
	Profiles *ConsumerConfig `mapstructure:"profiles"`

	// Header extraction
	HeaderExtraction HeaderExtractionConfig `mapstructure:"header_extraction"`
}

// AuthConfig contains authentication configuration
type AuthConfig struct {
	Token        string    `mapstructure:"token"`
	Username     string    `mapstructure:"username"`
	Password     string    `mapstructure:"password"`
	NKeySeedFile string    `mapstructure:"nkey_seed_file"`
	JWTFile      string    `mapstructure:"jwt_file"`
	SeedFile     string    `mapstructure:"seed_file"`
	TLS          TLSConfig `mapstructure:"tls"`
}

// TLSConfig contains TLS configuration
type TLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// ConnectionConfig contains connection-level configuration
type ConnectionConfig struct {
	Name            string        `mapstructure:"name"`
	MaxReconnects   int           `mapstructure:"max_reconnects"`
	ReconnectWait   time.Duration `mapstructure:"reconnect_wait"`
	ReconnectJitter time.Duration `mapstructure:"reconnect_jitter"`
	PingInterval    time.Duration `mapstructure:"ping_interval"`
	MaxPingsOut     int           `mapstructure:"max_pings_out"`
	DrainTimeout    time.Duration `mapstructure:"drain_timeout"`
}

// ConsumerConfig defines configuration for a signal-specific consumer
type ConsumerConfig struct {
	ConsumerType string           `mapstructure:"consumer_type"` // "jetstream" or "core"
	Subjects     []string         `mapstructure:"subjects"`
	Encoding     string           `mapstructure:"encoding"`
	Workers      int              `mapstructure:"workers"`
	Retry        RetryConfig      `mapstructure:"retry"`
	JetStream    *JetStreamConfig `mapstructure:"jetstream"`
}

// JetStreamConfig contains JetStream-specific configuration
type JetStreamConfig struct {
	Stream         string        `mapstructure:"stream"`
	Consumer       string        `mapstructure:"consumer"`
	Durable        bool          `mapstructure:"durable"`
	DeliverPolicy  string        `mapstructure:"deliver_policy"`
	AckPolicy      string        `mapstructure:"ack_policy"`
	AckWait        time.Duration `mapstructure:"ack_wait"`
	MaxDeliver     int           `mapstructure:"max_deliver"`
	FilterSubjects []string      `mapstructure:"filter_subjects"`
	Pull           *PullConfig   `mapstructure:"pull"`
	Push           *PushConfig   `mapstructure:"push"`
}

// PullConfig contains pull-based consumer configuration
type PullConfig struct {
	BatchSize     int `mapstructure:"batch_size"`
	MaxWaiting    int `mapstructure:"max_waiting"`
	MaxAckPending int `mapstructure:"max_ack_pending"`
}

// PushConfig contains push-based consumer configuration
type PushConfig struct {
	DeliverSubject string `mapstructure:"deliver_subject"`
	DeliverGroup   string `mapstructure:"deliver_group"`
}

// RetryConfig contains retry configuration
type RetryConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	InitialInterval time.Duration `mapstructure:"initial_interval"`
	MaxInterval     time.Duration `mapstructure:"max_interval"`
	MaxElapsedTime  time.Duration `mapstructure:"max_elapsed_time"`
}

// HeaderExtractionConfig configures header extraction
type HeaderExtractionConfig struct {
	ExtractHeaders bool     `mapstructure:"extract_headers"`
	Headers        []string `mapstructure:"headers"`
}

// Validate checks if the configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.Servers) == 0 {
		return errors.New("at least one NATS server must be specified")
	}

	if cfg.Traces == nil && cfg.Metrics == nil && cfg.Logs == nil && cfg.Profiles == nil {
		return errors.New("at least one signal consumer must be configured")
	}

	if cfg.Traces != nil {
		if err := cfg.Traces.Validate(); err != nil {
			return fmt.Errorf("traces consumer: %w", err)
		}
	}
	if cfg.Metrics != nil {
		if err := cfg.Metrics.Validate(); err != nil {
			return fmt.Errorf("metrics consumer: %w", err)
		}
	}
	if cfg.Logs != nil {
		if err := cfg.Logs.Validate(); err != nil {
			return fmt.Errorf("logs consumer: %w", err)
		}
	}

	return nil
}

// Validate checks if ConsumerConfig is valid
func (c *ConsumerConfig) Validate() error {
	if len(c.Subjects) == 0 {
		return errors.New("at least one subject must be specified")
	}

	if c.ConsumerType != "jetstream" && c.ConsumerType != "core" {
		return fmt.Errorf("invalid consumer_type: %s (must be 'jetstream' or 'core')", c.ConsumerType)
	}

	if c.ConsumerType == "jetstream" && c.JetStream == nil {
		return errors.New("jetstream configuration is required when consumer_type is 'jetstream'")
	}

	if c.Workers <= 0 {
		return errors.New("workers must be greater than 0")
	}

	return nil
}

const (
	typeStr   = "nats"
	stability = component.StabilityLevelBeta
)

// NewFactory creates a factory for NATS receiver
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, stability),
		receiver.WithMetrics(createMetricsReceiver, stability),
		receiver.WithLogs(createLogsReceiver, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Servers: []string{"nats://localhost:4222"},
		Connection: ConnectionConfig{
			Name:            "otel-collector-receiver",
			MaxReconnects:   -1,
			ReconnectWait:   2 * time.Second,
			ReconnectJitter: 100 * time.Millisecond,
			PingInterval:    2 * time.Minute,
			MaxPingsOut:     2,
			DrainTimeout:    30 * time.Second,
		},
		HeaderExtraction: HeaderExtractionConfig{
			ExtractHeaders: true,
		},
	}
}

func createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	c := cfg.(*Config)
	if c.Traces == nil {
		return nil, errors.New("traces consumer not configured")
	}
	return newTracesReceiver(c, set, nextConsumer)
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	c := cfg.(*Config)
	if c.Metrics == nil {
		return nil, errors.New("metrics consumer not configured")
	}
	return newMetricsReceiver(c, set, nextConsumer)
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	c := cfg.(*Config)
	if c.Logs == nil {
		return nil, errors.New("logs consumer not configured")
	}
	return newLogsReceiver(c, set, nextConsumer)
}

// tracesReceiver implements receiver.Traces
type tracesReceiver struct {
	config       *Config
	settings     receiver.Settings
	nextConsumer consumer.Traces
	receiver     *natsReceiver
}

func newTracesReceiver(
	config *Config,
	settings receiver.Settings,
	nextConsumer consumer.Traces,
) (*tracesReceiver, error) {
	return &tracesReceiver{
		config:       config,
		settings:     settings,
		nextConsumer: nextConsumer,
	}, nil
}

func (r *tracesReceiver) Start(ctx context.Context, _ component.Host) error {
	recv, err := newNATSReceiver(r.config, r.settings, "traces")
	if err != nil {
		return err
	}
	r.receiver = recv

	unmarshaler, err := newTracesUnmarshaler(r.config.Traces.Encoding)
	if err != nil {
		return fmt.Errorf("failed to create unmarshaler: %w", err)
	}

	return r.receiver.startConsuming(ctx, r.config.Traces, func(ctx context.Context, msg *nats.Msg) error {
		ctx = client.NewContext(ctx, extractMetadata(msg, r.config.HeaderExtraction))
		traces, err := unmarshaler.Unmarshal(msg.Data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal traces: %w", err)
		}
		return r.nextConsumer.ConsumeTraces(ctx, traces)
	})
}

func (r *tracesReceiver) Shutdown(ctx context.Context) error {
	if r.receiver != nil {
		return r.receiver.Shutdown(ctx)
	}
	return nil
}

// metricsReceiver implements receiver.Metrics
type metricsReceiver struct {
	config       *Config
	settings     receiver.Settings
	nextConsumer consumer.Metrics
	receiver     *natsReceiver
}

func newMetricsReceiver(
	config *Config,
	settings receiver.Settings,
	nextConsumer consumer.Metrics,
) (*metricsReceiver, error) {
	return &metricsReceiver{
		config:       config,
		settings:     settings,
		nextConsumer: nextConsumer,
	}, nil
}

func (r *metricsReceiver) Start(ctx context.Context, _ component.Host) error {
	recv, err := newNATSReceiver(r.config, r.settings, "metrics")
	if err != nil {
		return err
	}
	r.receiver = recv

	unmarshaler, err := newMetricsUnmarshaler(r.config.Metrics.Encoding)
	if err != nil {
		return fmt.Errorf("failed to create unmarshaler: %w", err)
	}

	return r.receiver.startConsuming(ctx, r.config.Metrics, func(ctx context.Context, msg *nats.Msg) error {
		ctx = client.NewContext(ctx, extractMetadata(msg, r.config.HeaderExtraction))
		metrics, err := unmarshaler.Unmarshal(msg.Data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal metrics: %w", err)
		}
		return r.nextConsumer.ConsumeMetrics(ctx, metrics)
	})
}

func (r *metricsReceiver) Shutdown(ctx context.Context) error {
	if r.receiver != nil {
		return r.receiver.Shutdown(ctx)
	}
	return nil
}

// logsReceiver implements receiver.Logs
type logsReceiver struct {
	config       *Config
	settings     receiver.Settings
	nextConsumer consumer.Logs
	receiver     *natsReceiver
}

func newLogsReceiver(
	config *Config,
	settings receiver.Settings,
	nextConsumer consumer.Logs,
) (*logsReceiver, error) {
	return &logsReceiver{
		config:       config,
		settings:     settings,
		nextConsumer: nextConsumer,
	}, nil
}

func (r *logsReceiver) Start(ctx context.Context, _ component.Host) error {
	recv, err := newNATSReceiver(r.config, r.settings, "logs")
	if err != nil {
		return err
	}
	r.receiver = recv

	unmarshaler, err := newLogsUnmarshaler(r.config.Logs.Encoding)
	if err != nil {
		return fmt.Errorf("failed to create unmarshaler: %w", err)
	}

	return r.receiver.startConsuming(ctx, r.config.Logs, func(ctx context.Context, msg *nats.Msg) error {
		ctx = client.NewContext(ctx, extractMetadata(msg, r.config.HeaderExtraction))
		logs, err := unmarshaler.Unmarshal(msg.Data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal logs: %w", err)
		}
		return r.nextConsumer.ConsumeLogs(ctx, logs)
	})
}

func (r *logsReceiver) Shutdown(ctx context.Context) error {
	if r.receiver != nil {
		return r.receiver.Shutdown(ctx)
	}
	return nil
}

// core NATS receiver
type messageHandler func(ctx context.Context, msg *nats.Msg) error

type natsReceiver struct {
	config     *Config
	settings   receiver.Settings
	signalType string
	conn       *nats.Conn
	js         jetstream.JetStream
	workerPool *workerPool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	logger     *zap.Logger
}

func newNATSReceiver(
	config *Config,
	settings receiver.Settings,
	signalType string,
) (*natsReceiver, error) {
	return &natsReceiver{
		config:     config,
		settings:   settings,
		signalType: signalType,
		logger:     settings.Logger,
	}, nil
}

func (r *natsReceiver) startConsuming(
	ctx context.Context,
	consumerCfg *ConsumerConfig,
	handler messageHandler,
) error {
	conn, err := r.createConnection()
	if err != nil {
		return fmt.Errorf("failed to create NATS connection: %w", err)
	}
	r.conn = conn

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.workerPool = newWorkerPool(consumerCfg.Workers, r.logger)
	r.workerPool.start(ctx)

	if consumerCfg.ConsumerType == "jetstream" {
		js, err := jetstream.New(r.conn)
		if err != nil {
			return fmt.Errorf("failed to create JetStream context: %w", err)
		}
		r.js = js
		return r.startJetStreamConsumer(ctx, consumerCfg, handler)
	}

	return r.startCoreConsumer(ctx, consumerCfg, handler)
}

func (r *natsReceiver) createConnection() (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(r.config.Connection.Name),
		nats.MaxReconnects(r.config.Connection.MaxReconnects),
		nats.ReconnectWait(r.config.Connection.ReconnectWait),
		nats.ReconnectJitter(r.config.Connection.ReconnectJitter, r.config.Connection.ReconnectJitter),
		nats.PingInterval(r.config.Connection.PingInterval),
		nats.MaxPingsOutstanding(r.config.Connection.MaxPingsOut),
		nats.DrainTimeout(r.config.Connection.DrainTimeout),
	}

	if err := r.addAuthOptions(&opts); err != nil {
		return nil, fmt.Errorf("failed to configure authentication: %w", err)
	}

	opts = append(opts,
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			r.logger.Warn("Disconnected from NATS", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			r.logger.Info("Reconnected to NATS", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			r.logger.Error("NATS error", zap.Error(err))
		}),
	)

	conn, err := nats.Connect(strings.Join(r.config.Servers, ","), opts...)
	if err != nil {
		return nil, err
	}

	r.logger.Info("Connected to NATS", zap.Strings("servers", r.config.Servers))
	return conn, nil
}

func (r *natsReceiver) addAuthOptions(opts *[]nats.Option) error {
	auth := r.config.Auth

	if auth.Token != "" {
		*opts = append(*opts, nats.Token(auth.Token))
		return nil
	}
	if auth.Username != "" && auth.Password != "" {
		*opts = append(*opts, nats.UserInfo(auth.Username, auth.Password))
		return nil
	}
	if auth.NKeySeedFile != "" {
		opt, err := nats.NkeyOptionFromSeed(auth.NKeySeedFile)
		if err != nil {
			return err
		}
		*opts = append(*opts, opt)
		return nil
	}
	if auth.JWTFile != "" && auth.SeedFile != "" {
		*opts = append(*opts, nats.UserCredentials(auth.JWTFile, auth.SeedFile))
		return nil
	}
	if auth.TLS.CertFile != "" && auth.TLS.KeyFile != "" {
		tlsConfig, err := r.createTLSConfig()
		if err != nil {
			return err
		}
		*opts = append(*opts, nats.Secure(tlsConfig))
	}

	return nil
}

func (r *natsReceiver) createTLSConfig() (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: r.config.Auth.TLS.InsecureSkipVerify,
	}

	if r.config.Auth.TLS.CAFile != "" {
		caCert, err := os.ReadFile(r.config.Auth.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsCfg.RootCAs = caCertPool
	}

	if r.config.Auth.TLS.CertFile != "" && r.config.Auth.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(r.config.Auth.TLS.CertFile, r.config.Auth.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

func (r *natsReceiver) startJetStreamConsumer(
	ctx context.Context,
	consumerCfg *ConsumerConfig,
	handler messageHandler,
) error {
	jsCfg := consumerCfg.JetStream

	consumer, err := r.getOrCreateConsumer(jsCfg)
	if err != nil {
		return fmt.Errorf("failed to get/create JetStream consumer: %w", err)
	}

	if jsCfg.Pull != nil {
		return r.startPullConsumer(ctx, consumer, jsCfg.Pull, handler)
	}
	return r.startPushConsumer(ctx, consumer, handler)
}

func (r *natsReceiver) getOrCreateConsumer(cfg *JetStreamConfig) (jetstream.Consumer, error) {
	consumer, err := r.js.Consumer(context.Background(), cfg.Stream, cfg.Consumer)
	if err == nil {
		r.logger.Info("Using existing JetStream consumer",
			zap.String("stream", cfg.Stream),
			zap.String("consumer", cfg.Consumer))
		return consumer, nil
	}

	consumerCfg := jetstream.ConsumerConfig{
		Name:           cfg.Consumer,
		Durable:        cfg.Consumer,
		FilterSubjects: cfg.FilterSubjects,
		AckWait:        cfg.AckWait,
		MaxDeliver:     cfg.MaxDeliver,
	}

	switch cfg.DeliverPolicy {
	case "all":
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case "last":
		consumerCfg.DeliverPolicy = jetstream.DeliverLastPolicy
	case "new":
		consumerCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	default:
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	switch cfg.AckPolicy {
	case "explicit":
		consumerCfg.AckPolicy = jetstream.AckExplicitPolicy
	case "all":
		consumerCfg.AckPolicy = jetstream.AckAllPolicy
	case "none":
		consumerCfg.AckPolicy = jetstream.AckNonePolicy
	default:
		consumerCfg.AckPolicy = jetstream.AckExplicitPolicy
	}

	consumer, err = r.js.CreateOrUpdateConsumer(context.Background(), cfg.Stream, consumerCfg)
	if err != nil {
		return nil, err
	}

	r.logger.Info("Created JetStream consumer",
		zap.String("stream", cfg.Stream),
		zap.String("consumer", cfg.Consumer))

	return consumer, nil
}

func (r *natsReceiver) startPullConsumer(
	ctx context.Context,
	consumer jetstream.Consumer,
	pullCfg *PullConfig,
	handler messageHandler,
) error {
	r.wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msgs, err := consumer.Fetch(pullCfg.BatchSize, jetstream.FetchMaxWait(5*time.Second))
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					r.logger.Error("Failed to fetch messages", zap.Error(err))
					time.Sleep(time.Second)
					continue
				}
				for msg := range msgs.Messages() {
					select {
					case <-ctx.Done():
						return
					default:
						r.workerPool.submit(&task{msg: msg, handler: handler, logger: r.logger})
					}
				}
			}
		}
	})
	return nil
}

func (r *natsReceiver) startPushConsumer(
	_ context.Context,
	consumer jetstream.Consumer,
	handler messageHandler,
) error {
	_, err := consumer.Consume(func(msg jetstream.Msg) {
		r.workerPool.submit(&task{msg: msg, handler: handler, logger: r.logger})
	})
	return err
}

func (r *natsReceiver) startCoreConsumer(
	_ context.Context,
	consumerCfg *ConsumerConfig,
	handler messageHandler,
) error {
	for _, subject := range consumerCfg.Subjects {
		_, err := r.conn.Subscribe(subject, func(msg *nats.Msg) {
			r.workerPool.submit(&task{msg: msg, handler: handler, logger: r.logger})
		})
		if err != nil {
			return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
		}
		r.logger.Info("Subscribed to subject", zap.String("subject", subject))
	}
	return nil
}

func (r *natsReceiver) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	if r.conn != nil {
		if err := r.conn.Drain(); err != nil {
			r.logger.Warn("Error draining NATS connection", zap.Error(err))
		}
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		if r.workerPool != nil {
			r.workerPool.shutdown()
		}
		close(done)
	}()

	select {
	case <-done:
		r.logger.Info("NATS receiver shutdown complete")
	case <-ctx.Done():
		r.logger.Warn("Shutdown timeout exceeded")
	}

	if r.conn != nil {
		r.conn.Close()
	}

	return nil
}

// Worker pool
type task struct {
	msg     any // *nats.Msg or jetstream.Msg
	handler messageHandler
	logger  *zap.Logger
}

func (t *task) execute(ctx context.Context) {
	var natsMsg *nats.Msg

	switch msg := t.msg.(type) {
	case *nats.Msg:
		natsMsg = msg
	case jetstream.Msg:
		natsMsg = &nats.Msg{
			Subject: msg.Subject(),
			Reply:   msg.Reply(),
			Data:    msg.Data(),
			Header:  msg.Headers(),
		}
	default:
		t.logger.Error("Unknown message type")
		return
	}

	if err := t.handler(ctx, natsMsg); err != nil {
		t.logger.Error("Failed to process message",
			zap.String("subject", natsMsg.Subject),
			zap.Error(err))

		if jsMsg, ok := t.msg.(jetstream.Msg); ok {
			if nakErr := jsMsg.Nak(); nakErr != nil {
				t.logger.Error("Failed to NAK message", zap.Error(nakErr))
			}
		}
		return
	}

	if jsMsg, ok := t.msg.(jetstream.Msg); ok {
		if ackErr := jsMsg.Ack(); ackErr != nil {
			t.logger.Error("Failed to ACK message", zap.Error(ackErr))
		}
	}
}

type workerPool struct {
	workers int
	tasks   chan *task
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *zap.Logger
}

func newWorkerPool(workers int, logger *zap.Logger) *workerPool {
	return &workerPool{
		workers: workers,
		tasks:   make(chan *task, workers*10),
		logger:  logger,
	}
}

func (wp *workerPool) start(ctx context.Context) {
	wp.ctx, wp.cancel = context.WithCancel(ctx)
	for i := range wp.workers {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	wp.logger.Info("Started worker pool", zap.Int("workers", wp.workers))
}

func (wp *workerPool) worker(_ int) {
	defer wp.wg.Done()
	for {
		select {
		case <-wp.ctx.Done():
			return
		case task := <-wp.tasks:
			task.execute(wp.ctx)
		}
	}
}

func (wp *workerPool) submit(task *task) {
	select {
	case wp.tasks <- task:
	case <-wp.ctx.Done():
	}
}

func (wp *workerPool) shutdown() {
	if wp.cancel != nil {
		wp.cancel()
	}
	wp.wg.Wait()
	close(wp.tasks)
}

// Metadata extraction
func extractMetadata(msg *nats.Msg, config HeaderExtractionConfig) client.Info {
	headers := make(map[string][]string)

	if config.ExtractHeaders {
		headers["nats.subject"] = []string{msg.Subject}
		if msg.Reply != "" {
			headers["nats.reply"] = []string{msg.Reply}
		}
		for _, key := range config.Headers {
			if values := msg.Header.Values(key); len(values) > 0 {
				headers[key] = values
			}
		}
	}

	return client.Info{
		Metadata: client.NewMetadata(headers),
	}
}

// Unmarshalers
type TracesUnmarshaler interface {
	Unmarshal([]byte) (ptrace.Traces, error)
}

type MetricsUnmarshaler interface {
	Unmarshal([]byte) (pmetric.Metrics, error)
}

type LogsUnmarshaler interface {
	Unmarshal([]byte) (plog.Logs, error)
}

// Traces
type otlpProtoTracesUnmarshaler struct{}

func (u *otlpProtoTracesUnmarshaler) Unmarshal(b []byte) (ptrace.Traces, error) {
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		return ptrace.Traces{}, err
	}
	return req.Traces(), nil
}

type otlpJSONTracesUnmarshaler struct{}

func (u *otlpJSONTracesUnmarshaler) Unmarshal(b []byte) (ptrace.Traces, error) {
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalJSON(b); err != nil {
		return ptrace.Traces{}, err
	}
	return req.Traces(), nil
}

func newTracesUnmarshaler(encoding string) (TracesUnmarshaler, error) {
	switch encoding {
	case "otlp_proto":
		return &otlpProtoTracesUnmarshaler{}, nil
	case "otlp_json":
		return &otlpJSONTracesUnmarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

// Metrics
type otlpProtoMetricsUnmarshaler struct{}

func (u *otlpProtoMetricsUnmarshaler) Unmarshal(b []byte) (pmetric.Metrics, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		return pmetric.Metrics{}, err
	}
	return req.Metrics(), nil
}

type otlpJSONMetricsUnmarshaler struct{}

func (u *otlpJSONMetricsUnmarshaler) Unmarshal(b []byte) (pmetric.Metrics, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalJSON(b); err != nil {
		return pmetric.Metrics{}, err
	}
	return req.Metrics(), nil
}

func newMetricsUnmarshaler(encoding string) (MetricsUnmarshaler, error) {
	switch encoding {
	case "otlp_proto":
		return &otlpProtoMetricsUnmarshaler{}, nil
	case "otlp_json":
		return &otlpJSONMetricsUnmarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

// Logs
type otlpProtoLogsUnmarshaler struct{}

func (u *otlpProtoLogsUnmarshaler) Unmarshal(b []byte) (plog.Logs, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		return plog.Logs{}, err
	}
	return req.Logs(), nil
}

type otlpJSONLogsUnmarshaler struct{}

func (u *otlpJSONLogsUnmarshaler) Unmarshal(b []byte) (plog.Logs, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalJSON(b); err != nil {
		return plog.Logs{}, err
	}
	return req.Logs(), nil
}

func newLogsUnmarshaler(encoding string) (LogsUnmarshaler, error) {
	switch encoding {
	case "otlp_proto":
		return &otlpProtoLogsUnmarshaler{}, nil
	case "otlp_json":
		return &otlpJSONLogsUnmarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

// Circuit breaker — wired but not yet used at the call sites.
// Kept for future use when wrapping the downstream consumer call.
type circuitBreaker struct {
	maxFailures  int32
	resetTimeout time.Duration
	failures     atomic.Int32
	state        atomic.Int32 // 0=closed, 1=open, 2=half-open
	lastFailTime atomic.Int64
}

func newCircuitBreaker(maxFailures int, resetTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		maxFailures:  int32(maxFailures),
		resetTimeout: resetTimeout,
	}
}

func (cb *circuitBreaker) canAttempt() bool {
	switch cb.state.Load() {
	case 0:
		return true
	case 1:
		lastFail := time.Unix(0, cb.lastFailTime.Load())
		if time.Since(lastFail) >= cb.resetTimeout {
			if cb.state.CompareAndSwap(1, 2) {
				return true
			}
		}
		return false
	default: // half-open
		return true
	}
}

func (cb *circuitBreaker) recordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(0)
}

func (cb *circuitBreaker) recordFailure() {
	if cb.failures.Add(1) >= cb.maxFailures {
		cb.state.Store(1)
	}
	cb.lastFailTime.Store(time.Now().UnixNano())
}

func (cb *circuitBreaker) call(fn func() error) error {
	if !cb.canAttempt() {
		return errors.New("circuit breaker is open")
	}
	if err := fn(); err != nil {
		cb.recordFailure()
		return err
	}
	cb.recordSuccess()
	return nil
}
