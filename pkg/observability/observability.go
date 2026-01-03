package observability

import (
	"context"
	"fmt"
	"net/http"

	"go.uber.org/zap"
)

// Config holds configuration for all observability components
type Config struct {
	Metrics MetricsConfig `json:"metrics"`
	Tracing TracingConfig `json:"tracing"`
	Logging LoggingConfig `json:"logging"`
}

// MetricsConfig holds Prometheus metrics configuration
type MetricsConfig struct {
	Enabled bool   `json:"enabled"`
	Port    int    `json:"port"`
	Path    string `json:"path"`
}

// Observability provides a unified interface for metrics, tracing, and logging
type Observability struct {
	Metrics *Metrics
	Logger  *Logger
	config  Config
	server  *http.Server
}

// New creates a new observability instance
func New(config Config) (*Observability, error) {
	obs := &Observability{
		config: config,
	}

	// Initialize metrics
	if config.Metrics.Enabled {
		obs.Metrics = NewMetrics()
	}

	// Initialize logging
	logger, err := NewLogger(config.Logging)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}
	obs.Logger = logger

	return obs, nil
}

// Start starts the observability services
func (o *Observability) Start(ctx context.Context) error {
	// Initialize tracing if configured
	if o.config.Tracing.ServiceName != "" {
		shutdown, err := InitTracing(o.config.Tracing)
		if err != nil {
			o.Logger.WithError(err).Error("Failed to initialize tracing")
		} else {
			o.Logger.Info("Tracing initialized successfully")
			// Store shutdown function for later use
			go func() {
				<-ctx.Done()
				if err := shutdown(context.Background()); err != nil {
					o.Logger.WithError(err).Error("Failed to shutdown tracing")
				}
			}()
		}
	}

	// Start metrics server if enabled
	if o.config.Metrics.Enabled && o.Metrics != nil {
		return o.startMetricsServer()
	}

	return nil
}

// Stop stops the observability services
func (o *Observability) Stop(ctx context.Context) error {
	if o.server != nil {
		return o.server.Shutdown(ctx)
	}
	return nil
}

// startMetricsServer starts the Prometheus metrics HTTP server
func (o *Observability) startMetricsServer() error {
	mux := http.NewServeMux()
	mux.Handle(o.config.Metrics.Path, o.Metrics.Handler())

	// Add health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	o.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", o.config.Metrics.Port),
		Handler: mux,
	}

	o.Logger.Info("Starting metrics server",
		zap.Int("port", o.config.Metrics.Port),
		zap.String("path", o.config.Metrics.Path))

	go func() {
		if err := o.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			o.Logger.WithError(err).Error("Metrics server failed")
		}
	}()

	return nil
}

// DefaultConfig returns a default observability configuration
func DefaultConfig(serviceName string) Config {
	return Config{
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
			Path:    "/metrics",
		},
		Tracing: TracingConfig{
			ServiceName:    serviceName,
			ServiceVersion: "1.0.0",
			JaegerEndpoint: "http://localhost:14268/api/traces",
			SamplingRate:   1.0, // Sample all traces in development
		},
		Logging: LoggingConfig{
			Level:       "info",
			Development: false,
			OutputPaths: []string{"stdout"},
		},
	}
}

// DevelopmentConfig returns a development observability configuration
func DevelopmentConfig(serviceName string) Config {
	config := DefaultConfig(serviceName)
	config.Logging.Development = true
	config.Logging.Level = "debug"
	return config
}
