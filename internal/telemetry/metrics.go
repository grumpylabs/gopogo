// Package telemetry provides OpenTelemetry metrics and tracing instrumentation
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config holds telemetry configuration
type Config struct {
	Enabled        bool
	ExporterType   string // "otlp", "stdout"
	Protocol       string            // "grpc" or "http"; empty uses OTEL_EXPORTER_OTLP_PROTOCOL, else grpc
	OTLPEndpoint   string            // host:port or base URL; empty uses OTEL_EXPORTER_OTLP_*
	Insecure       bool              // plaintext OTLP to a host:port endpoint
	Headers        map[string]string // extra OTLP request headers, e.g. Authorization
	ServiceName    string
	ServiceVersion string
	Environment    string
	SampleRatio    float64 // fraction of new traces to sample, 0-1
}

// Metrics holds the metric instruments for gopogo
type Metrics struct {
	meter         metric.Meter
	meterProvider *sdkmetric.MeterProvider

	// Cache operation counts
	CacheStoreCount  metric.Int64Counter
	CacheLoadCount   metric.Int64Counter
	CacheDeleteCount metric.Int64Counter

	// Cache operation durations
	CacheStoreDuration  metric.Float64Histogram
	CacheLoadDuration   metric.Float64Histogram
	CacheDeleteDuration metric.Float64Histogram

	// Cache hit/miss
	CacheHitCount  metric.Int64Counter
	CacheMissCount metric.Int64Counter

	// Memory
	CacheMemoryUsed metric.Int64ObservableGauge
	CacheItemCount  metric.Int64ObservableGauge

	// Eviction and expiration
	CacheEvictionCount  metric.Int64Counter
	CacheExpirationCount metric.Int64Counter

	// Persistence
	CacheSaveDuration metric.Float64Histogram
	CacheLoadFileDuration metric.Float64Histogram
	CacheSaveErrors   metric.Int64Counter
	CacheLoadFileErrors metric.Int64Counter

	// Sweep
	CacheSweepDuration metric.Float64Histogram
	CacheSweepExpired  metric.Int64Counter
}

// createResource creates a resource with service information
func createResource(cfg *Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
	}
	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironment(cfg.Environment))
	}
	// OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME override the defaults.
	return resource.New(context.Background(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
}

// NewMetrics creates and initializes OpenTelemetry metrics
func NewMetrics(ctx context.Context, cfg *Config) (*Metrics, error) {
	m := &Metrics{}

	if !cfg.Enabled {
		otel.SetMeterProvider(noop.NewMeterProvider())
		return m, nil
	}

	res, err := createResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var exporter sdkmetric.Exporter
	switch cfg.ExporterType {
	case "otlp":
		exporter, err = newOTLPMetricExporter(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
	case "stdout":
		exporter, err = stdoutmetric.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", cfg.ExporterType)
	}

	reader := sdkmetric.NewPeriodicReader(exporter,
		sdkmetric.WithInterval(30*time.Second),
	)

	m.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)

	otel.SetMeterProvider(m.meterProvider)
	m.meter = m.meterProvider.Meter(cfg.ServiceName)

	if err := m.initializeInstruments(); err != nil {
		return nil, err
	}

	// Go runtime metrics (go.memory.*, go.goroutine.count, go.gc.* ...).
	if err := runtime.Start(runtime.WithMeterProvider(m.meterProvider)); err != nil {
		return nil, fmt.Errorf("failed to start runtime metrics: %w", err)
	}

	return m, nil
}

func (m *Metrics) initializeInstruments() error {
	var err error

	// Cache operation counts
	m.CacheStoreCount, err = m.meter.Int64Counter(
		"cache.store.count",
		metric.WithDescription("Number of cache store operations"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.store.count: %w", err)
	}

	m.CacheLoadCount, err = m.meter.Int64Counter(
		"cache.load.count",
		metric.WithDescription("Number of cache load operations"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.load.count: %w", err)
	}

	m.CacheDeleteCount, err = m.meter.Int64Counter(
		"cache.delete.count",
		metric.WithDescription("Number of cache delete operations"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.delete.count: %w", err)
	}

	// Cache operation durations
	m.CacheStoreDuration, err = m.meter.Float64Histogram(
		"cache.store.duration",
		metric.WithDescription("Duration of cache store operations"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.store.duration: %w", err)
	}

	m.CacheLoadDuration, err = m.meter.Float64Histogram(
		"cache.load.duration",
		metric.WithDescription("Duration of cache load operations"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.load.duration: %w", err)
	}

	m.CacheDeleteDuration, err = m.meter.Float64Histogram(
		"cache.delete.duration",
		metric.WithDescription("Duration of cache delete operations"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.delete.duration: %w", err)
	}

	// Hit/miss
	m.CacheHitCount, err = m.meter.Int64Counter(
		"cache.hit.count",
		metric.WithDescription("Number of cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.hit.count: %w", err)
	}

	m.CacheMissCount, err = m.meter.Int64Counter(
		"cache.miss.count",
		metric.WithDescription("Number of cache misses"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.miss.count: %w", err)
	}

	// Eviction and expiration
	m.CacheEvictionCount, err = m.meter.Int64Counter(
		"cache.eviction.count",
		metric.WithDescription("Number of cache evictions"),
		metric.WithUnit("{eviction}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.eviction.count: %w", err)
	}

	m.CacheExpirationCount, err = m.meter.Int64Counter(
		"cache.expiration.count",
		metric.WithDescription("Number of expired entries removed"),
		metric.WithUnit("{expiration}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.expiration.count: %w", err)
	}

	// Persistence
	m.CacheSaveDuration, err = m.meter.Float64Histogram(
		"cache.save.duration",
		metric.WithDescription("Duration of cache save to disk"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.save.duration: %w", err)
	}

	m.CacheLoadFileDuration, err = m.meter.Float64Histogram(
		"cache.loadfile.duration",
		metric.WithDescription("Duration of cache load from disk"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.loadfile.duration: %w", err)
	}

	m.CacheSaveErrors, err = m.meter.Int64Counter(
		"cache.save.errors",
		metric.WithDescription("Number of cache save errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.save.errors: %w", err)
	}

	m.CacheLoadFileErrors, err = m.meter.Int64Counter(
		"cache.loadfile.errors",
		metric.WithDescription("Number of cache load errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.loadfile.errors: %w", err)
	}

	// Sweep
	m.CacheSweepDuration, err = m.meter.Float64Histogram(
		"cache.sweep.duration",
		metric.WithDescription("Duration of cache sweep operations"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.sweep.duration: %w", err)
	}

	m.CacheSweepExpired, err = m.meter.Int64Counter(
		"cache.sweep.expired",
		metric.WithDescription("Number of entries removed by sweep"),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.sweep.expired: %w", err)
	}

	return nil
}

// RegisterGauges registers observable gauges that poll cache state.
// memUsedFn and itemCountFn are callbacks that return current values.
func (m *Metrics) RegisterGauges(memUsedFn func() int64, itemCountFn func() int64) error {
	if m.meter == nil {
		return nil
	}
	var err error
	m.CacheMemoryUsed, err = m.meter.Int64ObservableGauge(
		"cache.memory.used",
		metric.WithDescription("Current cache memory usage in bytes"),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(memUsedFn())
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.memory.used: %w", err)
	}

	m.CacheItemCount, err = m.meter.Int64ObservableGauge(
		"cache.items.count",
		metric.WithDescription("Current number of items in cache"),
		metric.WithUnit("{item}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(itemCountFn())
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.items.count: %w", err)
	}

	return nil
}

// Shutdown shuts down the meter provider
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m.meterProvider != nil {
		return m.meterProvider.Shutdown(ctx)
	}
	return nil
}

// RecordStore records a cache store operation
func (m *Metrics) RecordStore(ctx context.Context, result string, durationMs float64) {
	if m.CacheStoreCount == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("result", result))
	m.CacheStoreCount.Add(ctx, 1, attrs)
	m.CacheStoreDuration.Record(ctx, durationMs, attrs)
}

// RecordLoad records a cache load operation
func (m *Metrics) RecordLoad(ctx context.Context, hit bool, durationMs float64) {
	if m.CacheLoadCount == nil {
		return
	}
	m.CacheLoadCount.Add(ctx, 1)
	m.CacheLoadDuration.Record(ctx, durationMs)
	if hit {
		m.CacheHitCount.Add(ctx, 1)
	} else {
		m.CacheMissCount.Add(ctx, 1)
	}
}

// RecordDelete records a cache delete operation
func (m *Metrics) RecordDelete(ctx context.Context, found bool, durationMs float64) {
	if m.CacheDeleteCount == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.Bool("found", found))
	m.CacheDeleteCount.Add(ctx, 1, attrs)
	m.CacheDeleteDuration.Record(ctx, durationMs, attrs)
}

// RecordEviction records cache evictions
func (m *Metrics) RecordEviction(ctx context.Context, count int64) {
	if m.CacheEvictionCount == nil {
		return
	}
	m.CacheEvictionCount.Add(ctx, count)
}

// RecordExpiration records expired entry removals
func (m *Metrics) RecordExpiration(ctx context.Context, count int64) {
	if m.CacheExpirationCount == nil {
		return
	}
	m.CacheExpirationCount.Add(ctx, count)
}

// RecordSave records a cache save-to-disk operation
func (m *Metrics) RecordSave(ctx context.Context, success bool, durationMs float64) {
	if m.CacheSaveDuration == nil {
		return
	}
	m.CacheSaveDuration.Record(ctx, durationMs)
	if !success {
		m.CacheSaveErrors.Add(ctx, 1)
	}
}

// RecordLoadFile records a cache load-from-disk operation
func (m *Metrics) RecordLoadFile(ctx context.Context, success bool, durationMs float64) {
	if m.CacheLoadFileDuration == nil {
		return
	}
	m.CacheLoadFileDuration.Record(ctx, durationMs)
	if !success {
		m.CacheLoadFileErrors.Add(ctx, 1)
	}
}

// RecordSweep records a sweep operation
func (m *Metrics) RecordSweep(ctx context.Context, expired int64, durationMs float64) {
	if m.CacheSweepDuration == nil {
		return
	}
	m.CacheSweepDuration.Record(ctx, durationMs)
	if expired > 0 {
		m.CacheSweepExpired.Add(ctx, expired)
	}
}
