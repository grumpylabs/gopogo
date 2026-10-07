// Package telemetry provides OpenTelemetry metrics and tracing instrumentation
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Config holds telemetry configuration
type Config struct {
	Enabled         bool
	ExporterType    string            // "otlp", "stdout"
	MetricsExporter string            // "otlp", "stdout" or "none"; empty uses ExporterType
	TracesExporter  string            // "otlp", "stdout" or "none"; empty uses ExporterType
	Protocol        string            // "grpc" or "http"; empty uses OTEL_EXPORTER_OTLP_PROTOCOL, else grpc
	OTLPEndpoint    string            // host:port or base URL; empty uses OTEL_EXPORTER_OTLP_*
	Insecure        bool              // plaintext OTLP to a host:port endpoint
	Headers         map[string]string // extra OTLP request headers, e.g. Authorization
	ServiceName     string
	ServiceVersion  string
	Environment     string
	SampleRatio     float64 // fraction of new traces to sample, 0-1
	SampleRatioSet  bool    // SampleRatio was given explicitly; else OTEL_TRACES_SAMPLER may choose
	Debug           bool    // log every export
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
	CacheEvictionCount   metric.Int64Counter
	CacheExpirationCount metric.Int64ObservableCounter

	// Persistence
	CacheSaveDuration     metric.Float64Histogram
	CacheLoadFileDuration metric.Float64Histogram
	CacheSaveErrors       metric.Int64Counter
	CacheLoadFileErrors   metric.Int64Counter

	// Sweep
	CacheSweepDuration metric.Float64Histogram
	CacheSweepExpired  metric.Int64Counter
}

// createResource creates a resource with service information
func createResource(cfg *Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.ServiceInstanceID(instanceID()),
	}
	if cfg.Environment != "" {
		attrs = append(attrs,
			semconv.DeploymentEnvironmentName(cfg.Environment),
			// The key before deployment.environment.name; some backends
			// still read it.
			attribute.String("deployment.environment", cfg.Environment))
	}
	// OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME override the defaults.
	res, err := resource.New(context.Background(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithOS(),
		resource.WithProcessPID(),
		resource.WithProcessExecutableName(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithContainer(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
	// A detector that finds nothing (e.g. no container ID outside a
	// container) leaves a partial resource, which is still usable.
	if errors.Is(err, resource.ErrPartialResource) {
		err = nil
	}
	return res, err
}

// instanceID is the default service.instance.id: the host name, which is
// the pod name in Kubernetes, else a random ID.
func instanceID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Duration histograms are in seconds. opBuckets suit single cache
// operations, which mostly take microseconds (10us to 1s); longBuckets
// suit saving, loading and sweeping the whole cache (1ms to 60s).
var (
	opBuckets = []float64{
		0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
	}
	longBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
	}
)

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

	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	var exporter sdkmetric.Exporter
	switch kind := cfg.exporterFor(cfg.MetricsExporter); kind {
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
	case "none":
	default:
		return nil, fmt.Errorf("unsupported metrics exporter: %s", kind)
	}
	if exporter != nil {
		if cfg.Debug {
			exporter = loggingMetricExporter{exporter}
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(30*time.Second),
		)))
	}

	m.meterProvider = sdkmetric.NewMeterProvider(opts...)

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
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(opBuckets...),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.store.duration: %w", err)
	}

	m.CacheLoadDuration, err = m.meter.Float64Histogram(
		"cache.load.duration",
		metric.WithDescription("Duration of cache load operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(opBuckets...),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.load.duration: %w", err)
	}

	m.CacheDeleteDuration, err = m.meter.Float64Histogram(
		"cache.delete.duration",
		metric.WithDescription("Duration of cache delete operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(opBuckets...),
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

	// Persistence
	m.CacheSaveDuration, err = m.meter.Float64Histogram(
		"cache.save.duration",
		metric.WithDescription("Duration of cache save to disk"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(longBuckets...),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.save.duration: %w", err)
	}

	m.CacheLoadFileDuration, err = m.meter.Float64Histogram(
		"cache.loadfile.duration",
		metric.WithDescription("Duration of cache load from disk"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(longBuckets...),
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
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(longBuckets...),
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

// RegisterGauges registers instruments that poll cache state: memory used
// and item count (gauges), and expired entries removed (a counter).
func (m *Metrics) RegisterGauges(memUsedFn, itemCountFn, expiredFn func() int64) error {
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

	// Read from the cache's own counter, which every expiry path updates
	// (access, delete and both sweeps), as STATS num_expired does.
	m.CacheExpirationCount, err = m.meter.Int64ObservableCounter(
		"cache.expiration.count",
		metric.WithDescription("Number of expired entries removed"),
		metric.WithUnit("{expiration}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(expiredFn())
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create cache.expiration.count: %w", err)
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
func (m *Metrics) RecordStore(ctx context.Context, result string, elapsed time.Duration) {
	if m.CacheStoreCount == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("result", result))
	m.CacheStoreCount.Add(ctx, 1, attrs)
	m.CacheStoreDuration.Record(ctx, elapsed.Seconds(), attrs)
}

// RecordLoad records a cache load operation
func (m *Metrics) RecordLoad(ctx context.Context, hit bool, elapsed time.Duration) {
	if m.CacheLoadCount == nil {
		return
	}
	m.CacheLoadCount.Add(ctx, 1)
	m.CacheLoadDuration.Record(ctx, elapsed.Seconds())
	if hit {
		m.CacheHitCount.Add(ctx, 1)
	} else {
		m.CacheMissCount.Add(ctx, 1)
	}
}

// RecordDelete records a cache delete operation
func (m *Metrics) RecordDelete(ctx context.Context, found bool, elapsed time.Duration) {
	if m.CacheDeleteCount == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.Bool("found", found))
	m.CacheDeleteCount.Add(ctx, 1, attrs)
	m.CacheDeleteDuration.Record(ctx, elapsed.Seconds(), attrs)
}

// RecordEviction records cache evictions
func (m *Metrics) RecordEviction(ctx context.Context, count int64) {
	if m.CacheEvictionCount == nil {
		return
	}
	m.CacheEvictionCount.Add(ctx, count)
}

// RecordSave records a cache save-to-disk operation
func (m *Metrics) RecordSave(ctx context.Context, success bool, elapsed time.Duration) {
	if m.CacheSaveDuration == nil {
		return
	}
	m.CacheSaveDuration.Record(ctx, elapsed.Seconds())
	if !success {
		m.CacheSaveErrors.Add(ctx, 1)
	}
}

// RecordLoadFile records a cache load-from-disk operation
func (m *Metrics) RecordLoadFile(ctx context.Context, success bool, elapsed time.Duration) {
	if m.CacheLoadFileDuration == nil {
		return
	}
	m.CacheLoadFileDuration.Record(ctx, elapsed.Seconds())
	if !success {
		m.CacheLoadFileErrors.Add(ctx, 1)
	}
}

// RecordSweep records a sweep operation
func (m *Metrics) RecordSweep(ctx context.Context, expired int64, elapsed time.Duration) {
	if m.CacheSweepDuration == nil {
		return
	}
	m.CacheSweepDuration.Record(ctx, elapsed.Seconds())
	if expired > 0 {
		m.CacheSweepExpired.Add(ctx, expired)
	}
}
