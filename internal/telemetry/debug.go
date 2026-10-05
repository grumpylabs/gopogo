package telemetry

import (
	"context"
	"sort"
	"strings"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// With Config.Debug, every export is logged with its item count, duration
// and result, so it is visible whether telemetry is reaching the collector.

type loggingMetricExporter struct {
	sdkmetric.Exporter
}

func (e loggingMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	start := time.Now()
	err := e.Exporter.Export(ctx, rm)
	n := 0
	for _, sm := range rm.ScopeMetrics {
		n += len(sm.Metrics)
	}
	reportExport(zap.L(), "metrics", n, start, err)
	return err
}

type loggingSpanExporter struct {
	sdktrace.SpanExporter
}

func (e loggingSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	start := time.Now()
	err := e.SpanExporter.ExportSpans(ctx, spans)
	reportExport(zap.L(), "spans", len(spans), start, err)
	return err
}

func reportExport(logger *zap.Logger, signal string, n int, start time.Time, err error) {
	fields := []zap.Field{zap.String("signal", signal), zap.Int("count", n), zap.Duration("took", time.Since(start).Round(time.Millisecond))}
	if err != nil {
		logger.Warn("telemetry export failed", append(fields, zap.Error(err))...)
		return
	}
	logger.Info("telemetry exported", fields...)
}

// exporterFor returns a signal's exporter: its own setting, else
// ExporterType.
func (cfg *Config) exporterFor(signal string) string {
	if signal != "" {
		return signal
	}
	return cfg.ExporterType
}

// Describe summarizes where telemetry goes, naming headers but never showing
// their values.
func (cfg *Config) Describe() string {
	if !cfg.Enabled {
		return "telemetry: disabled"
	}
	signals := "metrics " + cfg.exporterFor(cfg.MetricsExporter) +
		", traces " + cfg.exporterFor(cfg.TracesExporter) +
		", logs " + cfg.ExporterType
	if cfg.ExporterType != "otlp" && cfg.exporterFor(cfg.MetricsExporter) != "otlp" &&
		cfg.exporterFor(cfg.TracesExporter) != "otlp" {
		return "telemetry: " + signals
	}
	proto := "grpc"
	if useHTTP, err := cfg.useHTTP(); err == nil && useHTTP {
		proto = "http"
	}
	endpoint := cfg.OTLPEndpoint
	if endpoint == "" {
		endpoint = "(OTEL_EXPORTER_OTLP_ENDPOINT or default)"
	}
	var names []string
	for k := range cfg.Headers {
		names = append(names, k)
	}
	sort.Strings(names)
	headers := "none"
	if len(names) > 0 {
		headers = strings.Join(names, ",")
	}
	return "telemetry: " + signals + "; otlp/" + proto + " to " + endpoint + ", headers: " + headers
}
