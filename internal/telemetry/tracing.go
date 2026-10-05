package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Tracer wraps the OpenTelemetry tracer provider.
type Tracer struct {
	provider *sdktrace.TracerProvider
}

// NewTracer creates and initializes OpenTelemetry tracing and registers it as
// the global tracer provider, with W3C trace context and baggage propagation.
// When telemetry is disabled it installs a no-op provider.
func NewTracer(ctx context.Context, cfg *Config) (*Tracer, error) {
	t := &Tracer{}

	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return t, nil
	}

	res, err := createResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	switch cfg.ExporterType {
	case "otlp":
		exporter, err = newOTLPTraceExporter(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
	case "stdout":
		exporter, err = stdouttrace.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout trace exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", cfg.ExporterType)
	}

	// Respect the caller's sampling decision; sample new traces at the
	// configured ratio, since a cache can serve many commands per second.
	ratio := cfg.SampleRatio
	if ratio < 0 || ratio > 1 {
		return nil, fmt.Errorf("trace sample ratio %v is not between 0 and 1", ratio)
	}
	t.provider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)

	otel.SetTracerProvider(t.provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return t, nil
}

// Shutdown flushes pending spans and shuts down the tracer provider.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.provider != nil {
		return t.provider.Shutdown(ctx)
	}
	return nil
}
