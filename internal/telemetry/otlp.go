package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// OTLP endpoint rules, shared by the metric and trace exporters:
//
//   - Protocol is Config.Protocol, else OTEL_EXPORTER_OTLP_PROTOCOL, else grpc.
//     "http" and "http/protobuf" select OTLP/HTTP.
//   - An endpoint URL (https://host/prefix) is a base: OTLP/HTTP posts to
//     <base>/v1/metrics and <base>/v1/traces, and the scheme decides TLS.
//   - A host:port endpoint uses TLS unless Config.Insecure.
//   - With no endpoint the exporters use OTEL_EXPORTER_OTLP_* (whose URL scheme
//     decides TLS) or their default, localhost:4317 (grpc) / 4318 (http).
//   - Config.Headers, when set, replace OTEL_EXPORTER_OTLP_HEADERS.

func (cfg *Config) useHTTP() (bool, error) {
	p := cfg.Protocol
	if p == "" {
		p = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	switch p {
	case "", "grpc":
		return false, nil
	case "http", "http/protobuf":
		return true, nil
	}
	return false, fmt.Errorf("unsupported OTLP protocol %q (use grpc or http)", p)
}

// endpointIsURL reports whether OTLPEndpoint is a URL rather than host:port.
func (cfg *Config) endpointIsURL() bool {
	return strings.Contains(cfg.OTLPEndpoint, "://")
}

// insecureForDefault reports whether to force plaintext when no endpoint is
// given: only when no OTEL_EXPORTER_OTLP_ENDPOINT URL decides it instead.
func (cfg *Config) insecureForDefault() bool {
	return cfg.Insecure && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == ""
}

// signalURL joins a base endpoint URL and an OTLP/HTTP signal path.
func signalURL(base, signal string) string {
	return strings.TrimRight(base, "/") + "/v1/" + signal
}

func newOTLPMetricExporter(ctx context.Context, cfg *Config) (sdkmetric.Exporter, error) {
	useHTTP, err := cfg.useHTTP()
	if err != nil {
		return nil, err
	}
	if useHTTP {
		var opts []otlpmetrichttp.Option
		switch {
		case cfg.endpointIsURL():
			opts = append(opts, otlpmetrichttp.WithEndpointURL(signalURL(cfg.OTLPEndpoint, "metrics")))
		case cfg.OTLPEndpoint != "":
			opts = append(opts, otlpmetrichttp.WithEndpoint(cfg.OTLPEndpoint))
			if cfg.Insecure {
				opts = append(opts, otlpmetrichttp.WithInsecure())
			}
		case cfg.insecureForDefault():
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlpmetrichttp.WithHeaders(cfg.Headers))
		}
		return otlpmetrichttp.New(ctx, opts...)
	}

	var opts []otlpmetricgrpc.Option
	switch {
	case cfg.endpointIsURL():
		opts = append(opts, otlpmetricgrpc.WithEndpointURL(cfg.OTLPEndpoint))
	case cfg.OTLPEndpoint != "":
		opts = append(opts, otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint))
		if cfg.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
	case cfg.insecureForDefault():
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.Headers))
	}
	return otlpmetricgrpc.New(ctx, opts...)
}

func newOTLPTraceExporter(ctx context.Context, cfg *Config) (sdktrace.SpanExporter, error) {
	useHTTP, err := cfg.useHTTP()
	if err != nil {
		return nil, err
	}
	if useHTTP {
		var opts []otlptracehttp.Option
		switch {
		case cfg.endpointIsURL():
			opts = append(opts, otlptracehttp.WithEndpointURL(signalURL(cfg.OTLPEndpoint, "traces")))
		case cfg.OTLPEndpoint != "":
			opts = append(opts, otlptracehttp.WithEndpoint(cfg.OTLPEndpoint))
			if cfg.Insecure {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
		case cfg.insecureForDefault():
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
		}
		return otlptracehttp.New(ctx, opts...)
	}

	var opts []otlptracegrpc.Option
	switch {
	case cfg.endpointIsURL():
		opts = append(opts, otlptracegrpc.WithEndpointURL(cfg.OTLPEndpoint))
	case cfg.OTLPEndpoint != "":
		opts = append(opts, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint))
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
	case cfg.insecureForDefault():
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}
	return otlptracegrpc.New(ctx, opts...)
}

// ParseHeaders parses "key=value,key2=value2" (the OTEL_EXPORTER_OTLP_HEADERS
// format; values may be percent-encoded, e.g. Bearer%20token).
func ParseHeaders(s string) (map[string]string, error) {
	headers := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		if strings.TrimSpace(pair) == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid OTLP header %q: want key=value", pair)
		}
		headers[k] = strings.TrimSpace(strings.ReplaceAll(v, "%20", " "))
	}
	return headers, nil
}
