package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// Logger exports gopogo's log output as OpenTelemetry log records, alongside
// the usual stderr output.
type Logger struct {
	provider *sdklog.LoggerProvider
}

// NewLogger creates an OpenTelemetry logger provider using the same exporter
// settings as metrics and traces. When telemetry is disabled it does nothing.
func NewLogger(ctx context.Context, cfg *Config) (*Logger, error) {
	l := &Logger{}
	if !cfg.Enabled {
		return l, nil
	}

	res, err := createResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var exporter sdklog.Exporter
	switch cfg.ExporterType {
	case "otlp":
		exporter, err = newOTLPLogExporter(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP log exporter: %w", err)
		}
	case "stdout":
		exporter, err = stdoutlog.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout log exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", cfg.ExporterType)
	}

	l.provider = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	global.SetLoggerProvider(l.provider)
	return l, nil
}

// Install makes the standard log package and slog's default logger write to
// stderr in the log package's usual format and, when telemetry is enabled,
// emit each entry as an OpenTelemetry log record.
//
// OpenTelemetry's own error reports (such as a failed export) go to stderr
// only; routing them through the log package would turn a failing log
// export into more log records to export.
func (l *Logger) Install(level slog.Level) {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		fmt.Fprintf(os.Stderr, "%s %v\n", time.Now().Format("2006/01/02 15:04:05"), err)
	}))
	handlers := []slog.Handler{&stderrHandler{w: os.Stderr}}
	if l.provider != nil {
		handlers = append(handlers, otelslog.NewHandler("github.com/grumpylabs/gopogo",
			otelslog.WithLoggerProvider(l.provider)))
	}
	slog.SetDefault(slog.New(levelHandler{min: level, Handler: fanoutHandler(handlers)}))
}

// levelHandler drops records below min.
type levelHandler struct {
	slog.Handler
	min slog.Level
}

func (h levelHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.min && h.Handler.Enabled(ctx, l)
}

func (h levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return levelHandler{min: h.min, Handler: h.Handler.WithAttrs(attrs)}
}

func (h levelHandler) WithGroup(name string) slog.Handler {
	return levelHandler{min: h.min, Handler: h.Handler.WithGroup(name)}
}

// Shutdown flushes pending log records and shuts down the provider.
func (l *Logger) Shutdown(ctx context.Context) error {
	if l.provider != nil {
		return l.provider.Shutdown(ctx)
	}
	return nil
}

func newOTLPLogExporter(ctx context.Context, cfg *Config) (sdklog.Exporter, error) {
	useHTTP, err := cfg.useHTTP()
	if err != nil {
		return nil, err
	}
	if useHTTP {
		var opts []otlploghttp.Option
		switch {
		case cfg.endpointIsURL():
			opts = append(opts, otlploghttp.WithEndpointURL(signalURL(cfg.OTLPEndpoint, "logs")))
		case cfg.OTLPEndpoint != "":
			opts = append(opts, otlploghttp.WithEndpoint(cfg.OTLPEndpoint))
			if cfg.Insecure {
				opts = append(opts, otlploghttp.WithInsecure())
			}
		case cfg.insecureForDefault():
			opts = append(opts, otlploghttp.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlploghttp.WithHeaders(cfg.Headers))
		}
		return otlploghttp.New(ctx, opts...)
	}

	var opts []otlploggrpc.Option
	switch {
	case cfg.endpointIsURL():
		opts = append(opts, otlploggrpc.WithEndpointURL(cfg.OTLPEndpoint))
	case cfg.OTLPEndpoint != "":
		opts = append(opts, otlploggrpc.WithEndpoint(cfg.OTLPEndpoint))
		if cfg.Insecure {
			opts = append(opts, otlploggrpc.WithInsecure())
		}
	case cfg.insecureForDefault():
		opts = append(opts, otlploggrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlploggrpc.WithHeaders(cfg.Headers))
	}
	return otlploggrpc.New(ctx, opts...)
}

// stderrHandler writes records as "2006/01/02 15:04:05 message key=value",
// matching the log package's default output.
type stderrHandler struct {
	mu    sync.Mutex
	w     io.Writer
	attrs []slog.Attr
}

func (h *stderrHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *stderrHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Time.Format("2006/01/02 15:04:05"))
	b.WriteByte(' ')
	if r.Level >= slog.LevelWarn || r.Level < slog.LevelInfo {
		b.WriteString(r.Level.String())
		b.WriteByte(' ')
	}
	b.WriteString(r.Message)
	write := func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	}
	for _, a := range h.attrs {
		write(a)
	}
	r.Attrs(write)
	b.WriteByte('\n')
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func (h *stderrHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &stderrHandler{w: h.w, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *stderrHandler) WithGroup(string) slog.Handler { return h }

// fanoutHandler sends each record to every handler.
type fanoutHandler []slog.Handler

func (f fanoutHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var first error
	for _, h := range f {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func (f fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(fanoutHandler, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(attrs)
	}
	return out
}

func (f fanoutHandler) WithGroup(name string) slog.Handler {
	out := make(fanoutHandler, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(name)
	}
	return out
}
