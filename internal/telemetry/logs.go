package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger exports gopogo's log output as OpenTelemetry log records, alongside
// JSON lines on stderr.
type Logger struct {
	provider *sdklog.LoggerProvider
}

// stderrLogger writes to stderr only. Telemetry's own reports use it, so a
// failing or busy log export never produces more log records to export.
var stderrLogger atomic.Pointer[zap.Logger]

func init() { stderrLogger.Store(zap.NewNop()) }

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
	if cfg.Debug {
		exporter = loggingLogExporter{exporter}
	}

	l.provider = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	global.SetLoggerProvider(l.provider)
	return l, nil
}

// Install builds the process logger: JSON lines on stderr and, when
// telemetry is enabled, an OpenTelemetry log record per entry whose body is
// the same JSON line, so a backend that shows only the body still shows
// every field. It becomes zap's global logger, and the standard log package
// writes through it at info level. Entries below level are dropped.
func (l *Logger) Install(level zapcore.Level) *zap.Logger {
	enabled := zap.NewAtomicLevelAt(level)
	enc := zapcore.NewJSONEncoder(encoderConfig())
	stderr := zapcore.NewCore(enc, stderrSyncer(), enabled)
	stderrLogger.Store(zap.New(&traceCore{stderr}, zap.AddCaller()))

	core := stderr
	if l.provider != nil {
		core = zapcore.NewTee(stderr, &otelCore{
			LevelEnabler: enabled,
			enc:          enc.Clone(),
			logger:       l.provider.Logger("github.com/grumpylabs/gopogo"),
		})
	}
	logger := zap.New(&traceCore{core}, zap.AddCaller())
	zap.ReplaceGlobals(logger)
	zap.RedirectStdLog(logger)

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		stderrLogger.Load().Error("telemetry error", zap.Error(err))
	}))
	return logger
}

// stderrSyncer buffers stderr: with debug logging every command is an
// entry, and a write per entry serializes all connections on one lock and
// one syscall. Entries reach stderr within a second, at once for panics and
// fatal errors, and on Sync at shutdown.
func stderrSyncer() zapcore.WriteSyncer {
	return &zapcore.BufferedWriteSyncer{
		WS:            zapcore.AddSync(os.Stderr),
		Size:          256 << 10,
		FlushInterval: time.Second,
	}
}

func encoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "time"
	cfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder
	return cfg
}

// Shutdown flushes pending log records and shuts down the provider.
func (l *Logger) Shutdown(ctx context.Context) error {
	if l.provider != nil {
		return l.provider.Shutdown(ctx)
	}
	return nil
}

// otelCore emits each entry as an OpenTelemetry log record with the
// entry's JSON encoding as its body, its fields as attributes, and its
// caller as code.* attributes. A Context field supplies the record's trace
// and span.
type otelCore struct {
	zapcore.LevelEnabler
	enc    zapcore.Encoder
	with   []zapcore.Field
	logger otellog.Logger
}

func (c *otelCore) With(fields []zapcore.Field) zapcore.Core {
	enc := c.enc.Clone()
	for _, f := range fields {
		f.AddTo(enc)
	}
	return &otelCore{
		LevelEnabler: c.LevelEnabler,
		enc:          enc,
		with:         append(c.with[:len(c.with):len(c.with)], fields...),
		logger:       c.logger,
	}
}

func (c *otelCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *otelCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	buf, err := c.enc.EncodeEntry(e, fields)
	if err != nil {
		return err
	}
	body := strings.TrimSuffix(buf.String(), "\n")
	buf.Free()

	// Strings and integers, nearly every field, convert directly; others
	// go through zap's map encoder. Debug logging writes an entry per
	// command, so this avoids a map and its entries per entry.
	ctx := context.Background()
	kvs := make([]otellog.KeyValue, 0, len(c.with)+len(fields)+3)
	var other *zapcore.MapObjectEncoder
	for _, list := range [2][]zapcore.Field{c.with, fields} {
		for _, f := range list {
			if fc, ok := f.Interface.(context.Context); ok {
				ctx = fc
				continue
			}
			if kv, ok := simpleAttr(f); ok {
				kvs = append(kvs, kv)
				continue
			}
			if other == nil {
				other = zapcore.NewMapObjectEncoder()
			}
			f.AddTo(other)
		}
	}
	if other != nil {
		for k, v := range other.Fields {
			kvs = append(kvs, otellog.KeyValue{Key: k, Value: logValue(v)})
		}
	}
	if e.Caller.Defined {
		kvs = append(kvs,
			otellog.String(string(semconv.CodeFilePathKey), e.Caller.File),
			otellog.Int(string(semconv.CodeLineNumberKey), e.Caller.Line),
			otellog.String(string(semconv.CodeFunctionNameKey), e.Caller.Function))
	}
	var r otellog.Record
	r.SetTimestamp(e.Time)
	r.SetSeverity(severity(e.Level))
	r.SetSeverityText(e.Level.CapitalString())
	r.SetBody(otellog.StringValue(body))
	r.AddAttributes(kvs...)
	c.logger.Emit(ctx, r)
	return nil
}

// simpleAttr converts a string, integer or bool field, the types that
// encode the same in zap's map encoder.
func simpleAttr(f zapcore.Field) (otellog.KeyValue, bool) {
	switch f.Type {
	case zapcore.StringType:
		return otellog.String(f.Key, f.String), true
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type:
		return otellog.Int64(f.Key, f.Integer), true
	case zapcore.BoolType:
		return otellog.Bool(f.Key, f.Integer == 1), true
	}
	return otellog.KeyValue{}, false
}

// logValue converts a value from zap's map encoder.
func logValue(v any) otellog.Value {
	switch v := v.(type) {
	case string:
		return otellog.StringValue(v)
	case bool:
		return otellog.BoolValue(v)
	case int:
		return otellog.IntValue(v)
	case int64:
		return otellog.Int64Value(v)
	case int32:
		return otellog.Int64Value(int64(v))
	case uint64:
		return otellog.Int64Value(int64(v))
	case uint32:
		return otellog.Int64Value(int64(v))
	case float64:
		return otellog.Float64Value(v)
	case float32:
		return otellog.Float64Value(float64(v))
	case time.Duration:
		return otellog.StringValue(v.String())
	case time.Time:
		return otellog.StringValue(v.Format(time.RFC3339Nano))
	case []any:
		vals := make([]otellog.Value, len(v))
		for i, x := range v {
			vals[i] = logValue(x)
		}
		return otellog.SliceValue(vals...)
	case map[string]any:
		kvs := make([]otellog.KeyValue, 0, len(v))
		for k, x := range v {
			kvs = append(kvs, otellog.KeyValue{Key: k, Value: logValue(x)})
		}
		return otellog.MapValue(kvs...)
	default:
		return otellog.StringValue(fmt.Sprint(v))
	}
}

func (*otelCore) Sync() error { return nil }

func severity(l zapcore.Level) otellog.Severity {
	switch {
	case l <= zapcore.DebugLevel:
		return otellog.SeverityDebug
	case l == zapcore.InfoLevel:
		return otellog.SeverityInfo
	case l == zapcore.WarnLevel:
		return otellog.SeverityWarn
	case l == zapcore.ErrorLevel:
		return otellog.SeverityError
	default:
		return otellog.SeverityFatal
	}
}

// traceCore adds trace_id and span_id fields for an entry logged with a
// Context field whose context holds a valid span.
type traceCore struct {
	zapcore.Core
}

func (c *traceCore) With(fields []zapcore.Field) zapcore.Core {
	return &traceCore{c.Core.With(fields)}
}

func (c *traceCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *traceCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	for _, f := range fields {
		ctx, ok := f.Interface.(context.Context)
		if !ok {
			continue
		}
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			fields = append(fields[:len(fields):len(fields)],
				zap.String("trace_id", sc.TraceID().String()),
				zap.String("span_id", sc.SpanID().String()))
		}
		break
	}
	return c.Core.Write(e, fields)
}

// loggingLogExporter reports each log export on stderr only; reporting it
// through the exported logger would itself produce a record per export.
type loggingLogExporter struct {
	sdklog.Exporter
}

func (e loggingLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	start := time.Now()
	err := e.Exporter.Export(ctx, records)
	reportExport(stderrLogger.Load(), "logs", len(records), start, err)
	return err
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
