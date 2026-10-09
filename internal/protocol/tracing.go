package protocol

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Each command runs in a server span named after the command (unknown
// commands are named UNKNOWN, so client input cannot create unbounded span
// names), with database and network attributes from the OpenTelemetry
// semantic conventions. Keys and values are never recorded. HTTP requests
// continue a caller's trace from W3C traceparent headers; the other protocols
// have nowhere to carry trace context, so their spans start new traces.

const instrumentationName = "github.com/grumpylabs/gopogo/internal/protocol"

var tracingEnabled, commandLogs atomic.Bool

// EnableTracing turns on command spans. Spans go to the global tracer
// provider; until this is called no spans are created.
func EnableTracing() {
	tracingEnabled.Store(true)
}

// EnableCommandLogs writes a debug log record for every command, when the
// global logger has debug enabled.
func EnableCommandLogs() {
	commandLogs.Store(true)
}

// commandObs follows one command: its span, when tracing is on, and a debug
// log record when debug logging is on and the command is sampled.
type commandObs struct {
	ctx   context.Context
	span  trace.Span
	start time.Time
	proto Type
	addr  string
	name  string
}

// commandDuration and httpDuration are the gopogo.command.duration and
// http.server.request.duration histograms, set by EnableCommandMetrics.
var (
	commandDuration atomic.Pointer[metric.Float64Histogram]
	httpDuration    atomic.Pointer[metric.Float64Histogram]
)

// commandDurationBuckets suit a cache, whose commands mostly take
// microseconds: 10us to 1s.
var commandDurationBuckets = []float64{
	0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
}

// EnableCommandMetrics records every command's duration in the
// gopogo.command.duration histogram, with the same attributes as its span,
// and every HTTP request's in http.server.request.duration.
func EnableCommandMetrics(mp metric.MeterProvider) error {
	meter := mp.Meter(instrumentationName)
	h, err := meter.Float64Histogram("gopogo.command.duration",
		metric.WithDescription("Duration of commands handled by the server"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(commandDurationBuckets...))
	if err != nil {
		return err
	}
	hh, err := meter.Float64Histogram("http.server.request.duration",
		metric.WithDescription("Duration of HTTP server requests"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(commandDurationBuckets...))
	if err != nil {
		return err
	}
	commandDuration.Store(&h)
	httpDuration.Store(&hh)
	return nil
}

// metricAttrKey identifies one combination of command metric attributes.
type metricAttrKey struct {
	proto   Type
	name    string
	errType string
}

// metricAttrs caches each combination's record options, so recording a
// command builds and sorts no attribute set. Lookups take no lock; a new
// combination copies the map. Names are normalized and error types come
// from the server's own replies, so there are a few hundred at most;
// maxMetricAttrs bounds the cache regardless.
var metricAttrs atomic.Pointer[map[metricAttrKey][]metric.RecordOption]

const maxMetricAttrs = 4096

func commandMetricAttrs(proto Type, name, errType string) []metric.RecordOption {
	key := metricAttrKey{proto, name, errType}
	cached := metricAttrs.Load()
	if cached != nil {
		if opts, ok := (*cached)[key]; ok {
			return opts
		}
	}
	attrs := []attribute.KeyValue{
		semconv.DBSystemNameKey.String(dbSystem),
		semconv.DBOperationName(name),
		semconv.NetworkProtocolName(proto.String()),
	}
	if errType != "" {
		attrs = append(attrs, semconv.ErrorTypeKey.String(errType))
	}
	opts := []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attrs...))}
	if cached != nil && len(*cached) >= maxMetricAttrs {
		return opts
	}
	// A racing writer may drop this entry or another's; either is rebuilt
	// on its next use.
	next := make(map[metricAttrKey][]metric.RecordOption, 1)
	if cached != nil {
		next = make(map[metricAttrKey][]metric.RecordOption, len(*cached)+1)
		for k, v := range *cached {
			next[k] = v
		}
	}
	next[key] = opts
	metricAttrs.Store(&next)
	return opts
}

// beginCommand starts observing a command. name must already be normalized.
// With no spans, metrics or command logs it records nothing, not even the
// time.
func beginCommand(ctx context.Context, proto Type, addr, name string) commandObs {
	if !tracingEnabled.Load() && commandDuration.Load() == nil && !commandLogs.Load() {
		return commandObs{}
	}
	ctx, span := startCommandSpan(ctx, proto, addr, name)
	return commandObs{ctx: ctx, span: span, start: time.Now(), proto: proto, addr: addr, name: name}
}

// end records the command's outcome. errMsg is the error reply, or "".
func (o commandObs) end(errMsg string) {
	if o.start.IsZero() {
		return
	}
	elapsed := time.Since(o.start)
	endCommandSpan(o.span, errMsg)
	if h := commandDuration.Load(); h != nil {
		errType := ""
		if errMsg != "" {
			errType = errorType(errMsg)
		}
		// o.ctx carries the command's span, so the SDK can attach it as an
		// exemplar.
		(*h).Record(o.ctx, elapsed.Seconds(), commandMetricAttrs(o.proto, o.name, errType)...)
	}
	if !commandLogs.Load() {
		return
	}
	ce := zap.L().Check(zapcore.DebugLevel, "command")
	if ce == nil {
		return
	}
	took := elapsed.Microseconds()
	host, port := splitClient(o.addr)
	// Field names follow the OpenTelemetry semantic conventions, matching
	// the command's span.
	fields := []zap.Field{
		zap.String(string(semconv.DBSystemNameKey), dbSystem),
		zap.String(string(semconv.DBOperationNameKey), o.name),
		zap.String(string(semconv.NetworkProtocolNameKey), o.proto.String()),
		zap.Int64("duration_us", took),
		// Links the record to the command's trace.
		logContext(o.ctx),
	}
	if host != "" {
		fields = append(fields, zap.String(string(semconv.ClientAddressKey), host))
	}
	if port != 0 {
		fields = append(fields, zap.Int(string(semconv.ClientPortKey), port))
	}
	from := ""
	if host != "" {
		from = " from " + host
	}
	if errMsg != "" {
		ce.Message = fmt.Sprintf("%s %s failed in %dus%s: %s", o.proto, o.name, took, from, errMsg)
		fields = append(fields,
			zap.String(string(semconv.ErrorTypeKey), errorType(errMsg)),
			zap.String(string(semconv.ExceptionMessageKey), errMsg))
	} else {
		ce.Message = fmt.Sprintf("%s %s ok in %dus%s", o.proto, o.name, took, from)
	}
	ce.Write(fields...)
}

// dbSystem is the db.system.name of gopogo's spans and logs.
const dbSystem = "gopogo"

// splitClient splits a client's host:port; a Unix socket client has neither.
func splitClient(addr string) (host string, port int) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0
	}
	port, _ = strconv.Atoi(p)
	return h, port
}

// errorType is an error reply's code, e.g. ERR or WRONGTYPE.
func errorType(errMsg string) string {
	if i := strings.IndexByte(errMsg, ' '); i > 0 {
		return errMsg[:i]
	}
	return errMsg
}

// logContext carries ctx to the log cores without encoding it; they take
// the trace and span from it.
func logContext(ctx context.Context) zap.Field {
	return zap.Field{Key: "context", Type: zapcore.SkipType, Interface: ctx}
}

// startCommandSpan starts the span for one command, or returns ctx and nil
// when tracing is off. name must already be normalized.
func startCommandSpan(ctx context.Context, proto Type, addr, name string) (context.Context, trace.Span) {
	if !tracingEnabled.Load() {
		return ctx, nil
	}
	attrs := []attribute.KeyValue{
		semconv.DBSystemNameKey.String(dbSystem),
		semconv.DBOperationName(name),
		semconv.NetworkProtocolName(proto.String()),
	}
	if host, port := splitClient(addr); host != "" {
		attrs = append(attrs, semconv.ClientAddress(host), semconv.NetworkTransportTCP)
		if port != 0 {
			attrs = append(attrs, semconv.ClientPort(port))
		}
	} else {
		attrs = append(attrs, semconv.NetworkTransportUnix)
	}
	// Over HTTP the request has its own server span; the command is a step
	// inside it.
	kind := trace.SpanKindServer
	if proto == TypeHTTP {
		kind = trace.SpanKindInternal
	}
	return otel.Tracer(instrumentationName).Start(ctx, name,
		trace.WithSpanKind(kind),
		trace.WithAttributes(attrs...))
}

// endCommandSpan records the command's outcome and ends the span. errMsg is
// the error reply ("" for success); its first word (e.g. ERR, WRONGPASS)
// becomes error.type.
func endCommandSpan(span trace.Span, errMsg string) {
	if span == nil {
		return
	}
	if errMsg != "" {
		span.SetAttributes(semconv.ErrorTypeKey.String(errorType(errMsg)))
		span.SetStatus(codes.Error, errMsg)
	}
	span.End()
}
