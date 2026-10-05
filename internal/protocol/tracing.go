package protocol

import (
	"context"
	"math"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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

var tracingEnabled atomic.Bool

// EnableTracing turns on command spans. Spans go to the global tracer
// provider; until this is called no spans are created.
func EnableTracing() {
	tracingEnabled.Store(true)
}

// debugLogSample is the fraction of commands logged at debug level, stored
// as float64 bits.
var debugLogSample atomic.Uint64

func init() { SetDebugLogSample(1) }

// SetDebugLogSample sets the fraction (0-1) of commands logged at debug
// level when the log level is debug.
func SetDebugLogSample(ratio float64) {
	debugLogSample.Store(math.Float64bits(min(max(ratio, 0), 1)))
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

// beginCommand starts observing a command. name must already be normalized.
func beginCommand(ctx context.Context, proto Type, addr, name string) commandObs {
	ctx, span := startCommandSpan(ctx, proto, addr, name)
	return commandObs{ctx: ctx, span: span, start: time.Now(), proto: proto, addr: addr, name: name}
}

// end records the command's outcome. errMsg is the error reply, or "".
func (o commandObs) end(errMsg string) {
	endCommandSpan(o.span, errMsg)
	ce := zap.L().Check(zapcore.DebugLevel, "command")
	if ce == nil {
		return
	}
	if r := math.Float64frombits(debugLogSample.Load()); r < 1 && rand.Float64() >= r {
		return
	}
	fields := []zap.Field{
		zap.String("command", o.name),
		zap.String("protocol", o.proto.String()),
		zap.String("client", o.addr),
		zap.Int64("duration_us", time.Since(o.start).Microseconds()),
		// Links the record to the command's trace.
		logContext(o.ctx),
	}
	if errMsg != "" {
		fields = append(fields, zap.String("error", errMsg))
	}
	ce.Write(fields...)
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
		attribute.String("db.system.name", "gopogo"),
		attribute.String("db.operation.name", name),
		semconv.NetworkProtocolName(proto.String()),
	}
	if host, port, err := net.SplitHostPort(addr); err == nil {
		attrs = append(attrs, semconv.ClientAddress(host))
		if p, err := strconv.Atoi(port); err == nil {
			attrs = append(attrs, semconv.ClientPort(p))
		}
	}
	return otel.Tracer(instrumentationName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindServer),
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
		errType := errMsg
		if i := strings.IndexByte(errMsg, ' '); i > 0 {
			errType = errMsg[:i]
		}
		span.SetAttributes(attribute.String("error.type", errType))
		span.SetStatus(codes.Error, errMsg)
	}
	span.End()
}
