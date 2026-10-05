package protocol

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
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
