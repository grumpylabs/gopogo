package protocol

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	EnableTracing()
	t.Cleanup(func() {
		tracingEnabled.Store(false)
		otel.SetTracerProvider(noop.NewTracerProvider())
	})
	return rec
}

func spanAttr(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

func TestCommandSpans(t *testing.T) {
	rec := recordSpans(t)
	ch := cache.New(nil)

	c := newRESPClient(t, ch)
	c.expect("OK", "SET", "secret-key", "secret-value")
	c.do("BOGUS", "x")
	c.do("INCR", "secret-key") // not an integer: error span

	server, client := net.Pipe()
	go NewMemcacheHandler(ch, "").Handle(server)
	defer client.Close()
	r := bufio.NewReader(client)
	client.SetDeadline(time.Now().Add(5 * time.Second))
	client.Write([]byte("get secret-key\r\n"))
	for l, _ := r.ReadString('\n'); l != "END\r\n"; l, _ = r.ReadString('\n') {
	}
	client.Write([]byte("incr nope x\r\n"))
	r.ReadString('\n')

	// The server ends a span after its reply is read; wait for the last one.
	deadline := time.Now().Add(2 * time.Second)
	for len(rec.Ended()) < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	spans := rec.Ended()
	if len(spans) != 5 {
		t.Fatalf("got %d spans, want 5", len(spans))
	}
	want := []struct {
		name, proto string
		failed      bool
	}{
		{"SET", "redis", false}, {"UNKNOWN", "redis", true}, {"INCR", "redis", true},
		{"get", "memcache", false}, {"incr", "memcache", true},
	}
	for i, w := range want {
		s := spans[i]
		if s.Name() != w.name || s.SpanKind() != trace.SpanKindServer {
			t.Errorf("span %d: %s/%v, want %s/server", i, s.Name(), s.SpanKind(), w.name)
		}
		if got := spanAttr(s, "network.protocol.name"); got != w.proto {
			t.Errorf("span %d protocol %q, want %q", i, got, w.proto)
		}
		if got := spanAttr(s, "db.operation.name"); got != w.name {
			t.Errorf("span %d db.operation.name %q", i, got)
		}
		if failed := s.Status().Code == codes.Error; failed != w.failed {
			t.Errorf("span %d %s: error status %v, want %v", i, s.Name(), failed, w.failed)
		}
		for _, kv := range s.Attributes() {
			if strings.Contains(kv.Value.Emit(), "secret") {
				t.Errorf("span %d records a key or value: %s=%s", i, kv.Key, kv.Value.Emit())
			}
		}
	}
	if got := spanAttr(spans[1], "error.type"); got != "ERR" {
		t.Errorf("error.type %q, want ERR", got)
	}
	if got := spanAttr(spans[4], "error.type"); got != "CLIENT_ERROR" {
		t.Errorf("memcache error.type %q, want CLIENT_ERROR", got)
	}
}

func TestHTTPSpanContinuesTrace(t *testing.T) {
	rec := recordSpans(t)
	server, client := net.Pipe()
	go NewHTTPHandler(cache.New(nil), "").Handle(server)
	defer client.Close()

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	req, _ := http.NewRequest("GET", "http://gopogo/k", nil)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	client.SetDeadline(time.Now().Add(5 * time.Second))
	req.Write(client)
	resp, err := http.ReadResponse(bufio.NewReader(client), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for len(rec.Ended()) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "GET" {
		t.Fatalf("spans: %v", spans)
	}
	if got := spans[0].SpanContext().TraceID().String(); got != traceID {
		t.Fatalf("trace id %s, want the caller's %s", got, traceID)
	}
	if got := spans[0].Parent().SpanID().String(); got != "00f067aa0ba902b7" {
		t.Fatalf("parent span %s, want the caller's", got)
	}
}

func TestCommandLogs(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core)))

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		beginCommand(ctx, TypeRedis, "10.0.0.1:5000", "GET").end("")
	}
	beginCommand(ctx, TypeRedis, "10.0.0.1:5000", "INCR").end("ERR value is not an integer or out of range")

	if logs.Len() != 51 {
		t.Fatalf("got %d records, want one per command", logs.Len())
	}
	ok := logs.All()[0]
	if !strings.HasPrefix(ok.Message, "redis GET ok in ") || !strings.HasSuffix(ok.Message, "us from 10.0.0.1") {
		t.Errorf("message %q", ok.Message)
	}
	failed := logs.All()[50]
	if !strings.HasPrefix(failed.Message, "redis INCR failed in ") ||
		!strings.HasSuffix(failed.Message, "from 10.0.0.1: ERR value is not an integer or out of range") {
		t.Errorf("message %q", failed.Message)
	}
	if got := failed.ContextMap()["error.type"]; got != "ERR" {
		t.Errorf("error.type = %v", got)
	}
}
