package protocol

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
	for len(rec.Ended()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("spans: %v", spans)
	}
	// The command ends first, inside the request's server span.
	cmd, req0 := spans[0], spans[1]
	if req0.Name() != "GET /{key}" || req0.SpanKind() != trace.SpanKindServer {
		t.Fatalf("request span %q kind %v", req0.Name(), req0.SpanKind())
	}
	if got := req0.SpanContext().TraceID().String(); got != traceID {
		t.Fatalf("trace id %s, want the caller's %s", got, traceID)
	}
	if got := req0.Parent().SpanID().String(); got != "00f067aa0ba902b7" {
		t.Fatalf("parent span %s, want the caller's", got)
	}
	for k, want := range map[string]string{
		"http.request.method": "GET", "http.route": "/{key}", "url.scheme": "http",
		"network.protocol.version": "1.1", "http.response.status_code": "404",
	} {
		if got := spanAttr(req0, k); got != want {
			t.Errorf("request span %s = %q, want %q", k, got, want)
		}
	}
	if spanAttr(req0, "url.path") != "" {
		t.Error("request span records url.path, which holds the key")
	}
	if cmd.Name() != "GET" || cmd.SpanKind() != trace.SpanKindInternal || cmd.Parent().SpanID() != req0.SpanContext().SpanID() {
		t.Fatalf("command span %q kind %v parent %v", cmd.Name(), cmd.SpanKind(), cmd.Parent().SpanID())
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

func TestCommandDurationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	if err := EnableCommandMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { commandDuration.Store(nil); httpDuration.Store(nil) })

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		beginCommand(ctx, TypeRedis, "10.0.0.1:5000", "GET").end("")
	}
	beginCommand(ctx, TypeMemcache, "10.0.0.1:5000", "set").end("")
	beginCommand(ctx, TypeRedis, "10.0.0.1:5000", "INCR").end("ERR value is not an integer or out of range")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	var hist metricdata.Histogram[float64]
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name == "gopogo.command.duration" {
				if md.Unit != "s" {
					t.Errorf("unit %q", md.Unit)
				}
				hist, found = md.Data.(metricdata.Histogram[float64])
			}
		}
	}
	if !found {
		t.Fatal("gopogo.command.duration not reported")
	}
	counts := map[string]uint64{}
	for _, dp := range hist.DataPoints {
		counts[dp.Attributes.Encoded(attribute.DefaultEncoder())] = dp.Count
		if len(dp.Bounds) != len(commandDurationBuckets) || dp.Bounds[0] != 0.00001 {
			t.Errorf("bounds %v", dp.Bounds)
		}
	}
	want := map[string]uint64{
		"db.operation.name=GET,db.system.name=gopogo,network.protocol.name=redis":                 3,
		"db.operation.name=set,db.system.name=gopogo,network.protocol.name=memcache":              1,
		"db.operation.name=INCR,db.system.name=gopogo,error.type=ERR,network.protocol.name=redis": 1,
	}
	for k, n := range want {
		if counts[k] != n {
			t.Errorf("%s: count %d, want %d (all: %v)", k, counts[k], n, counts)
		}
	}
}

func TestHTTPServerMetricAndErrors(t *testing.T) {
	rec := recordSpans(t)
	reader := sdkmetric.NewManualReader()
	if err := EnableCommandMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { commandDuration.Store(nil); httpDuration.Store(nil) })

	server, client := net.Pipe()
	go NewHTTPHandler(cache.New(nil), "").Handle(server)
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(client)
	do := func(method, target, body string) int {
		req, _ := http.NewRequest(method, "http://gopogo"+target, strings.NewReader(body))
		req.Write(client)
		resp, err := http.ReadResponse(br, req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := do("PUT", "/k", "v"); code != 200 {
		t.Fatalf("PUT: %d", code)
	}
	if code := do("PUT", "/k?ex=soon", "v"); code != 500 {
		t.Fatalf("PUT with a bad ex: %d", code)
	}
	if code := do("GET", "/@nope", ""); code != 400 {
		t.Fatalf("GET /@nope: %d", code)
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(rec.Ended()) < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	var failed sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "PUT /{key}" && spanAttr(s, "http.response.status_code") == "500" {
			failed = s
		}
	}
	if failed == nil || failed.Status().Code != codes.Error || spanAttr(failed, "error.type") != "500" {
		t.Fatalf("no failed PUT span with error status: %v", rec.Ended())
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	counts := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "http.server.request.duration" {
				continue
			}
			for _, dp := range md.Data.(metricdata.Histogram[float64]).DataPoints {
				counts[dp.Attributes.Encoded(attribute.DefaultEncoder())] += dp.Count
			}
		}
	}
	base := "network.protocol.name=http,network.protocol.version=1.1,url.scheme=http"
	for k, n := range map[string]uint64{
		"http.request.method=PUT,http.response.status_code=200,http.route=/{key}," + base:                1,
		"error.type=500,http.request.method=PUT,http.response.status_code=500,http.route=/{key}," + base: 1,
		"http.request.method=GET,http.response.status_code=400," + base:                                  1,
	} {
		if counts[k] != n {
			t.Errorf("%s: %d, want %d (all: %v)", k, counts[k], n, counts)
		}
	}
}
