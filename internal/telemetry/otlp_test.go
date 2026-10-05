package telemetry

import (
	"context"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestParseHeaders(t *testing.T) {
	h, err := ParseHeaders("Authorization=Bearer%20abc, X-Org = o1 ,")
	if err != nil || h["Authorization"] != "Bearer abc" || h["X-Org"] != "o1" || len(h) != 2 {
		t.Fatalf("got %v, %v", h, err)
	}
	if h, _ := ParseHeaders("Authorization=Bearer abc"); h["Authorization"] != "Bearer abc" {
		t.Fatalf("literal space: %v", h)
	}
	if _, err := ParseHeaders("novalue"); err == nil {
		t.Fatal("expected an error for a header without =")
	}
}

// TestOTLPHTTPBaseURL checks that a base URL endpoint receives metrics and
// traces under <base>/v1/... with the configured headers.
func TestOTLPHTTPBaseURL(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{} // path -> Authorization header
	var logBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got[r.URL.Path] = r.Header.Get("Authorization")
		if strings.HasSuffix(r.URL.Path, "/v1/logs") {
			logBody = append(logBody, body...)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &Config{
		Enabled:      true,
		ExporterType: "otlp",
		Protocol:     "http",
		OTLPEndpoint: srv.URL + "/src-test/",
		Headers:      map[string]string{"Authorization": "Bearer t0ken"},
		ServiceName:  "gopogo-test",
		SampleRatio:  1,
	}
	ctx := context.Background()
	m, err := NewMetrics(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := NewTracer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	lg, err := NewLogger(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	lg.Install(slog.LevelInfo)
	t.Cleanup(func() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) })
	log.Printf("hello from the log package")
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	m.CacheStoreCount.Add(ctx, 1)
	_, span := otel.Tracer("test").Start(ctx, "op")
	span.End()
	if err := tr.Shutdown(ctx); err != nil {
		t.Fatalf("trace export: %v", err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("metric export: %v", err)
	}
	if err := lg.Shutdown(ctx); err != nil {
		t.Fatalf("log export: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(string(logBody), "hello from the log package") {
		t.Errorf("log record body not exported")
	}
	for _, path := range []string{"/src-test/v1/metrics", "/src-test/v1/traces", "/src-test/v1/logs"} {
		if auth, ok := got[path]; !ok || auth != "Bearer t0ken" {
			t.Errorf("%s: received=%v auth=%q (all: %v)", path, ok, auth, got)
		}
	}
}

func TestOTLPProtocolValidation(t *testing.T) {
	cfg := &Config{Protocol: "carrier-pigeon"}
	if _, err := cfg.useHTTP(); err == nil {
		t.Fatal("expected an error for an unknown protocol")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	if h, err := (&Config{}).useHTTP(); err != nil || !h {
		t.Fatalf("env protocol: http=%v err=%v", h, err)
	}
}
