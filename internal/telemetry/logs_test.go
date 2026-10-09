package telemetry

import (
	"context"
	"strings"
	"sync"
	"testing"

	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type captureExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *captureExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range records {
		e.records = append(e.records, r.Clone())
	}
	return nil
}

func (*captureExporter) Shutdown(context.Context) error   { return nil }
func (*captureExporter) ForceFlush(context.Context) error { return nil }

func TestOTelCoreRecord(t *testing.T) {
	exp := &captureExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	core := &otelCore{
		LevelEnabler: zapcore.DebugLevel,
		enc:          zapcore.NewJSONEncoder(encoderConfig()),
		logger:       provider.Logger("test"),
	}
	zap.New(core, zap.AddCaller()).With(zap.String("db.system.name", "gopogo")).
		Warn("redis GET failed", zap.Int("client.port", 6379), zap.Bool("cached", true), zap.Strings("protocols", []string{"redis", "http"}))

	if len(exp.records) != 1 {
		t.Fatalf("got %d records", len(exp.records))
	}
	r := exp.records[0]
	if r.Severity() != otellog.SeverityWarn || r.SeverityText() != "WARN" {
		t.Errorf("severity %v %q", r.Severity(), r.SeverityText())
	}
	body := r.Body().AsString()
	for _, want := range []string{`"msg":"redis GET failed"`, `"db.system.name":"gopogo"`, `"client.port":6379`} {
		if !strings.Contains(body, want) {
			t.Errorf("body %s lacks %s", body, want)
		}
	}
	attrs := map[string]otellog.Value{}
	r.WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	if v := attrs["db.system.name"]; v.AsString() != "gopogo" {
		t.Errorf("db.system.name = %v", v)
	}
	if v := attrs["client.port"]; v.AsInt64() != 6379 {
		t.Errorf("client.port = %v", v)
	}
	if v := attrs["cached"]; v.Kind() != otellog.KindBool || !v.AsBool() {
		t.Errorf("cached = %v", v)
	}
	if v := attrs["protocols"]; v.Kind() != otellog.KindSlice || len(v.AsSlice()) != 2 {
		t.Errorf("protocols = %v", v)
	}
	if v := attrs["code.file.path"]; !strings.HasSuffix(v.AsString(), "logs_test.go") {
		t.Errorf("code.file.path = %v", v)
	}
	if v := attrs["code.line.number"]; v.AsInt64() == 0 {
		t.Errorf("code.line.number = %v", v)
	}
}
