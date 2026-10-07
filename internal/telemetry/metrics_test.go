package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestExpirationCount(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	m := &Metrics{meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")}
	c := cache.New(nil)
	if err := m.RegisterGauges(c.MemUsed, func() int64 { return int64(c.NumItems()) },
		func() int64 { return int64(c.NumExpired()) }); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"get", "del", "swept1", "swept2"} {
		if _, err := c.Store([]byte(k), []byte("v"), &cache.StoreOptions{TTL: 10 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	// One expiry found by a read, one by a delete, two by the sweep.
	if _, ok := c.Load([]byte("get")); ok {
		t.Fatal("expired key was returned")
	}
	c.Delete([]byte("del"))
	if n := c.Sweep(); n != 2 {
		t.Fatalf("sweep removed %d, want 2", n)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "cache.expiration.count" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok || !sum.IsMonotonic || len(sum.DataPoints) != 1 {
				t.Fatalf("cache.expiration.count data %#v", md.Data)
			}
			if got := sum.DataPoints[0].Value; got != 4 {
				t.Errorf("cache.expiration.count = %d, want 4", got)
			}
			return
		}
	}
	t.Fatal("cache.expiration.count not reported")
}

func TestDurationsInSeconds(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	m := &Metrics{meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")}
	if err := m.initializeInstruments(); err != nil {
		t.Fatal(err)
	}
	m.RecordStore(context.Background(), "inserted", 1500*time.Microsecond)
	m.RecordSave(context.Background(), true, 2*time.Second)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			hist, ok := md.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			if md.Unit != "s" {
				t.Errorf("%s unit %q, want s", md.Name, md.Unit)
			}
			for _, dp := range hist.DataPoints {
				got[md.Name] = dp.Sum
			}
		}
	}
	if got["cache.store.duration"] != 0.0015 || got["cache.save.duration"] != 2 {
		t.Errorf("sums %v, want store 0.0015s and save 2s", got)
	}
}

func TestSamplerFromEnvironment(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	sampled := func(cfg *Config) bool {
		tr, err := NewTracer(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer tr.Shutdown(context.Background())
		_, span := tr.provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()
		return span.SpanContext().IsSampled()
	}
	cfg := &Config{Enabled: true, ExporterType: "otlp", TracesExporter: "none", ServiceName: "t", SampleRatio: 1}
	if sampled(cfg) {
		t.Error("OTEL_TRACES_SAMPLER=always_off ignored when no ratio was given")
	}
	cfg.SampleRatioSet = true
	if !sampled(cfg) {
		t.Error("an explicit ratio of 1 did not override OTEL_TRACES_SAMPLER")
	}
}
