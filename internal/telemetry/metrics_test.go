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
