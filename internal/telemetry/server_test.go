package telemetry

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// send writes a raw command and reads one reply line.
func send(t *testing.T, h interface{ Handle(net.Conn) }, cmd string) string {
	t.Helper()
	server, client := net.Pipe()
	go h.Handle(server)
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	client.Write([]byte(cmd))
	line, err := bufio.NewReader(client).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return line
}

func TestServerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	m := &Metrics{meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")}
	c := cache.New(nil)
	if err := m.RegisterServerMetrics(c); err != nil {
		t.Fatal(err)
	}

	redis := protocol.NewRedisHandler(c, "", "")
	send(t, redis, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	send(t, redis, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	send(t, redis, "*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n")
	send(t, protocol.NewMemcacheHandler(c, ""), "get k\r\n")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	// points maps "metric{attr=value,...}" to the observed value.
	points := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			switch d := md.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					points[md.Name+p.Attributes.Encoded(attribute.DefaultEncoder())] = float64(p.Value)
				}
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					points[md.Name+p.Attributes.Encoded(attribute.DefaultEncoder())] = float64(p.Value)
				}
			case metricdata.Sum[float64]:
				for _, p := range d.DataPoints {
					points[md.Name+p.Attributes.Encoded(attribute.DefaultEncoder())] = p.Value
				}
			}
		}
	}
	key := func(name string, attrs ...string) string {
		s := name
		if len(attrs) > 0 {
			s += attrs[0]
			for _, a := range attrs[1:] {
				s += "," + a
			}
		}
		return s
	}
	// Counters are process-wide, so check lower bounds.
	for k, min := range map[string]float64{
		key("gopogo.commands", "db.operation.name=get", "network.protocol.name=redis"):                  2,
		key("gopogo.commands", "db.operation.name=set", "network.protocol.name=redis"):                  1,
		key("gopogo.commands", "db.operation.name=get", "network.protocol.name=memcache"):               1,
		key("gopogo.keyspace.lookups", "network.protocol.name=redis", "operation=get", "result=hit"):    1,
		key("gopogo.keyspace.lookups", "network.protocol.name=redis", "operation=get", "result=miss"):   1,
		key("gopogo.keyspace.lookups", "network.protocol.name=memcache", "operation=get", "result=hit"): 1,
		key("cache.items.stored"):   1,
		key("process.memory.usage"): 1,
	} {
		if points[k] < min {
			t.Errorf("%s = %v, want >= %v", k, points[k], min)
		}
	}
	if _, ok := points[key("gopogo.connections.active")]; !ok {
		t.Error("gopogo.connections.active not reported")
	}
	if _, _, ok := protocol.CPUSeconds(); ok && points[key("process.cpu.time", "cpu.mode=user")] <= 0 {
		t.Error("process.cpu.time user not reported")
	}
	if t.Failed() {
		for k, v := range points {
			fmt.Println(k, v)
		}
	}
}
