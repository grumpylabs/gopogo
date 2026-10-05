package telemetry

import (
	"context"
	"fmt"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
	"github.com/grumpylabs/gopogo/internal/sysmem"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RegisterServerMetrics exports the server counters shown by STATS, the
// connection counts and process CPU and memory as observable instruments,
// read when metrics are collected:
//
//	gopogo.commands           {protocol, command=get|set|flush|touch}
//	gopogo.keyspace.lookups   {protocol, operation=get|delete|incr|decr|touch, result=hit|miss}
//	gopogo.store.rejected     {protocol, reason=no_memory|too_large}
//	gopogo.auth.attempts      {protocol, result=success|failure}
//	gopogo.connections.active, gopogo.connections.limit (gauges)
//	gopogo.connections.accepted, gopogo.connections.rejected
//	cache.items.stored        successful writes
//	process.cpu.time          {cpu.mode=user|system}
//	process.memory.usage      resident memory (approximate off Linux)
//
// Per-protocol series appear once that protocol has a nonzero count.
func (m *Metrics) RegisterServerMetrics(c *cache.Cache) error {
	if m.meter == nil {
		return nil
	}
	mt := m.meter
	commands, err1 := mt.Int64ObservableCounter("gopogo.commands",
		metric.WithDescription("Commands by protocol, as counted by STATS cmd_*"), metric.WithUnit("{command}"))
	lookups, err2 := mt.Int64ObservableCounter("gopogo.keyspace.lookups",
		metric.WithDescription("Key lookups by operation and hit or miss"), metric.WithUnit("{lookup}"))
	rejected, err3 := mt.Int64ObservableCounter("gopogo.store.rejected",
		metric.WithDescription("Writes refused"), metric.WithUnit("{write}"))
	auth, err4 := mt.Int64ObservableCounter("gopogo.auth.attempts",
		metric.WithDescription("Authentication checks"), metric.WithUnit("{attempt}"))
	connActive, err5 := mt.Int64ObservableGauge("gopogo.connections.active",
		metric.WithDescription("Open client connections"), metric.WithUnit("{connection}"))
	connLimit, err6 := mt.Int64ObservableGauge("gopogo.connections.limit",
		metric.WithDescription("Maximum client connections (--maxconns)"), metric.WithUnit("{connection}"))
	connAccepted, err7 := mt.Int64ObservableCounter("gopogo.connections.accepted",
		metric.WithDescription("Client connections accepted"), metric.WithUnit("{connection}"))
	connRejected, err8 := mt.Int64ObservableCounter("gopogo.connections.rejected",
		metric.WithDescription("Client connections refused at the limit"), metric.WithUnit("{connection}"))
	stored, err9 := mt.Int64ObservableCounter("cache.items.stored",
		metric.WithDescription("Successful writes (inserts and replacements)"), metric.WithUnit("{item}"))
	cpu, err10 := mt.Float64ObservableCounter("process.cpu.time",
		metric.WithDescription("CPU time used by the process"), metric.WithUnit("s"))
	rss, err11 := mt.Int64ObservableGauge("process.memory.usage",
		metric.WithDescription("Resident memory of the process"), metric.WithUnit("By"))
	for _, err := range []error{err1, err2, err3, err4, err5, err6, err7, err8, err9, err10, err11} {
		if err != nil {
			return fmt.Errorf("failed to create server metrics: %w", err)
		}
	}

	_, err := mt.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, p := range protocol.CounterSnapshot() {
			if p.Protocol == "unknown" {
				continue
			}
			proto := attribute.String("protocol", p.Protocol)
			obs := func(inst metric.Int64Observable, v uint64, attrs ...attribute.KeyValue) {
				if v > 0 {
					o.ObserveInt64(inst, int64(v), metric.WithAttributes(append(attrs, proto)...))
				}
			}
			cmd := func(name string) attribute.KeyValue { return attribute.String("command", name) }
			obs(commands, p.CmdGet, cmd("get"))
			obs(commands, p.CmdSet, cmd("set"))
			obs(commands, p.CmdFlush, cmd("flush"))
			obs(commands, p.CmdTouch, cmd("touch"))

			lookup := func(op string, hits, misses uint64) {
				opAttr := attribute.String("operation", op)
				obs(lookups, hits, opAttr, attribute.String("result", "hit"))
				obs(lookups, misses, opAttr, attribute.String("result", "miss"))
			}
			lookup("get", p.GetHits, p.GetMisses)
			lookup("delete", p.DeleteHits, p.DeleteMisses)
			lookup("incr", p.IncrHits, p.IncrMisses)
			lookup("decr", p.DecrHits, p.DecrMisses)
			lookup("touch", p.TouchHits, p.TouchMisses)

			obs(rejected, p.StoreNoMemory, attribute.String("reason", "no_memory"))
			obs(rejected, p.StoreTooLarge, attribute.String("reason", "too_large"))
			obs(auth, p.AuthCmds-p.AuthErrors, attribute.String("result", "success"))
			obs(auth, p.AuthErrors, attribute.String("result", "failure"))
		}

		cs := &protocol.ConnStats
		o.ObserveInt64(connActive, cs.Curr.Load())
		o.ObserveInt64(connLimit, cs.Max)
		o.ObserveInt64(connAccepted, cs.Total.Load())
		o.ObserveInt64(connRejected, cs.Rejected.Load())
		o.ObserveInt64(stored, int64(c.TotalItems()))
		if user, system, ok := protocol.CPUSeconds(); ok {
			o.ObserveFloat64(cpu, user, metric.WithAttributes(attribute.String("cpu.mode", "user")))
			o.ObserveFloat64(cpu, system, metric.WithAttributes(attribute.String("cpu.mode", "system")))
		}
		o.ObserveInt64(rss, sysmem.RSS())
		return nil
	}, commands, lookups, rejected, auth, connActive, connLimit, connAccepted, connRejected, stored, cpu, rss)
	if err != nil {
		return fmt.Errorf("failed to register server metrics: %w", err)
	}
	return nil
}
