package protocol

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/sysmem"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Version is reported by the VERSION and STATS commands. main sets it from
// its build-time version.
var Version = "dev"

var startTime = time.Now()

// ConnStats holds server connection counters reported by STATS. The server
// updates them as it accepts and closes connections.
var ConnStats struct {
	Max      int64
	Curr     atomic.Int64
	Total    atomic.Int64
	Rejected atomic.Int64
}

// Commit is reported as githash by STATS; main sets it.
var Commit = "dev"

// commandCounters are command counters reported by STATS, counted the way
// pogocache counts them (per key for multi-key reads, deletes and touches).
type commandCounters struct {
	cmdGet, cmdSet, cmdFlush, cmdTouch atomic.Uint64
	getHits, getMisses                 atomic.Uint64
	deleteHits, deleteMisses           atomic.Uint64
	incrHits, incrMisses               atomic.Uint64
	decrHits, decrMisses               atomic.Uint64
	touchHits, touchMisses             atomic.Uint64
	storeTooLarge, storeNoMemory       atomic.Uint64
	authCmds, authErrors               atomic.Uint64
}

// counters holds one set of counters per protocol, indexed by Type. STATS
// reports their sum; telemetry exports them per protocol.
var counters [TypePostgres + 1]commandCounters

// ctr returns the counters for protocol p.
func ctr(p Type) *commandCounters {
	if p < 0 || int(p) >= len(counters) {
		p = TypeUnknown
	}
	return &counters[p]
}

// countHit adds one to hits or misses.
func countHit(found bool, hits, misses *atomic.Uint64) {
	if found {
		hits.Add(1)
	} else {
		misses.Add(1)
	}
}

// countAuth records an authentication check on protocol p.
func countAuth(p Type, ok bool) {
	c := ctr(p)
	c.authCmds.Add(1)
	if !ok {
		c.authErrors.Add(1)
	}
}

// ProtocolCounters is a snapshot of one protocol's command counters.
type ProtocolCounters struct {
	Protocol                                   string
	CmdGet, CmdSet, CmdFlush, CmdTouch         uint64
	GetHits, GetMisses                         uint64
	DeleteHits, DeleteMisses                   uint64
	IncrHits, IncrMisses, DecrHits, DecrMisses uint64
	TouchHits, TouchMisses                     uint64
	StoreTooLarge, StoreNoMemory               uint64
	AuthCmds, AuthErrors                       uint64
}

// CounterSnapshot returns the command counters of each protocol.
func CounterSnapshot() []ProtocolCounters {
	out := make([]ProtocolCounters, 0, len(counters))
	for i := range counters {
		c := &counters[i]
		out = append(out, ProtocolCounters{
			Protocol: Type(i).String(),
			CmdGet:   c.cmdGet.Load(), CmdSet: c.cmdSet.Load(),
			CmdFlush: c.cmdFlush.Load(), CmdTouch: c.cmdTouch.Load(),
			GetHits: c.getHits.Load(), GetMisses: c.getMisses.Load(),
			DeleteHits: c.deleteHits.Load(), DeleteMisses: c.deleteMisses.Load(),
			IncrHits: c.incrHits.Load(), IncrMisses: c.incrMisses.Load(),
			DecrHits: c.decrHits.Load(), DecrMisses: c.decrMisses.Load(),
			TouchHits: c.touchHits.Load(), TouchMisses: c.touchMisses.Load(),
			StoreTooLarge: c.storeTooLarge.Load(), StoreNoMemory: c.storeNoMemory.Load(),
			AuthCmds: c.authCmds.Load(), AuthErrors: c.authErrors.Load(),
		})
	}
	return out
}

// LogStats writes a debug log record with the main STATS counters, for
// following load in the log stream.
func LogStats(c *cache.Cache) {
	ce := zap.L().Check(zapcore.DebugLevel, "cache stats")
	if ce == nil {
		return
	}
	want := map[string]bool{
		"curr_items": true, "bytes": true, "total_items": true, "cmd_get": true,
		"cmd_set": true, "get_hits": true, "get_misses": true, "evictions": true,
		"curr_connections": true, "total_connections": true, "store_no_memory": true,
	}
	var fields []zap.Field
	stat := map[string]string{}
	for _, kv := range statLines(c) {
		stat[kv[0]] = kv[1]
		if want[kv[0]] {
			if n, err := strconv.ParseInt(kv[1], 10, 64); err == nil {
				fields = append(fields, zap.Int64(kv[0], n))
			}
		}
	}
	v := func(k string) int64 { n, _ := strconv.ParseInt(stat[k], 10, 64); return n }
	hitRate := 0.0
	if gets := v("get_hits") + v("get_misses"); gets > 0 {
		hitRate = 100 * float64(v("get_hits")) / float64(gets)
	}
	ce.Message = fmt.Sprintf(
		"cache holds %d items in %.1f MiB; %d gets (%.1f%% hits), %d sets, %d evictions, %d connections open",
		v("curr_items"), float64(v("bytes"))/(1<<20), v("cmd_get"), hitRate,
		v("cmd_set"), v("evictions"), v("curr_connections"))
	ce.Write(fields...)
}

// statLines returns server stats as name/value pairs in the order pogocache's
// STATS command reports them, followed by memcached-style extras.
func statLines(c *cache.Cache) [][2]string {
	s := c.Stats()
	num := func(v interface{}) string {
		switch n := v.(type) {
		case int:
			return strconv.Itoa(n)
		case int64:
			return strconv.FormatInt(n, 10)
		case uint64:
			return strconv.FormatUint(n, 10)
		}
		return "0"
	}
	// u sums a counter across protocols.
	u := func(field func(*commandCounters) *atomic.Uint64) string {
		var n uint64
		for i := range counters {
			n += field(&counters[i]).Load()
		}
		return strconv.FormatUint(n, 10)
	}

	lines := [][2]string{
		{"pid", strconv.Itoa(os.Getpid())},
		{"uptime", strconv.FormatInt(int64(time.Since(startTime).Seconds()), 10)},
		{"time", strconv.FormatInt(time.Now().Unix(), 10)},
		{"product", "gopogo"},
		{"version", Version},
		{"githash", Commit},
		{"pointer_size", strconv.Itoa(strconv.IntSize)},
	}
	if user, system, ok := CPUSeconds(); ok {
		lines = append(lines,
			[2]string{"rusage_user", strconv.FormatFloat(user, 'f', 6, 64)},
			[2]string{"rusage_system", strconv.FormatFloat(system, 'f', 6, 64)})
	}
	return append(lines, [][2]string{
		{"max_connections", strconv.FormatInt(ConnStats.Max, 10)},
		{"curr_connections", strconv.FormatInt(ConnStats.Curr.Load(), 10)},
		{"total_connections", strconv.FormatInt(ConnStats.Total.Load(), 10)},
		{"rejected_connections", strconv.FormatInt(ConnStats.Rejected.Load(), 10)},
		{"cmd_get", u(func(c *commandCounters) *atomic.Uint64 { return &c.cmdGet })},
		{"cmd_set", u(func(c *commandCounters) *atomic.Uint64 { return &c.cmdSet })},
		{"cmd_flush", u(func(c *commandCounters) *atomic.Uint64 { return &c.cmdFlush })},
		{"cmd_touch", u(func(c *commandCounters) *atomic.Uint64 { return &c.cmdTouch })},
		{"get_hits", u(func(c *commandCounters) *atomic.Uint64 { return &c.getHits })},
		{"get_misses", u(func(c *commandCounters) *atomic.Uint64 { return &c.getMisses })},
		{"delete_misses", u(func(c *commandCounters) *atomic.Uint64 { return &c.deleteMisses })},
		{"delete_hits", u(func(c *commandCounters) *atomic.Uint64 { return &c.deleteHits })},
		{"incr_misses", u(func(c *commandCounters) *atomic.Uint64 { return &c.incrMisses })},
		{"incr_hits", u(func(c *commandCounters) *atomic.Uint64 { return &c.incrHits })},
		{"decr_misses", u(func(c *commandCounters) *atomic.Uint64 { return &c.decrMisses })},
		{"decr_hits", u(func(c *commandCounters) *atomic.Uint64 { return &c.decrHits })},
		{"touch_hits", u(func(c *commandCounters) *atomic.Uint64 { return &c.touchHits })},
		{"touch_misses", u(func(c *commandCounters) *atomic.Uint64 { return &c.touchMisses })},
		{"store_too_large", u(func(c *commandCounters) *atomic.Uint64 { return &c.storeTooLarge })},
		{"store_no_memory", u(func(c *commandCounters) *atomic.Uint64 { return &c.storeNoMemory })},
		{"auth_cmds", u(func(c *commandCounters) *atomic.Uint64 { return &c.authCmds })},
		{"auth_errors", u(func(c *commandCounters) *atomic.Uint64 { return &c.authErrors })},
		{"threads", strconv.Itoa(runtime.GOMAXPROCS(0))},
		{"rss", strconv.FormatInt(sysmem.RSS(), 10)},
		{"bytes", num(s["mem_used"])},
		{"curr_items", num(s["num_items"])},
		{"total_items", strconv.FormatUint(c.TotalItems(), 10)},
		{"evictions", num(s["num_evicted"])},
		{"expired_unfetched", num(s["num_expired"])},
		{"limit_maxbytes", num(s["max_memory"])},
	}...)
}
