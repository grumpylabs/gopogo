package protocol

import (
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/sysmem"
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

// counters are server-wide command counters reported by STATS, counted the
// way pogocache counts them (per key for multi-key reads and deletes).
var counters struct {
	cmdGet, cmdSet, cmdFlush, cmdTouch atomic.Uint64
	getHits, getMisses                 atomic.Uint64
	deleteHits, deleteMisses           atomic.Uint64
	incrHits, incrMisses               atomic.Uint64
	decrHits, decrMisses               atomic.Uint64
	touchHits, touchMisses             atomic.Uint64
	storeTooLarge, storeNoMemory       atomic.Uint64
	authCmds, authErrors               atomic.Uint64
}

// countHit adds one to hits or misses.
func countHit(found bool, hits, misses *atomic.Uint64) {
	if found {
		hits.Add(1)
	} else {
		misses.Add(1)
	}
}

// countAuth records an authentication check.
func countAuth(ok bool) {
	counters.authCmds.Add(1)
	if !ok {
		counters.authErrors.Add(1)
	}
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
	u := func(v *atomic.Uint64) string { return strconv.FormatUint(v.Load(), 10) }

	lines := [][2]string{
		{"pid", strconv.Itoa(os.Getpid())},
		{"uptime", strconv.FormatInt(int64(time.Since(startTime).Seconds()), 10)},
		{"time", strconv.FormatInt(time.Now().Unix(), 10)},
		{"product", "gopogo"},
		{"version", Version},
		{"githash", Commit},
		{"pointer_size", strconv.Itoa(strconv.IntSize)},
	}
	if user, system, ok := cpuTimes(); ok {
		lines = append(lines, [2]string{"rusage_user", user}, [2]string{"rusage_system", system})
	}
	return append(lines, [][2]string{
		{"max_connections", strconv.FormatInt(ConnStats.Max, 10)},
		{"curr_connections", strconv.FormatInt(ConnStats.Curr.Load(), 10)},
		{"total_connections", strconv.FormatInt(ConnStats.Total.Load(), 10)},
		{"rejected_connections", strconv.FormatInt(ConnStats.Rejected.Load(), 10)},
		{"cmd_get", u(&counters.cmdGet)},
		{"cmd_set", u(&counters.cmdSet)},
		{"cmd_flush", u(&counters.cmdFlush)},
		{"cmd_touch", u(&counters.cmdTouch)},
		{"get_hits", u(&counters.getHits)},
		{"get_misses", u(&counters.getMisses)},
		{"delete_misses", u(&counters.deleteMisses)},
		{"delete_hits", u(&counters.deleteHits)},
		{"incr_misses", u(&counters.incrMisses)},
		{"incr_hits", u(&counters.incrHits)},
		{"decr_misses", u(&counters.decrMisses)},
		{"decr_hits", u(&counters.decrHits)},
		{"touch_hits", u(&counters.touchHits)},
		{"touch_misses", u(&counters.touchMisses)},
		{"store_too_large", u(&counters.storeTooLarge)},
		{"store_no_memory", u(&counters.storeNoMemory)},
		{"auth_cmds", u(&counters.authCmds)},
		{"auth_errors", u(&counters.authErrors)},
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
