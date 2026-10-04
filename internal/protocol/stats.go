package protocol

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
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

// statLines returns server stats as name/value pairs in the order pogocache's
// STATS command reports them, limited to what gopogo tracks.
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
	hits, _ := s["num_hits"].(uint64)
	misses, _ := s["num_misses"].(uint64)
	return [][2]string{
		{"pid", strconv.Itoa(os.Getpid())},
		{"uptime", strconv.FormatInt(int64(time.Since(startTime).Seconds()), 10)},
		{"time", strconv.FormatInt(time.Now().Unix(), 10)},
		{"product", "gopogo"},
		{"version", Version},
		{"pointer_size", strconv.Itoa(strconv.IntSize)},
		{"max_connections", strconv.FormatInt(ConnStats.Max, 10)},
		{"curr_connections", strconv.FormatInt(ConnStats.Curr.Load(), 10)},
		{"total_connections", strconv.FormatInt(ConnStats.Total.Load(), 10)},
		{"rejected_connections", strconv.FormatInt(ConnStats.Rejected.Load(), 10)},
		{"cmd_get", strconv.FormatUint(hits+misses, 10)},
		{"get_hits", num(s["num_hits"])},
		{"get_misses", num(s["num_misses"])},
		{"evictions", num(s["num_evicted"])},
		{"expired_unfetched", num(s["num_expired"])},
		{"bytes", num(s["mem_used"])},
		{"limit_maxbytes", num(s["max_memory"])},
		{"curr_items", num(s["num_items"])},
	}
}
