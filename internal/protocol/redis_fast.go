package protocol

import (
	"context"
	"strconv"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// fast runs plain GET key and SET key value, the commands that dominate
// cache traffic, straight from the parsed bytes: no argument strings, no
// command lookup and no result value. Like pogocache's own GET and SET
// paths, it does what cmdGet and cmdSet do for these forms, with the same
// replies, counters, MONITOR lines and telemetry. It reports false for
// anything else, which takes the general path.
func (h *RedisHandler) fast(c *Conn, argv [][]byte, out []byte) ([]byte, bool) {
	s := c.redis
	if !s.authed || s.ctx != nil {
		return out, false
	}
	switch {
	case len(argv) == 2 && isCommand(argv[0], "GET"):
		obs := beginCommand(context.Background(), TypeRedis, s.addr, "GET")
		publishFast(s.addr, argv)
		entry, found := h.exec.cache.Load(argv[1])
		ctr(TypeRedis).cmdGet.Add(1)
		countHit(found, &ctr(TypeRedis).getHits, &ctr(TypeRedis).getMisses)
		if !found {
			out = append(out, "$-1\r\n"...)
		} else {
			v := entry.Value()
			out = strconv.AppendInt(append(out, '$'), int64(len(v)), 10)
			out = append(out, '\r', '\n')
			out = append(append(out, v...), '\r', '\n')
		}
		obs.end("")
		return out, true

	case len(argv) == 3 && isCommand(argv[0], "SET"):
		obs := beginCommand(context.Background(), TypeRedis, s.addr, "SET")
		publishFast(s.addr, argv)
		ctr(TypeRedis).cmdSet.Add(1)
		// The cache keeps both, so they cannot share the input buffer:
		// one allocation holds the pair.
		kv := make([]byte, len(argv[1])+len(argv[2]))
		key := kv[:copy(kv, argv[1]):len(argv[1])]
		val := kv[len(key):]
		copy(val, argv[2])
		if _, err := h.exec.cache.Store(key, val, nil); err == cache.ErrOutOfMemory {
			r := noMemory(TypeRedis)
			out = appendRESP(out, rv{kind: '-', s: r.err})
			obs.end(r.err)
			return out, true
		}
		out = append(out, "+OK\r\n"...)
		obs.end("")
		return out, true
	}
	return out, false
}

// isCommand reports whether name is cmd, which is upper case, in any case.
func isCommand(name []byte, cmd string) bool {
	if len(name) != len(cmd) {
		return false
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; c != cmd[i] && c-('a'-'A') != cmd[i] {
			return false
		}
	}
	return true
}

// publishFast reports a fast-path command to MONITOR clients, if any.
func publishFast(addr string, argv [][]byte) {
	if monitors.active.Load() == 0 {
		return
	}
	monitors.publish(addr, argStrings(argv))
}
