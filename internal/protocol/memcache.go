package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

type MemcacheHandler struct {
	cache *cache.Cache
	auth  string
}

// NewMemcacheHandler creates a Memcache text protocol handler. The memcache
// protocol has no way to authenticate, so when auth is set every command is
// refused, as in pogocache; use another protocol for password-protected
// access.
func NewMemcacheHandler(cache *cache.Cache, auth string) *MemcacheHandler {
	return &MemcacheHandler{
		cache: cache,
		auth:  auth,
	}
}

// incrOrDecr returns the hit and miss counters for incr or decr.
func incrOrDecr(incr bool) (hits, misses *atomic.Uint64) {
	if incr {
		return &ctr(TypeMemcache).incrHits, &ctr(TypeMemcache).incrMisses
	}
	return &ctr(TypeMemcache).decrHits, &ctr(TypeMemcache).decrMisses
}

// memcacheNoMemory is memcached's reply when a write does not fit and eviction
// is disabled.
const memcacheNoMemory = "SERVER_ERROR out of memory storing object\r\n"

// memcacheStorageCmds are the commands followed by a data block.
var memcacheStorageCmds = map[string]bool{
	"set": true, "add": true, "replace": true, "append": true, "prepend": true, "cas": true,
}

// Handle serves a connection known to speak the memcache text protocol and
// closes it.
func (h *MemcacheHandler) Handle(conn net.Conn) {
	c := NewConn(&Handlers{Memcache: h}, conn.RemoteAddr().String(), false)
	c.proto = TypeMemcache
	serve(conn, c)
}

// process runs the complete commands at the start of in. A storage command
// is complete once its data block and the line end after it have arrived.
func (h *MemcacheHandler) process(c *Conn, in, out []byte) (int, []byte, Action) {
	w := &outBuf{b: out}
	pos := 0
	for pos < len(in) && len(w.b) < outputLimit {
		i := bytes.IndexByte(in[pos:], '\n')
		if i < 0 {
			if len(in)-pos > maxLine {
				w.WriteString("CLIENT_ERROR line too long\r\n")
				return pos, w.b, Close
			}
			break
		}
		next := pos + i + 1
		line := strings.TrimSpace(string(in[pos:next]))
		parts := strings.Fields(line)
		if len(parts) == 0 {
			pos = next
			continue
		}
		cmd := strings.ToLower(parts[0])

		// A storage command's data block follows its line. Without auth it
		// is read only when the handler gets far enough to read it; with
		// auth it is skipped, n bytes and CRLF.
		var data []byte
		if n, ok := memcacheDataLen(cmd, parts, h.auth != ""); ok {
			if len(in) < next+n {
				break
			}
			// The cache keeps the value, so it cannot share the input buffer.
			data = bytes.Clone(in[next : next+n])
			if h.auth != "" {
				if len(in) < next+n+2 {
					break
				}
				next += n + 2
			} else {
				j := bytes.IndexByte(in[next+n:], '\n')
				if j < 0 {
					break
				}
				next += n + j + 1
			}
		}
		pos = next

		spanName := cmd
		if !memcacheCmds[cmd] {
			spanName = "UNKNOWN"
		}
		obs := beginCommand(context.Background(), TypeMemcache, c.addr, spanName)
		replyStart := len(w.b)
		if h.auth != "" {
			if cmd == "quit" {
				obs.end("")
				return pos, w.b, Close
			}
			countAuth(TypeMemcache, false)
			w.WriteString("CLIENT_ERROR Authentication required\r\n")
			obs.end(memcacheErrorReply(w.b[replyStart:]))
			continue
		}
		monitors.publish(c.addr, parts)

		switch cmd {
		case "get", "gets":
			h.writeValues(w, parts[1:], cmd == "gets", nil)

		case "set":
			h.handleStore(w, parts, data, false, false)

		case "add":
			h.handleStore(w, parts, data, true, false)

		case "replace":
			h.handleStore(w, parts, data, false, true)

		case "append":
			h.handleAppend(w, parts, data, true)

		case "prepend":
			h.handleAppend(w, parts, data, false)

		case "cas":
			h.handleCAS(w, parts, data)

		case "delete":
			h.handleDelete(w, parts)

		case "incr":
			h.handleIncr(w, parts, true)

		case "decr":
			h.handleIncr(w, parts, false)

		case "touch":
			h.handleTouch(w, parts)

		case "gat", "gats":
			h.handleGAT(w, parts, cmd == "gats")

		case "verbosity":
			// Logging levels are not configurable; accept and acknowledge.
			if len(parts) < 2 || len(parts) > 3 {
				w.WriteString("ERROR\r\n")
			} else if !(len(parts) == 3 && parts[2] == "noreply") {
				w.WriteString("OK\r\n")
			}

		case "flush_all":
			ctr(TypeMemcache).cmdFlush.Add(1)
			h.cache.Clear()
			w.WriteString("OK\r\n")

		case "stats":
			h.handleStats(w)

		case "version":
			w.WriteString("VERSION " + Version + "\r\n")

		case "quit":
			obs.end("")
			return pos, w.b, Close

		default:
			w.WriteString("ERROR\r\n")
		}
		obs.end(memcacheErrorReply(w.b[replyStart:]))
	}
	return pos, w.b, Continue
}

// memcacheDataLen returns the length of the data block that follows a
// storage command, and whether the command reads one: the handlers read it
// only after their earlier arguments parse, and with auth a block with a
// valid length is skipped.
func memcacheDataLen(cmd string, parts []string, auth bool) (int, bool) {
	if !memcacheStorageCmds[cmd] || len(parts) < 5 {
		return 0, false
	}
	n, err := strconv.Atoi(parts[4])
	if err != nil || n < 0 || n > maxBulkLen {
		return 0, false
	}
	if auth || cmd == "append" || cmd == "prepend" {
		return n, true
	}
	if _, err := strconv.ParseUint(parts[2], 10, 32); err != nil {
		return 0, false
	}
	if _, err := strconv.ParseInt(parts[3], 10, 64); err != nil {
		return 0, false
	}
	if cmd == "cas" {
		if len(parts) < 6 {
			return 0, false
		}
		if _, err := strconv.ParseUint(parts[5], 10, 64); err != nil {
			return 0, false
		}
	}
	return n, true
}

// memcacheCmds are the commands the handler implements; others are traced
// as UNKNOWN.
var memcacheCmds = map[string]bool{
	"get": true, "gets": true, "gat": true, "gats": true, "set": true, "add": true,
	"replace": true, "append": true, "prepend": true, "cas": true, "delete": true,
	"incr": true, "decr": true, "touch": true, "flush_all": true, "stats": true,
	"version": true, "verbosity": true, "quit": true,
}

// memcacheErrorReply returns the first line of reply if it is a memcache
// error (ERROR, CLIENT_ERROR or SERVER_ERROR), else "".
func memcacheErrorReply(reply []byte) string {
	if i := bytes.Index(reply, []byte("\r\n")); i >= 0 {
		reply = reply[:i]
	}
	if bytes.Equal(reply, []byte("ERROR")) || bytes.HasPrefix(reply, []byte("CLIENT_ERROR")) ||
		bytes.HasPrefix(reply, []byte("SERVER_ERROR")) {
		return string(reply)
	}
	return ""
}

// handleGAT implements gat/gats <exptime> <key>*: return the found keys, like
// get/gets, and set their expiration.
func (h *MemcacheHandler) handleGAT(writer *outBuf, parts []string, withCAS bool) {
	if len(parts) < 3 {
		writer.WriteString("ERROR\r\n")
		return
	}
	exptime, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	expireAt := memcacheExpireAt(exptime)
	h.writeValues(writer, parts[2:], withCAS, func(e *cache.Entry) { e.SetExpireAt(expireAt) })
}

// writeValues writes a VALUE block for each found key, then END. touch, if
// set, is applied to each found entry after its value is read.
func (h *MemcacheHandler) writeValues(writer *outBuf, keys []string, withCAS bool, touch func(*cache.Entry)) {
	for _, key := range keys {
		entry, found := h.cache.Load([]byte(key))
		ctr(TypeMemcache).cmdGet.Add(1)
		countHit(found, &ctr(TypeMemcache).getHits, &ctr(TypeMemcache).getMisses)
		if touch != nil {
			ctr(TypeMemcache).cmdTouch.Add(1)
			countHit(found, &ctr(TypeMemcache).touchHits, &ctr(TypeMemcache).touchMisses)
		}
		if !found {
			continue
		}
		if touch != nil {
			touch(entry)
		}
		
		if withCAS {
			fmt.Fprintf(writer, "VALUE %s %d %d %d\r\n", 
				key, entry.Flags(), len(entry.Value()), entry.CAS())
		} else {
			fmt.Fprintf(writer, "VALUE %s %d %d\r\n", 
				key, entry.Flags(), len(entry.Value()))
		}
		
		writer.Write(entry.Value())
		writer.WriteString("\r\n")
	}
	writer.WriteString("END\r\n")
}

// memcacheMaxRelative is memcached's limit for a relative exptime (30 days);
// larger values are absolute Unix times.
const memcacheMaxRelative = 60 * 60 * 24 * 30

// memcacheExpireAt converts a memcache exptime to an absolute expiry in Unix
// nanoseconds: 0 never expires, a negative value or a past Unix time has
// already expired, up to 30 days is relative, and more is a Unix time.
func memcacheExpireAt(exptime int64) int64 {
	now := time.Now()
	switch {
	case exptime == 0:
		return 0
	case exptime < 0:
		return 1 // in the past: expired
	case exptime > memcacheMaxRelative:
		if at := time.Unix(exptime, 0); at.After(now) {
			return at.UnixNano()
		}
		return 1
	default:
		return now.Add(time.Duration(exptime) * time.Second).UnixNano()
	}
}

// memcacheTTL converts a memcache exptime to a StoreOptions TTL: 0 for none,
// and 1ns (expired at once) for a negative or past exptime.
func memcacheTTL(exptime int64) time.Duration {
	switch at := memcacheExpireAt(exptime); at {
	case 0:
		return 0
	case 1:
		return time.Nanosecond
	default:
		return time.Until(time.Unix(0, at))
	}
}

func (h *MemcacheHandler) handleStore(writer *outBuf, parts []string, data []byte, addOnly, replaceOnly bool) {
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	size, err := strconv.Atoi(parts[4])
	if err != nil || size < 0 || size > maxBulkLen {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 5 && parts[5] == "noreply"
	
	
	existing, _ := h.cache.Load([]byte(key))
	
	if addOnly && existing != nil {
		if !noreply {
			writer.WriteString("NOT_STORED\r\n")
		}
		return
	}
	
	if replaceOnly && existing == nil {
		if !noreply {
			writer.WriteString("NOT_STORED\r\n")
		}
		return
	}
	
	opts := &cache.StoreOptions{
		Flags: uint32(flags),
	}
	
	opts.TTL = memcacheTTL(exptime)
	
	ctr(TypeMemcache).cmdSet.Add(1)
	if _, err := h.cache.Store([]byte(key), data, opts); err != nil {
		ctr(TypeMemcache).storeNoMemory.Add(1)
		if !noreply {
			writer.WriteString(memcacheNoMemory)
		}
		return
	}
	
	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (h *MemcacheHandler) handleCAS(writer *outBuf, parts []string, data []byte) {
	if len(parts) < 6 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	size, err := strconv.Atoi(parts[4])
	if err != nil || size < 0 || size > maxBulkLen {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	cas, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 6 && parts[6] == "noreply"
	
	
	opts := &cache.StoreOptions{
		Flags: uint32(flags),
	}
	
	opts.TTL = memcacheTTL(exptime)
	
	ctr(TypeMemcache).cmdSet.Add(1)
	success, err := h.cache.CompareAndSwap([]byte(key), data, cas, opts)
	if err == cache.ErrOutOfMemory {
		ctr(TypeMemcache).storeNoMemory.Add(1)
	}
	if err != nil {
		if !noreply {
			if err == cache.ErrOutOfMemory {
				writer.WriteString(memcacheNoMemory)
			} else {
				writer.WriteString("NOT_FOUND\r\n")
			}
		}
		return
	}
	
	if !success {
		if !noreply {
			writer.WriteString("EXISTS\r\n")
		}
		return
	}
	
	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (h *MemcacheHandler) handleAppend(writer *outBuf, parts []string, data []byte, isAppend bool) {
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	size, err := strconv.Atoi(parts[4])
	if err != nil || size < 0 || size > maxBulkLen {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 5 && parts[5] == "noreply"
	
	
	// Update is atomic and keeps the entry's flags and TTL.
	err = h.cache.Update([]byte(key), func(cur []byte, found bool) ([]byte, error) {
		if !found {
			return nil, errMemcacheNotFound
		}
		out := make([]byte, 0, len(cur)+len(data))
		if isAppend {
			out = append(append(out, cur...), data...)
		} else {
			out = append(append(out, data...), cur...)
		}
		return out, nil
	})
	if err != nil {
		if err == cache.ErrOutOfMemory {
			ctr(TypeMemcache).storeNoMemory.Add(1)
		}
		if !noreply {
			if err == cache.ErrOutOfMemory {
				writer.WriteString(memcacheNoMemory)
			} else {
				writer.WriteString("NOT_STORED\r\n")
			}
		}
		return
	}
	
	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (h *MemcacheHandler) handleDelete(writer *outBuf, parts []string) {
	if len(parts) < 2 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	noreply := len(parts) > 2 && parts[len(parts)-1] == "noreply"
	
	found := h.cache.Delete([]byte(key))
	countHit(found, &ctr(TypeMemcache).deleteHits, &ctr(TypeMemcache).deleteMisses)
	if found {
		if !noreply {
			writer.WriteString("DELETED\r\n")
		}
	} else {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
	}
}

var (
	errMemcacheNotFound   = errors.New("not found")
	errMemcacheNonNumeric = errors.New("non-numeric")
)

// handleIncr implements incr/decr on unsigned 64-bit decimal values. Like
// memcached, a missing key is NOT_FOUND, incr wraps around at 2^64 and decr
// stops at 0.
func (h *MemcacheHandler) handleIncr(writer *outBuf, parts []string, incr bool) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	key := parts[1]
	delta, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR invalid numeric delta argument\r\n")
		return
	}

	noreply := len(parts) > 3 && parts[3] == "noreply"

	var newVal uint64
	err = h.cache.Update([]byte(key), func(cur []byte, found bool) ([]byte, error) {
		if !found {
			return nil, errMemcacheNotFound
		}
		n, err := strconv.ParseUint(string(cur), 10, 64)
		if err != nil {
			return nil, errMemcacheNonNumeric
		}
		switch {
		case incr:
			newVal = n + delta
		case delta > n:
			newVal = 0
		default:
			newVal = n - delta
		}
		return strconv.AppendUint(nil, newVal, 10), nil
	})
	hits, misses := incrOrDecr(incr)
	switch err {
	case nil:
		hits.Add(1)
	case errMemcacheNotFound:
		misses.Add(1)
	case cache.ErrOutOfMemory:
		ctr(TypeMemcache).storeNoMemory.Add(1)
	}
	if noreply {
		return
	}
	switch err {
	case nil:
		fmt.Fprintf(writer, "%d\r\n", newVal)
	case errMemcacheNotFound:
		writer.WriteString("NOT_FOUND\r\n")
	case cache.ErrOutOfMemory:
		writer.WriteString(memcacheNoMemory)
	default:
		writer.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
	}
}

func (h *MemcacheHandler) handleTouch(writer *outBuf, parts []string) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	exptime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 3 && parts[3] == "noreply"
	
	entry, found := h.cache.Load([]byte(key))
	ctr(TypeMemcache).cmdTouch.Add(1)
	countHit(found, &ctr(TypeMemcache).touchHits, &ctr(TypeMemcache).touchMisses)
	if !found {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
		return
	}
	
	entry.SetExpireAt(memcacheExpireAt(exptime))
	
	if !noreply {
		writer.WriteString("TOUCHED\r\n")
	}
}

func (h *MemcacheHandler) handleStats(writer *outBuf) {
	for _, kv := range statLines(h.cache) {
		fmt.Fprintf(writer, "STAT %s %s\r\n", kv[0], kv[1])
	}
	writer.WriteString("END\r\n")
}