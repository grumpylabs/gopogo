package protocol

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// This file is the command layer shared by the RESP, HTTP and Postgres
// handlers, following pogocache's single command table. Each command returns a
// protocol-neutral result that the handler for the connection renders:
//
//   - RESP writes result.resp.
//   - Postgres writes result.pg as a row set plus a CommandComplete tag.
//   - HTTP writes result.http (only GET, SET and DEL are reachable over HTTP).
//
// Errors are carried in result.err with the RESP wording ("ERR ..."); the
// Postgres and HTTP renderers strip the "ERR " prefix as pogocache does.
//
// Memcache keeps its own handler (memcache.go).

// Executor runs commands against a cache.
type Executor struct {
	cache   *cache.Cache
	auth    string
	persist string // default path for SAVE and LOAD
}

// NewExecutor returns an Executor. auth is the password required by AUTH (empty
// for none) and persist the default SAVE/LOAD path (may be empty).
func NewExecutor(c *cache.Cache, auth, persist string) *Executor {
	return &Executor{cache: c, auth: auth, persist: persist}
}

// session is the per-connection state commands may read or change.
type session struct {
	proto  Type
	addr   string
	authed bool
}

// rv is a RESP value.
type rv struct {
	kind byte // '+' status, '-' error, ':' int, 'u' uint, '$' bulk, '_' null, '*' array
	s    string
	n    int64
	u    uint64
	arr  []rv
}

func rvStatus(s string) rv { return rv{kind: '+', s: s} }
func rvBulk(s string) rv   { return rv{kind: '$', s: s} }
func rvInt(n int64) rv     { return rv{kind: ':', n: n} }
func rvUint(u uint64) rv   { return rv{kind: 'u', u: u} }
func rvNull() rv           { return rv{kind: '_'} }
func rvArray(a ...rv) rv   { return rv{kind: '*', arr: a} }
func rvOK() rv             { return rvStatus("OK") }
func rvStrings(ss []string) rv {
	a := make([]rv, len(ss))
	for i, s := range ss {
		a[i] = rvBulk(s)
	}
	return rv{kind: '*', arr: a}
}

// pgResult is a Postgres rendering: an optional row set and a tag.
type pgResult struct {
	cols []string   // nil for no RowDescription
	rows [][]string // DataRows
	tag  string     // CommandComplete tag
}

// httpResult is an HTTP rendering.
type httpResult struct {
	status int
	body   string
}

type result struct {
	err     string
	resp    rv
	pg      pgResult
	http    *httpResult
	quit    bool // close the connection after replying
	monitor bool // switch the RESP connection to MONITOR streaming
}

func errResult(msg string) result { return result{err: msg} }

const (
	errWrongArgs  = "ERR wrong number of arguments"
	errSyntax     = "ERR syntax error"
	errNotInteger = "ERR value is not an integer or out of range"
	errExpire     = "ERR invalid expire time"
	errNoMemory   = "ERR out of memory"
)

func wrongArgs(name string) result {
	return errResult(fmt.Sprintf("%s for '%s' command", errWrongArgs, strings.ToLower(name)))
}

// pgRow returns a one-column, one-row Postgres rendering.
func pgRow(col, val, tag string) pgResult {
	return pgResult{cols: []string{col}, rows: [][]string{{val}}, tag: tag}
}

func pgTag(tag string) pgResult { return pgResult{tag: tag} }

type commandFunc func(x *Executor, s *session, name string, args []string) result

var commands map[string]commandFunc

func init() {
	commands = map[string]commandFunc{
		"AUTH":     cmdAuth,
		"PING":     cmdPing,
		"ECHO":     cmdEcho,
		"QUIT":     cmdQuit,
		"SELECT":   cmdSelect,
		"GET":      cmdGet,
		"SET":      cmdSet,
		"SETEX":    cmdSetEx,
		"DEL":      cmdDel,
		"EXISTS":   cmdExists,
		"MGET":     cmdMGet,
		"MGETS":    cmdMGet,
		"MSET":     cmdMSet,
		"INCR":     cmdIncr,
		"DECR":     cmdIncr,
		"UINCR":    cmdIncr,
		"UDECR":    cmdIncr,
		"INCRBY":   cmdIncr,
		"DECRBY":   cmdIncr,
		"UINCRBY":  cmdIncr,
		"UDECRBY":  cmdIncr,
		"APPEND":   cmdAppend,
		"PREPEND":  cmdAppend,
		"EXPIRE":   cmdExpire,
		"TTL":      cmdTTL,
		"PTTL":     cmdTTL,
		"TOUCH":    cmdTouch,
		"KEYS":     cmdKeys,
		"SCAN":     cmdScan,
		"DBSIZE":   cmdDBSize,
		"FLUSH":    cmdFlush,
		"FLUSHDB":  cmdFlush,
		"FLUSHALL": cmdFlush,
		"SWEEP":    cmdSweep,
		"PURGE":    cmdPurge,
		"SAVE":     cmdSaveLoad,
		"LOAD":     cmdSaveLoad,
		"STATS":    cmdStats,
		"INFO":     cmdInfo,
		"VERSION":  cmdVersion,
		"MONITOR":  cmdMonitor,
		"DEBUG":    cmdDebug,
	}
}

// exec runs one command for session s.
func (x *Executor) exec(s *session, args []string) result {
	if len(args) == 0 {
		return errResult("ERR empty command")
	}
	name := strings.ToUpper(args[0])
	if !s.authed && name != "AUTH" {
		return errResult("NOAUTH Authentication required.")
	}
	monitors.publish(s.addr, args)
	fn := commands[name]
	if fn == nil {
		return errResult(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	return fn(x, s, name, args)
}

func cmdAuth(x *Executor, s *session, name string, args []string) result {
	// AUTH <user> <password> is not supported and fails as a wrong password,
	// as in pogocache.
	switch {
	case len(args) == 1:
		return wrongArgs(name)
	case len(args) > 3:
		return errResult(errSyntax)
	case len(args) == 2 && args[1] == x.auth:
		s.authed = true
		return result{resp: rvOK(), pg: pgTag("AUTH OK")}
	}
	return errResult("WRONGPASS invalid username-password pair or user is disabled.")
}

func cmdPing(x *Executor, s *session, name string, args []string) result {
	switch len(args) {
	case 1:
		return result{resp: rvStatus("PONG"), pg: pgRow("message", "PONG", "PING")}
	case 2:
		return result{resp: rvBulk(args[1]), pg: pgRow("message", args[1], "PING")}
	}
	return wrongArgs(name)
}

func cmdEcho(x *Executor, s *session, name string, args []string) result {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	return result{resp: rvBulk(args[1]), pg: pgRow("message", args[1], "ECHO")}
}

func cmdQuit(x *Executor, s *session, name string, args []string) result {
	return result{resp: rvOK(), pg: pgTag("QUIT"), quit: true}
}

func cmdSelect(x *Executor, s *session, name string, args []string) result {
	if s.proto != TypeRedis {
		return errResult(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	if len(args) != 2 {
		return wrongArgs(name)
	}
	db, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errResult(errNotInteger)
	}
	if db != 0 {
		return errResult("ERR index is out of range")
	}
	return result{resp: rvOK()}
}

func cmdGet(x *Executor, s *session, name string, args []string) result {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	entry, found := x.cache.Load([]byte(args[1]))
	if !found {
		return result{
			resp: rvNull(),
			pg:   pgResult{cols: []string{"value"}, tag: "GET 0"},
			http: &httpResult{404, "Not Found\r\n"},
		}
	}
	val := string(entry.Value())
	return result{
		resp: rvBulk(val),
		pg:   pgRow("value", val, "GET 1"),
		http: &httpResult{200, val},
	}
}

// parseExpire converts an EX/PX/EXAT/PXAT argument to a TTL.
func parseExpire(kind, arg string) (time.Duration, bool) {
	n, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	switch kind {
	case "EX":
		if n > math.MaxInt64/int64(time.Second) {
			return 0, false
		}
		return time.Duration(n) * time.Second, true
	case "PX":
		if n > math.MaxInt64/int64(time.Millisecond) {
			return 0, false
		}
		return time.Duration(n) * time.Millisecond, true
	case "EXAT":
		return time.Until(time.Unix(n, 0)), true
	default: // PXAT
		return time.Until(time.UnixMilli(n)), true
	}
}

// cmdSet implements SET key value [EX s|PX ms|EXAT ts|PXAT ms] [NX|XX] [GET]
// [KEEPTTL] [FLAGS n] [CAS n].
func cmdSet(x *Executor, s *session, name string, args []string) result {
	if len(args) < 3 {
		return wrongArgs(name)
	}
	key, val := []byte(args[1]), []byte(args[2])
	opts := &cache.StoreOptions{}
	var hasEx, get, withCAS bool
	var cas uint64
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX", "PX", "EXAT", "PXAT":
			i++
			if i == len(args) {
				return errResult(errSyntax)
			}
			ttl, ok := parseExpire(opt, args[i])
			if !ok {
				return errResult(errExpire)
			}
			if ttl <= 0 {
				ttl = time.Nanosecond // already past: expires immediately
			}
			opts.TTL = ttl
			hasEx = true
		case "NX":
			opts.NX = true
		case "XX":
			opts.XX = true
		case "GET":
			get = true
		case "KEEPTTL":
			opts.KeepTTL = true
		case "FLAGS", "CAS":
			i++
			if i == len(args) {
				return errResult(errSyntax)
			}
			n, err := strconv.ParseUint(args[i], 10, 64)
			if err != nil {
				return errResult(errSyntax)
			}
			if opt == "FLAGS" {
				opts.Flags = uint32(n)
			} else {
				cas, withCAS = n, true
			}
		default:
			return errResult(errSyntax)
		}
	}
	if (opts.KeepTTL && hasEx) || (opts.NX && opts.XX) {
		return errResult(errSyntax)
	}

	var stored bool
	var old *string
	switch {
	case withCAS:
		ok, err := x.cache.CompareAndSwap(key, val, cas, opts)
		if err == cache.ErrOutOfMemory {
			return errResult(errNoMemory)
		}
		stored = ok
	case get:
		b := x.cache.Begin()
		if e, found := b.Load(key); found {
			v := string(e.Value())
			old = &v
		}
		res := b.Store(key, val, opts)
		b.End()
		if res == cache.NoMemory {
			return errResult(errNoMemory)
		}
		stored = res != cache.NotStored
	default:
		res, err := x.cache.Store(key, val, opts)
		if err == cache.ErrOutOfMemory {
			return errResult(errNoMemory)
		}
		stored = res != cache.NotStored
	}

	r := result{pg: pgTag(name + " 0"), http: &httpResult{404, "Not Found\r\n"}}
	if stored {
		r.pg = pgTag(name + " 1")
		r.http = &httpResult{200, "Stored\r\n"}
	}
	switch {
	case get && old != nil:
		r.resp = rvBulk(*old)
		r.pg = pgRow("value", *old, name+" 1")
	case get:
		r.resp = rvNull()
		r.pg = pgResult{cols: []string{"value"}, tag: name + " 0"}
	case stored:
		r.resp = rvOK()
	default:
		r.resp = rvNull()
	}
	return r
}

func cmdSetEx(x *Executor, s *session, name string, args []string) result {
	if len(args) != 4 {
		return wrongArgs(name)
	}
	ttl, ok := parseExpire("EX", args[2])
	if !ok {
		return errResult(errExpire)
	}
	if _, err := x.cache.Store([]byte(args[1]), []byte(args[3]), &cache.StoreOptions{TTL: ttl}); err != nil {
		return errResult(errNoMemory)
	}
	return result{resp: rvOK(), pg: pgTag("SETEX 1")}
}

func cmdDel(x *Executor, s *session, name string, args []string) result {
	if len(args) < 2 {
		return wrongArgs(name)
	}
	var deleted int64
	for _, k := range args[1:] {
		if x.cache.Delete([]byte(k)) {
			deleted++
		}
	}
	r := result{resp: rvInt(deleted), pg: pgTag(fmt.Sprintf("DEL %d", deleted))}
	if deleted == 0 {
		r.http = &httpResult{404, "Not Found\r\n"}
	} else {
		r.http = &httpResult{200, "Deleted\r\n"}
	}
	return r
}

func cmdExists(x *Executor, s *session, name string, args []string) result {
	if len(args) < 2 {
		return wrongArgs(name)
	}
	var n int64
	for _, k := range args[1:] {
		if _, found := x.cache.Load([]byte(k)); found {
			n++
		}
	}
	return result{resp: rvInt(n), pg: pgRow("exists", strconv.FormatInt(n, 10), "EXISTS")}
}

// cmdMGet implements MGET and MGETS (which adds flags and CAS per key).
func cmdMGet(x *Executor, s *session, name string, args []string) result {
	if len(args) < 2 {
		return wrongArgs(name)
	}
	withCAS := name == "MGETS"
	pg := pgResult{cols: []string{"key", "value"}}
	if withCAS {
		pg.cols = []string{"key", "flags", "cas", "value"}
	}
	vals := make([]rv, 0, len(args)-1)
	for _, k := range args[1:] {
		e, found := x.cache.Load([]byte(k))
		if !found {
			vals = append(vals, rvNull())
			continue
		}
		v := string(e.Value())
		if withCAS {
			vals = append(vals, rvArray(rvUint(uint64(e.Flags())), rvUint(e.CAS()), rvBulk(v)))
			pg.rows = append(pg.rows, []string{k,
				strconv.FormatUint(uint64(e.Flags()), 10), strconv.FormatUint(e.CAS(), 10), v})
		} else {
			vals = append(vals, rvBulk(v))
			pg.rows = append(pg.rows, []string{k, v})
		}
	}
	pg.tag = fmt.Sprintf("MGET %d", len(pg.rows))
	return result{resp: rvArray(vals...), pg: pg}
}

func cmdMSet(x *Executor, s *session, name string, args []string) result {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgs(name)
	}
	for i := 1; i < len(args); i += 2 {
		if _, err := x.cache.Store([]byte(args[i]), []byte(args[i+1]), nil); err != nil {
			return errResult(errNoMemory)
		}
	}
	return result{resp: rvOK(), pg: pgTag(fmt.Sprintf("MSET %d", (len(args)-1)/2))}
}

// cmdIncr implements INCR, DECR, INCRBY, DECRBY and their unsigned
// U-prefixed variants. Values are stored as decimal text.
func cmdIncr(x *Executor, s *session, name string, args []string) result {
	by := strings.HasSuffix(name, "BY")
	if (by && len(args) != 3) || (!by && len(args) != 2) {
		return wrongArgs(name)
	}
	deltaStr := "1"
	if by {
		deltaStr = args[2]
	}
	decr := strings.Contains(name, "DECR")
	tag := strings.TrimPrefix(name, "U")
	key := []byte(args[1])

	var err error
	if strings.HasPrefix(name, "U") {
		var delta, n uint64
		if delta, err = strconv.ParseUint(deltaStr, 10, 64); err != nil {
			return errResult(errNotInteger)
		}
		if n, err = x.cache.IncrementUnsigned(key, delta, decr); err == nil {
			v := strconv.FormatUint(n, 10)
			return result{resp: rvUint(n), pg: pgRow("value", v, tag)}
		}
	} else {
		var delta, n int64
		if delta, err = strconv.ParseInt(deltaStr, 10, 64); err != nil {
			return errResult(errNotInteger)
		}
		if decr {
			if delta == math.MinInt64 {
				return errResult("ERR increment or decrement would overflow")
			}
			delta = -delta
		}
		if n, err = x.cache.Increment(key, delta); err == nil {
			v := strconv.FormatInt(n, 10)
			return result{resp: rvInt(n), pg: pgRow("value", v, tag)}
		}
	}
	return errResult("ERR " + err.Error())
}

// cmdAppend implements APPEND and PREPEND, creating the key if missing and
// preserving the flags and TTL of an existing one.
func cmdAppend(x *Executor, s *session, name string, args []string) result {
	if len(args) != 3 {
		return wrongArgs(name)
	}
	prepend := name == "PREPEND"
	value := args[2]
	var n int
	err := x.cache.Update([]byte(args[1]), func(cur []byte, found bool) ([]byte, error) {
		out := make([]byte, 0, len(cur)+len(value))
		if prepend {
			out = append(append(out, value...), cur...)
		} else {
			out = append(append(out, cur...), value...)
		}
		n = len(out)
		return out, nil
	})
	if err != nil {
		return errResult(errNoMemory)
	}
	return result{resp: rvInt(int64(n)), pg: pgTag(fmt.Sprintf("%s %d", name, n))}
}

func cmdExpire(x *Executor, s *session, name string, args []string) result {
	if len(args) != 3 {
		return wrongArgs(name)
	}
	seconds, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return errResult(errNotInteger)
	}
	var n int64
	if entry, found := x.cache.Load([]byte(args[1])); found {
		entry.SetExpireAt(time.Now().Add(time.Duration(seconds) * time.Second).UnixNano())
		n = 1
	}
	return result{resp: rvInt(n), pg: pgTag(fmt.Sprintf("EXPIRE %d", n))}
}

// cmdTTL implements TTL and PTTL: -2 for a missing key, -1 for no expiry.
func cmdTTL(x *Executor, s *session, name string, args []string) result {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	unit := time.Second
	if name == "PTTL" {
		unit = time.Millisecond
	}
	var ttl int64
	entry, found := x.cache.Load([]byte(args[1]))
	switch {
	case !found:
		ttl = -2
	case entry.ExpireAt() == 0:
		ttl = -1
	default:
		ttl = max((entry.ExpireAt()-time.Now().UnixNano())/int64(unit), 0)
	}
	col := strings.ToLower(name)
	return result{resp: rvInt(ttl), pg: pgRow(col, strconv.FormatInt(ttl, 10), name+" 1")}
}

func cmdTouch(x *Executor, s *session, name string, args []string) result {
	if len(args) < 2 {
		return wrongArgs(name)
	}
	var n int64
	for _, k := range args[1:] {
		if _, found := x.cache.LoadWithOptions([]byte(k), nil); found {
			n++
		}
	}
	return result{resp: rvInt(n), pg: pgTag(fmt.Sprintf("TOUCH %d", n))}
}

func cmdKeys(x *Executor, s *session, name string, args []string) result {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	pattern := args[1]
	keys := make([]string, 0)
	x.cache.Iterate(func(entry *cache.Entry) bool {
		key := string(entry.Key())
		if matchPattern(pattern, key) {
			keys = append(keys, key)
		}
		return true
	})
	pg := pgResult{cols: []string{"key"}, tag: fmt.Sprintf("KEYS %d", len(keys))}
	for _, k := range keys {
		pg.rows = append(pg.rows, []string{k})
	}
	return result{resp: rvStrings(keys), pg: pg}
}

// cmdScan implements SCAN cursor [MATCH pattern] [COUNT count] [TYPE type].
func cmdScan(x *Executor, s *session, name string, args []string) result {
	if len(args) < 2 {
		return wrongArgs(name)
	}
	cursor, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return errResult("ERR invalid cursor")
	}
	pattern := "*"
	count := 100
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		if i+1 == len(args) {
			return errResult(errSyntax)
		}
		i++
		switch opt {
		case "MATCH":
			pattern = args[i]
		case "COUNT":
			n, err := strconv.ParseUint(args[i], 10, 64)
			if err != nil || n == 0 || n > math.MaxInt32 {
				return errResult(errSyntax)
			}
			count = int(n)
		case "TYPE":
			if !strings.EqualFold(args[i], "string") {
				return errResult(fmt.Sprintf("ERR unknown type name '%s'", args[i]))
			}
		default:
			return errResult(errSyntax)
		}
	}
	keys, next := x.cache.Scan(cursor, count, func(k []byte) bool {
		return matchPattern(pattern, string(k))
	})
	cur := strconv.FormatUint(next, 10)
	strs := make([]string, len(keys))
	pg := pgResult{cols: []string{"key", "cursor"}, tag: fmt.Sprintf("SCAN %d", len(keys))}
	for i, k := range keys {
		strs[i] = string(k)
		pg.rows = append(pg.rows, []string{strs[i], cur})
	}
	return result{resp: rvArray(rvBulk(cur), rvStrings(strs)), pg: pg}
}

func cmdDBSize(x *Executor, s *session, name string, args []string) result {
	if len(args) != 1 {
		return wrongArgs(name)
	}
	n := int64(x.cache.NumItems())
	return result{resp: rvInt(n), pg: pgRow("count", strconv.FormatInt(n, 10), "DBSIZE")}
}

// cmdFlush implements FLUSH/FLUSHDB/FLUSHALL [ASYNC|SYNC] [FAST] [DELAY n].
// FAST is accepted for pogocache compatibility.
func cmdFlush(x *Executor, s *session, name string, args []string) result {
	async := false
	var delay int64
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "ASYNC":
			async = true
		case "SYNC":
			async = false
		case "FAST":
		case "DELAY":
			i++
			if i == len(args) {
				return errResult(errSyntax)
			}
			n, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil || n < 0 || n > 31536000 {
				return errResult("ERR invalid delay argument")
			}
			delay = n
			if delay > 0 {
				async = true
			}
		default:
			return errResult(errSyntax)
		}
	}
	switch {
	case delay > 0:
		time.AfterFunc(time.Duration(delay)*time.Second, x.cache.Clear)
	case async:
		go x.cache.Clear()
	default:
		x.cache.Clear()
	}
	mode := " SYNC"
	if async {
		mode = " ASYNC"
	}
	return result{resp: rvOK(), pg: pgTag(name + mode)}
}

// cmdSweep implements SWEEP [ASYNC|FAST], removing expired and evicted
// entries. FAST is accepted for pogocache compatibility.
func cmdSweep(x *Executor, s *session, name string, args []string) result {
	if len(args) > 2 {
		return wrongArgs(name)
	}
	async := false
	for _, a := range args[1:] {
		switch strings.ToUpper(a) {
		case "ASYNC":
			async = true
		case "FAST":
		default:
			return errResult(errSyntax)
		}
	}
	sweep := func() {
		x.cache.Sweep()
		x.cache.SweepEvicted()
	}
	if async {
		go sweep()
		return result{resp: rvOK(), pg: pgTag("SWEEP ASYNC")}
	}
	sweep()
	return result{resp: rvOK(), pg: pgTag("SWEEP SYNC")}
}

// cmdPurge implements PURGE [ASYNC], returning freed memory to the OS.
func cmdPurge(x *Executor, s *session, name string, args []string) result {
	if len(args) > 2 {
		return wrongArgs(name)
	}
	if len(args) == 2 {
		if !strings.EqualFold(args[1], "ASYNC") {
			return errResult(errSyntax)
		}
		go debug.FreeOSMemory()
		return result{resp: rvOK(), pg: pgTag("PURGE ASYNC")}
	}
	debug.FreeOSMemory()
	return result{resp: rvOK(), pg: pgTag("PURGE SYNC")}
}

// cmdSaveLoad implements SAVE [TO <path>] [FAST] and LOAD [FROM <path>]
// [FAST]. FAST is accepted for pogocache compatibility.
func cmdSaveLoad(x *Executor, s *session, name string, args []string) result {
	load := name == "LOAD"
	path := x.persist
	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(args[i])
		switch {
		case arg == "FAST":
		case (load && arg == "FROM") || (!load && arg == "TO"):
			i++
			if i == len(args) {
				return errResult(errSyntax)
			}
			path = args[i]
		default:
			return errResult(errSyntax)
		}
	}
	if path == "" {
		return errResult("ERR path not provided")
	}
	if load {
		if _, err := x.cache.LoadFromFile(path); err != nil {
			return errResult("load failed")
		}
	} else if err := x.cache.Save(path); err != nil {
		return errResult("save failed")
	}
	return result{resp: rvOK(), pg: pgTag(name + " OK")}
}

func cmdStats(x *Executor, s *session, name string, args []string) result {
	if len(args) != 1 {
		return errResult(errSyntax)
	}
	lines := statLines(x.cache)
	pairs := make([]rv, len(lines))
	pg := pgResult{cols: []string{"stat", "value"}, tag: fmt.Sprintf("STATS %d", len(lines))}
	for i, kv := range lines {
		pairs[i] = rvArray(rvBulk(kv[0]), rvBulk(kv[1]))
		pg.rows = append(pg.rows, []string{kv[0], kv[1]})
	}
	return result{resp: rvArray(pairs...), pg: pg}
}

func cmdVersion(x *Executor, s *session, name string, args []string) result {
	return result{resp: rvStatus(Version), pg: pgRow("version", Version, "VERSION 1")}
}

// cmdMonitor switches a RESP connection to streaming every command the
// server runs. It is unavailable on other protocols, as in pogocache.
func cmdMonitor(x *Executor, s *session, name string, args []string) result {
	if s.proto != TypeRedis {
		return errResult("unavailable")
	}
	if len(args) != 1 {
		return wrongArgs(name)
	}
	return result{resp: rvOK(), monitor: true}
}

// cmdDebug implements DEBUG POPULATE <count> <prefix> <size> [min-max] and
// DEBUG DETACH.
func cmdDebug(x *Executor, s *session, name string, args []string) result {
	if len(args) < 2 {
		return wrongArgs(name)
	}
	switch strings.ToUpper(args[1]) {
	case "POPULATE":
		return debugPopulate(x, args[2:])
	case "DETACH":
		// pogocache reports when the command was received and when background
		// work began, in nanoseconds; commands run inline here.
		now := time.Now().UnixNano()
		v := fmt.Sprintf("%d:%d", now, now)
		return result{resp: rvBulk(v), pg: pgRow("detach", v, "DEBUG DETACH")}
	}
	return errResult("ERR unknown subcommand")
}

// debugPopulate stores count keys named <prefix>:<i> with size zero bytes,
// spread over all CPUs, each with a random TTL in [min,max) seconds when a
// range is given.
func debugPopulate(x *Executor, args []string) result {
	if len(args) != 3 && len(args) != 4 {
		return errResult(errWrongArgs)
	}
	count, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || count < 0 {
		return errResult(errSyntax)
	}
	prefix := args[1]
	size, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || size < 0 || size > 1<<30 {
		return errResult(errSyntax)
	}
	var exMin, exMax int
	if len(args) == 4 {
		lo, hi, ok := strings.Cut(args[3], "-")
		exMin, _ = strconv.Atoi(lo)
		exMax, _ = strconv.Atoi(hi)
		if !ok || exMin < 0 || exMax <= exMin {
			return errResult(errSyntax)
		}
	}
	val := make([]byte, size)
	workers := int64(runtime.NumCPU())
	var wg sync.WaitGroup
	for w := int64(0); w < workers; w++ {
		start, end := count*w/workers, count*(w+1)/workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := []byte(prefix + ":")
			n := len(key)
			for i := start; i < end; i++ {
				key = strconv.AppendInt(key[:n], i, 10)
				var opts *cache.StoreOptions
				if exMax > 0 {
					ex := exMin + rand.IntN(exMax-exMin)
					opts = &cache.StoreOptions{TTL: time.Duration(ex) * time.Second}
				}
				x.cache.Store(append([]byte(nil), key...), val, opts)
			}
		}()
	}
	wg.Wait()
	return result{resp: rvOK(), pg: pgTag(fmt.Sprintf("DEBUG POPULATE %d", count))}
}

// cmdInfo returns a Redis-style INFO block for client libraries that probe it.
func cmdInfo(x *Executor, s *session, name string, args []string) result {
	stats := x.cache.Stats()
	info := fmt.Sprintf("# Server\r\n"+
		"redis_version:7.0.0\r\n"+
		"redis_mode:standalone\r\n"+
		"process_id:1\r\n"+
		"tcp_port:6379\r\n"+
		"\r\n"+
		"# Keyspace\r\n"+
		"db0:keys=%d,expires=0\r\n"+
		"\r\n"+
		"# Stats\r\n"+
		"total_commands_processed:%d\r\n"+
		"keyspace_hits:%d\r\n"+
		"keyspace_misses:%d\r\n"+
		"evicted_keys:%d\r\n"+
		"expired_keys:%d\r\n"+
		"\r\n"+
		"# Memory\r\n"+
		"used_memory:%d\r\n"+
		"used_memory_human:%s\r\n",
		stats["num_items"],
		stats["num_ops"],
		stats["num_hits"],
		stats["num_misses"],
		stats["num_evicted"],
		stats["num_expired"],
		stats["mem_used"],
		formatMemory(stats["mem_used"].(int64)))
	return result{resp: rvBulk(info), pg: pgRow("info", info, "INFO")}
}

func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}

	i, j := 0, 0
	for i < len(pattern) && j < len(key) {
		if pattern[i] == '*' {
			if i == len(pattern)-1 {
				return true
			}
			for j < len(key) {
				if matchPattern(pattern[i+1:], key[j:]) {
					return true
				}
				j++
			}
			return false
		} else if pattern[i] == '?' || pattern[i] == key[j] {
			i++
			j++
		} else {
			return false
		}
	}

	for i < len(pattern) && pattern[i] == '*' {
		i++
	}

	return i == len(pattern) && j == len(key)
}

func formatMemory(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
