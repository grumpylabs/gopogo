package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// RedisHandler serves the RESP protocol (and RESP inline/telnet commands).
type RedisHandler struct {
	exec *Executor
}

// NewRedisHandler creates a Redis protocol handler. persist is the default
// path used by SAVE and LOAD; it may be empty.
func NewRedisHandler(cache *cache.Cache, auth, persist string) *RedisHandler {
	return &RedisHandler{exec: NewExecutor(cache, auth, persist)}
}

// Handle serves a connection known to speak RESP and closes it.
func (h *RedisHandler) Handle(conn net.Conn) {
	c := NewConn(&Handlers{Redis: h}, conn.RemoteAddr().String(), false)
	c.proto = TypeRedis
	serve(conn, c)
}

// process runs the complete commands at the start of in. A command that
// turns the connection into a MONITOR stream ends with a Takeover.
func (h *RedisHandler) process(c *Conn, in, out []byte) (int, []byte, Action) {
	if c.redis == nil {
		c.redis = &session{proto: TypeRedis, addr: c.addr, authed: h.exec.auth == ""}
	}
	pos := 0
	for pos < len(in) && len(out) < outputLimit {
		argv, n, err := c.scanRESP(in[pos:])
		if err != nil {
			return pos, appendRESP(out, rv{kind: '-', s: err.Error()}), Close
		}
		if n == 0 {
			break
		}
		pos += n
		if len(argv) == 0 {
			continue
		}
		if o, ok := h.fast(c, argv, out); ok {
			out = o
			continue
		}
		r := h.exec.exec(c.redis, argStrings(argv))
		if r.monitor {
			// Subscribe before replying so no command after the OK is missed.
			c.monitor = monitors.subscribe()
		}
		if r.err != "" {
			out = appendRESP(out, rv{kind: '-', s: r.err})
		} else {
			out = appendRESP(out, r.resp)
		}
		if r.quit {
			return pos, out, Close
		}
		if r.monitor {
			return pos, out, Takeover
		}
	}
	return pos, out, Continue
}

// monitor streams the subscribed command lines until the client sends QUIT
// or disconnects. Other input is ignored. in is input already read.
func (h *RedisHandler) monitor(nc net.Conn, in []byte, lines chan string) {
	defer monitors.unsubscribe(lines)

	quit := make(chan bool, 1) // true: client sent QUIT
	go func() {
		parser := &Conn{}
		buf := append([]byte(nil), in...)
		chunk := make([]byte, 4096)
		for {
			for {
				args, n, err := parser.parseRESP(buf)
				if err != nil {
					quit <- false
					return
				}
				if n == 0 {
					break
				}
				buf = buf[n:]
				if len(args) > 0 && strings.EqualFold(args[0], "QUIT") {
					quit <- true
					return
				}
			}
			n, err := nc.Read(chunk)
			if n == 0 && err != nil {
				quit <- false
				return
			}
			buf = append(buf, chunk[:n]...)
		}
	}()

	var out []byte
	for {
		select {
		case line := <-lines:
			out = append(out[:0], line...)
			// Batch whatever else is queued into one write.
			for n := len(lines); n > 0; n-- {
				out = append(out, <-lines...)
			}
			if _, err := nc.Write(out); err != nil {
				return
			}
		case sentQuit := <-quit:
			if sentQuit {
				nc.Write(appendRESP(nil, rvOK()))
			}
			return
		}
	}
}

// appendRESP appends v in RESP2. Unsigned values are written as simple
// strings, as pogocache does, since they may not fit a RESP integer.
func appendRESP(b []byte, v rv) []byte {
	switch v.kind {
	case '+', '-':
		b = append(b, v.kind)
		b = append(b, v.s...)
		return append(b, '\r', '\n')
	case ':':
		return appendRESPHeader(b, ':', v.n)
	case 'u':
		b = strconv.AppendUint(append(b, '+'), v.u, 10)
		return append(b, '\r', '\n')
	case '$':
		if v.b != nil {
			b = appendRESPHeader(b, '$', int64(len(v.b)))
			b = append(b, v.b...)
		} else {
			b = appendRESPHeader(b, '$', int64(len(v.s)))
			b = append(b, v.s...)
		}
		return append(b, '\r', '\n')
	case '_':
		return append(b, "$-1\r\n"...)
	case '*':
		b = appendRESPHeader(b, '*', int64(len(v.arr)))
		for _, e := range v.arr {
			b = appendRESP(b, e)
		}
		return b
	default:
		panic(fmt.Sprintf("appendRESP: unknown kind %q", v.kind))
	}
}

// appendRESPHeader appends a type byte, a number and CRLF.
func appendRESPHeader(b []byte, kind byte, n int64) []byte {
	b = strconv.AppendInt(append(b, kind), n, 10)
	return append(b, '\r', '\n')
}

// Limits on client-supplied lengths, as in Redis: a bad or hostile length
// gets an error instead of a huge or negative allocation.
const (
	maxArgs    = 1 << 20   // arguments in one command
	maxBulkLen = 512 << 20 // bytes in one argument or value
)

// readFull reads exactly n bytes. Memory grows with the bytes that arrive,
// so a client that claims a large length but sends little costs little.
func readFull(r io.Reader, n int) ([]byte, error) {
	const chunk = 64 << 10
	if n <= chunk {
		buf := make([]byte, n)
		_, err := io.ReadFull(r, buf)
		return buf, err
	}
	var b bytes.Buffer
	b.Grow(chunk)
	if _, err := io.CopyN(&b, r, int64(n)); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	return b.Bytes(), nil
}

// parseLen parses a RESP length: an optional minus sign and decimal digits.
// It does not allocate, unlike strconv.Atoi on a converted string.
func parseLen(b []byte) (int, error) {
	digits := b
	if len(digits) > 0 && digits[0] == '-' {
		digits = digits[1:]
	}
	if len(digits) == 0 || len(digits) > 18 {
		return 0, errInvalidLen
	}
	n := 0
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, errInvalidLen
		}
		n = n*10 + int(c-'0')
	}
	if len(digits) < len(b) {
		n = -n
	}
	return n, nil
}

var (
	errInvalidLen      = errors.New("invalid length")
	errMultibulkLen    = errors.New("invalid multibulk length")
	errBulkLen         = errors.New("invalid bulk length")
	errExpectedBulk    = errors.New("expected bulk string")
	errTooBigInline    = errors.New("too big inline request")
	errTooBigMultibulk = errors.New("too big multibulk request")
)

// parseRESP parses one command at the start of b: a RESP array of bulk
// strings, or an inline command line. It returns the arguments and the
// bytes consumed, or 0 consumed when b holds only part of a command. A
// blank line is consumed with no arguments.
func (c *Conn) parseRESP(b []byte) ([]string, int, error) {
	argv, n, err := c.scanRESP(b)
	if err != nil || n == 0 {
		return nil, n, err
	}
	return argStrings(argv), n, nil
}

// scanRESP is parseRESP without copying: the arguments are slices of b,
// valid until the next call.
func (c *Conn) scanRESP(b []byte) ([][]byte, int, error) {
	clear(c.argv)
	argv := c.argv[:0]
	i := bytes.IndexByte(b, '\n')
	if i < 0 {
		if len(b) > maxLine {
			return nil, 0, errTooBigInline
		}
		return nil, 0, nil
	}
	line := bytes.TrimSpace(b[:i+1])
	if len(line) == 0 {
		return argv, i + 1, nil
	}
	if line[0] != '*' {
		c.argv = append(argv, bytes.Fields(line)...)
		return c.argv, i + 1, nil
	}
	count, err := parseLen(line[1:])
	if err != nil || count > maxArgs {
		return nil, 0, errMultibulkLen
	}
	pos := i + 1
	for k := 0; k < count; k++ {
		j := bytes.IndexByte(b[pos:], '\n')
		if j < 0 {
			if len(b)-pos > maxLine {
				return nil, 0, errTooBigMultibulk
			}
			c.argv = argv
			return nil, 0, nil
		}
		line := bytes.TrimSpace(b[pos : pos+j+1])
		if len(line) == 0 || line[0] != '$' {
			return nil, 0, errExpectedBulk
		}
		size, err := parseLen(line[1:])
		if err != nil || size < 0 || size > maxBulkLen {
			return nil, 0, errBulkLen
		}
		start := pos + j + 1
		if len(b) < start+size+2 {
			c.argv = argv
			return nil, 0, nil
		}
		argv = append(argv, b[start:start+size])
		pos = start + size + 2
	}
	c.argv = argv
	return argv, pos, nil
}

// argStrings copies arguments to strings. The name gets its own string,
// since telemetry may keep it; the others share one allocation.
func argStrings(argv [][]byte) []string {
	args := make([]string, len(argv))
	if len(argv) == 0 {
		return args
	}
	args[0] = string(argv[0])
	n := 0
	for _, a := range argv[1:] {
		n += len(a)
	}
	var sb strings.Builder
	sb.Grow(n)
	for _, a := range argv[1:] {
		sb.Write(a)
	}
	all, off := sb.String(), 0
	for k, a := range argv[1:] {
		args[k+1] = all[off : off+len(a)]
		off += len(a)
	}
	return args
}
