package protocol

import (
	"bytes"
	"crypto/tls"
	"net"
)

// A client connection is a state machine over bytes: Process consumes
// complete requests from the input and appends replies to the output. It
// never blocks, so one goroutine per connection (ServeConn) and Linux epoll
// loops serving many connections per thread (internal/server) drive the
// same code. A connection that must block, Postgres and MONITOR, asks its
// transport for a net.Conn of its own and continues in Serve.

// Action tells a transport what to do after writing Process's output.
type Action int

const (
	// Continue reads more input; Process consumed every complete request,
	// or stopped early because the output reached outputLimit.
	Continue Action = iota
	// Close closes the connection.
	Close
	// Takeover passes the connection to Serve on a goroutine of its own.
	Takeover
)

// outputLimit is the reply size after which Process stops and lets the
// transport write, so a long pipeline of large replies is not held in
// memory at once.
const outputLimit = 64 << 10

// maxLine bounds a request line or header block that has no end yet.
const maxLine = 64 << 10

// readBufferSize is the input buffer for a connection served by its own
// goroutine.
const readBufferSize = 16 << 10

// Handlers are the handlers of the enabled protocols; a nil handler
// disables its protocol, and connections that speak it are closed.
type Handlers struct {
	Redis    *RedisHandler
	HTTP     *HTTPHandler
	Memcache *MemcacheHandler
	Postgres *PostgresHandler
}

// Conn is one client connection's protocol state.
type Conn struct {
	h     *Handlers
	addr  string
	tls   bool
	proto Type // TypeUnknown until the first bytes are seen

	redis   *session
	argv    [][]byte    // RESP parse scratch
	monitor chan string // MONITOR subscription, set before a Takeover
}

// NewConn returns the state for a new connection from addr. Its protocol is
// detected from its first bytes.
func NewConn(h *Handlers, addr string, isTLS bool) *Conn {
	return &Conn{h: h, addr: addr, tls: isTLS}
}

// Process handles the complete requests at the start of in, appending
// replies to out. It returns how many bytes of in it consumed and the
// extended out. When it returns Continue having consumed less than all of
// in, the rest is an incomplete request unless the output reached
// outputLimit; calling it again tells which.
func (c *Conn) Process(in, out []byte) (int, []byte, Action) {
	if c.proto == TypeUnknown {
		t, ok := detect(in)
		if !ok {
			return 0, out, Continue
		}
		c.proto = t
	}
	switch c.proto {
	case TypeRedis:
		if c.h.Redis != nil {
			return c.h.Redis.process(c, in, out)
		}
	case TypeHTTP:
		if c.h.HTTP != nil {
			return c.h.HTTP.process(c, in, out)
		}
	case TypeMemcache:
		if c.h.Memcache != nil {
			return c.h.Memcache.process(c, in, out)
		}
	case TypePostgres:
		if c.h.Postgres != nil {
			return 0, out, Takeover
		}
	}
	return 0, out, Close
}

// Serve continues a connection after Process returned Takeover, on the
// connection's own goroutine; in is the input Process did not consume.
func (c *Conn) Serve(nc net.Conn, in []byte) {
	switch {
	case c.monitor != nil:
		lines := c.monitor
		c.monitor = nil
		c.h.Redis.monitor(nc, in, lines)
	case c.proto == TypePostgres:
		c.h.Postgres.Handle(&prefixConn{Conn: nc, prefix: in})
	}
}

// Addr is the client's address.
func (c *Conn) Addr() string { return c.addr }

// Close releases what the connection holds when it closes without Serve.
func (c *Conn) Close() {
	if c.monitor != nil {
		monitors.unsubscribe(c.monitor)
		c.monitor = nil
	}
}

// detect sniffs the protocol from a connection's first bytes, using
// pogocache's rules: '*' is RESP, a zero byte is Postgres, a first line
// ending in " HTTP/x.y" is HTTP, a line starting with an uppercase letter
// is RESP inline (telnet) and any other line is Memcache. It needs the first
// line, or 4 KiB of it.
func detect(in []byte) (Type, bool) {
	if len(in) == 0 {
		return TypeUnknown, false
	}
	switch in[0] {
	case '*':
		return TypeRedis, true
	case 0:
		return TypePostgres, true
	}
	line := in
	if i := bytes.IndexByte(in, '\n'); i >= 0 {
		line = in[:i+1]
	} else if len(in) < 4096 {
		return TypeUnknown, false
	}
	if isHTTPRequestLine(line) {
		return TypeHTTP, true
	}
	line = bytes.TrimLeft(line, " ")
	if len(line) > 0 && line[0] >= 'A' && line[0] <= 'Z' {
		return TypeRedis, true
	}
	return TypeMemcache, true
}

// isHTTPRequestLine reports whether line ends in " HTTP/x.y\r\n".
func isHTTPRequestLine(line []byte) bool {
	n := len(line)
	return n >= 11 && bytes.Equal(line[n-11:n-5], []byte(" HTTP/")) &&
		line[n-4] == '.' && line[n-2] == '\r'
}

// ServeConn serves a connection on the calling goroutine, detecting its
// protocol, and closes it.
func ServeConn(nc net.Conn, h *Handlers) {
	_, isTLS := nc.(*tls.Conn)
	serve(nc, NewConn(h, nc.RemoteAddr().String(), isTLS))
}

// serve drives c from nc: read, process, write one reply batch, repeat.
func serve(nc net.Conn, c *Conn) {
	defer nc.Close()
	defer c.Close()
	in := inputBuffer{buf: make([]byte, readBufferSize)}
	var out []byte
	for {
		var act Action
		var err error
		out, act, err = processInput(nc, c, &in, out)
		switch {
		case err != nil || act == Close:
			return
		case act == Takeover:
			c.Serve(nc, in.data())
			return
		}
		in.makeRoom()
		n, err := nc.Read(in.free())
		in.end += n
		if n == 0 && err != nil {
			return
		}
	}
}

// processInput runs c over the buffered input, writing each batch of
// replies, until it needs more input or the connection changes hands.
func processInput(nc net.Conn, c *Conn, in *inputBuffer, out []byte) ([]byte, Action, error) {
	for {
		n, o, act := c.Process(in.data(), out[:0])
		out = o
		in.start += n
		if len(out) > 0 {
			if _, err := nc.Write(out); err != nil {
				return out, Close, err
			}
			if cap(out) > 2*outputLimit {
				out = nil
			}
		}
		if act != Continue || n == 0 || in.start == in.end {
			return out, act, nil
		}
	}
}

// inputBuffer holds a connection's unconsumed input, buf[start:end].
type inputBuffer struct {
	buf        []byte
	start, end int
}

func (b *inputBuffer) data() []byte { return b.buf[b.start:b.end] }
func (b *inputBuffer) free() []byte { return b.buf[b.end:] }

// makeRoom frees space to read into: it reuses the buffer from the start,
// moves a partial request to the front when little space is left after
// it, shrinks the buffer after a large request, or grows it for one.
func (b *inputBuffer) makeRoom() {
	switch {
	case b.start == b.end:
		b.start, b.end = 0, 0
		if len(b.buf) > 4*readBufferSize {
			b.buf = make([]byte, readBufferSize)
		}
	case b.start > 0 && len(b.buf)-b.end < readBufferSize/2:
		b.end = copy(b.buf, b.buf[b.start:b.end])
		b.start = 0
	}
	if b.end == len(b.buf) {
		grown := make([]byte, 2*len(b.buf))
		copy(grown, b.buf)
		b.buf = grown
	}
}

// prefixConn is a net.Conn whose reads return prefix first.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(b []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(b, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

// outBuf is an io.Writer that appends to a byte slice, for replies built
// with fmt or string writes.
type outBuf struct{ b []byte }

func (o *outBuf) Write(p []byte) (int, error) {
	o.b = append(o.b, p...)
	return len(p), nil
}

func (o *outBuf) WriteString(s string) (int, error) {
	o.b = append(o.b, s...)
	return len(s), nil
}

func (o *outBuf) WriteByte(c byte) error {
	o.b = append(o.b, c)
	return nil
}
