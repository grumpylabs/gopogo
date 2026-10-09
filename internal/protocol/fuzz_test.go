package protocol

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// Fuzz targets feed arbitrary bytes to each protocol handler as one client
// connection. A panic or a hang fails the run. `go test -fuzz FuzzRESP
// ./internal/protocol` explores further; plain `go test` replays the seeds.

// fuzzConn serves fixed input and discards replies.
type fuzzConn struct{ r io.Reader }

func (c *fuzzConn) Read(b []byte) (int, error)       { return c.r.Read(b) }
func (c *fuzzConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *fuzzConn) Close() error                     { return nil }
func (c *fuzzConn) LocalAddr() net.Addr              { return fuzzAddr }
func (c *fuzzConn) RemoteAddr() net.Addr             { return fuzzAddr }
func (c *fuzzConn) SetDeadline(time.Time) error      { return nil }
func (c *fuzzConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fuzzConn) SetWriteDeadline(time.Time) error { return nil }

var fuzzAddr = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4000}

// fuzzSkip rejects inputs that would write files (SAVE, LOAD) or fill
// memory on purpose (DEBUG POPULATE) rather than exercise a parser.
func fuzzSkip(t *testing.T, data []byte) {
	upper := strings.ToUpper(string(data))
	for _, word := range []string{"SAVE", "LOAD", "POPULATE"} {
		if strings.Contains(upper, word) {
			t.Skip()
		}
	}
}

func fuzzHandle(t *testing.T, h connHandler, data []byte) {
	fuzzSkip(t, data)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(&fuzzConn{r: bytes.NewReader(data)})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not return for input %q", data)
	}
}

func FuzzRESP(f *testing.F) {
	for _, s := range []string{
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"*1\r\n$-1\r\nX\r\n",
		"*-5\r\n",
		"PING\r\nINCR k\r\nMGET a b c\r\n",
		"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n",
		"SCAN 0 MATCH * COUNT 5\r\nKEYS *\r\nMONITOR\r\nQUIT\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzHandle(t, NewRedisHandler(cache.New(nil), "", ""), data)
	})
}

func FuzzMemcache(f *testing.F) {
	for _, s := range []string{
		"set k 0 0 1\r\nv\r\nget k\r\n",
		"set k 0 0 -1\r\n",
		"cas k 0 0 1 5\r\nv\r\ngets k\r\nincr k 1\r\n",
		"append k 0 0 2\r\nab\r\ngat 10 k\r\ndelete k\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzHandle(t, NewMemcacheHandler(cache.New(nil), ""), data)
	})
}

func FuzzHTTP(f *testing.F) {
	for _, s := range []string{
		"GET /k HTTP/1.1\r\nHost: x\r\n\r\n",
		"PUT /k?ttl=10&nx HTTP/1.1\r\nContent-Length: 1\r\n\r\nv",
		"GET /@keys?match=* HTTP/1.1\r\n\r\nDELETE /k HTTP/1.1\r\n\r\n",
		"BREW / HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzHandle(t, NewHTTPHandler(cache.New(nil), ""), data)
	})
}

func FuzzPostgres(f *testing.F) {
	startup := "\x00\x00\x00\x0d\x00\x03\x00\x00user\x00\x00"
	for _, s := range []string{
		startup + "Q\x00\x00\x00\x0fGET k\x00",
		startup + "Q\x00\x00\x00\x14SET k v\x00X\x00\x00\x00\x04",
		startup + "P\x00\x00\x00\x10\x00GET $1\x00\x00\x00S\x00\x00\x00\x04",
		"\x00\x00\x00\x08\x04\xd2\x16\x2f",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzHandle(t, NewPostgresHandler(cache.New(nil), "", ""), data)
	})
}

// FuzzDetect covers the first bytes of every connection: detection, then
// whichever protocol they select.
func FuzzDetect(f *testing.F) {
	for _, s := range []string{"*1\r\n", "GET / HTTP/1.1\r\n", "get k\r\n", "\x00\x00\x00\x08\x04\xd2\x16\x2f", "\x16\x03\x01", ""} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzSkip(t, data)
		c := cache.New(nil)
		ServeConn(&fuzzConn{r: bytes.NewReader(data)}, &Handlers{
			Redis:    NewRedisHandler(c, "", ""),
			HTTP:     NewHTTPHandler(c, ""),
			Memcache: NewMemcacheHandler(c, ""),
			Postgres: NewPostgresHandler(c, "", ""),
		})
	})
}

// FuzzSplit checks that a connection's replies do not depend on how its
// input is split into reads, the property the event loops rely on. Inputs
// whose replies depend on time are skipped.
func FuzzSplit(f *testing.F) {
	for _, in := range splitInputs {
		if in.proto != TypeHTTP {
			f.Add(in.input)
		}
	}
	f.Fuzz(func(t *testing.T, input string) {
		fuzzSkip(t, []byte(input))
		upper := strings.ToUpper(input)
		for _, word := range []string{"EX", "PX", "TTL", "STAT", "INFO", "DEBUG", "MONITOR", "SWEEP", "PURGE", "TOUCH", "GAT", "SCAN", "KEYS", "FLUSH"} {
			if strings.Contains(upper, word) {
				t.Skip()
			}
		}
		whole := feed(input, len(input)+1)
		if bytewise := feed(input, 1); bytewise != whole {
			t.Fatalf("input %q\nwhole:    %q\nbytewise: %q", input, whole, bytewise)
		}
	})
}

// feed runs input through a new connection, chunk bytes per read, and
// returns its output and how it ended.
func feed(input string, chunk int) string {
	ch := cache.New(nil)
	c := NewConn(&Handlers{
		Redis:    NewRedisHandler(ch, "", ""),
		Memcache: NewMemcacheHandler(ch, ""),
	}, "127.0.0.1:4000", false)
	var in, out []byte
	for fed := 0; fed < len(input); {
		end := min(fed+chunk, len(input))
		in = append(in, input[fed:end]...)
		fed = end
		for {
			n, o, act := c.Process(in, out)
			out = o
			in = in[n:]
			if act != Continue {
				c.Close()
				return fmt.Sprintf("%s[action %d]", out, act)
			}
			if n == 0 || len(in) == 0 {
				break
			}
		}
	}
	return fmt.Sprintf("%s[unconsumed %d]", out, len(in))
}
