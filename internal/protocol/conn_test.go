package protocol

import (
	"regexp"
	"strings"
	"testing"

	"github.com/grumpylabs/gopogo/internal/cache"
)

func TestDetect(t *testing.T) {
	tests := []struct {
		input string
		want  Type
	}{
		{"*1\r\n$4\r\nPING\r\n", TypeRedis},
		{"PING\r\n", TypeRedis},
		{"  SET k v\r\n", TypeRedis},
		{"\x00\x00\x00\x08\x04\xd2\x16\x2f", TypePostgres}, // SSLRequest
		{"\x00\x00\x00\x29\x00\x03\x00\x00user", TypePostgres},
		{"GET /key HTTP/1.1\r\nHost: x\r\n\r\n", TypeHTTP},
		{"PUT /key HTTP/1.0\r\n", TypeHTTP},
		{"OPTIONS * HTTP/1.1\r\n", TypeHTTP},
		{"get key\r\n", TypeMemcache},
		{"gets key\r\n", TypeMemcache},
		{"cas k 0 0 1 5\r\nx\r\n", TypeMemcache},
		{"append k 0 0 1\r\nx\r\n", TypeMemcache},
		{"touch k 10\r\n", TypeMemcache},
		{"quit\r\n", TypeMemcache},
	}
	for _, tt := range tests {
		got, ok := detect([]byte(tt.input))
		if !ok || got != tt.want {
			t.Errorf("%q: got %v, %v; want %v", tt.input, got, ok, tt.want)
		}
	}
	// A first line that has not ended is not enough, until it is 4 KiB.
	for _, in := range []string{"", "get ke", "GET /key HTTP/1.1"} {
		if got, ok := detect([]byte(in)); ok {
			t.Errorf("%q: detected %v before the line ended", in, got)
		}
	}
	if got, ok := detect([]byte(strings.Repeat("x", 4096))); !ok || got != TypeMemcache {
		t.Errorf("4 KiB without a line end: got %v, %v", got, ok)
	}
}

// splitInputs are request streams for each protocol whose replies do not
// depend on timing.
var splitInputs = []struct {
	name  string
	proto Type
	input string
}{
	{"resp", TypeRedis, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n" +
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" +
		"PING\r\n\r\nECHO  hi\r\n" +
		"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n" +
		"*3\r\n$4\r\nMSET\r\n$1\r\na\r\n$0\r\n\r\n" +
		"*2\r\n$6\r\nEXISTS\r\n$1\r\na\r\n" +
		"*1\r\n$5\r\nBOGUS\r\n" +
		"*-1\r\n" +
		"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"},
	{"memcache", TypeMemcache, "set k 5 0 5\r\nhello\r\n" +
		"get k\r\ngets k\r\n" +
		"append k 0 0 3\r\n abc\r\n" +
		"add k 0 0 1\r\nx\r\n" +
		"set n 0 0 2 noreply\r\n10\r\n" +
		"incr n 5\r\ndecr n 100\r\n" +
		"set bad x 0 1\r\n" +
		"delete k\r\nbogus\r\nget k n\r\n"},
	{"http", TypeHTTP, "PUT /k HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello" +
		"GET /k HTTP/1.1\r\nHost: x\r\n\r\n" +
		"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n" +
		"GET /c HTTP/1.1\r\nHost: x\r\n\r\n" +
		"DELETE /k HTTP/1.1\r\nHost: x\r\n\r\n" +
		"GET /k HTTP/1.1\r\nHost: x\r\n\r\n"},
}

// runSplit feeds input to a new connection chunk bytes at a time, as a
// transport would, and returns all output.
func runSplit(t *testing.T, proto Type, input string, chunk int) string {
	t.Helper()
	ch := cache.New(nil)
	h := &Handlers{
		Redis:    NewRedisHandler(ch, "", ""),
		HTTP:     NewHTTPHandler(ch, ""),
		Memcache: NewMemcacheHandler(ch, ""),
	}
	c := NewConn(h, "127.0.0.1:4000", false)
	c.proto = proto
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
				t.Fatalf("%s: action %v", input, act)
			}
			if n == 0 || len(in) == 0 {
				break
			}
		}
	}
	if len(in) != 0 {
		t.Fatalf("unconsumed input %q", in)
	}
	return dateLine.ReplaceAllString(string(out), "")
}

var dateLine = regexp.MustCompile(`Date: [^\r]*\r\n`)

// TestSplitInput checks that replies do not depend on how input is split
// into reads.
func TestSplitInput(t *testing.T) {
	for _, tt := range splitInputs {
		whole := runSplit(t, tt.proto, tt.input, len(tt.input))
		if whole == "" {
			t.Fatalf("%s: no output", tt.name)
		}
		for _, chunk := range []int{1, 2, 3, 7, 64} {
			if got := runSplit(t, tt.proto, tt.input, chunk); got != whole {
				t.Errorf("%s in %d-byte reads:\n%q\nwant\n%q", tt.name, chunk, got, whole)
			}
		}
	}
}
