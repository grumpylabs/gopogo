package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

type respClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func newRESPClient(t *testing.T, c *cache.Cache) *respClient {
	return newRESPClientAuth(t, c, "")
}

func newRESPClientAuth(t *testing.T, c *cache.Cache, auth string) *respClient {
	t.Helper()
	server, client := net.Pipe()
	go NewRedisHandler(c, auth, "").Handle(server)
	t.Cleanup(func() { client.Close() })
	return &respClient{t: t, conn: client, r: bufio.NewReader(client)}
}

// do sends a command and returns the reply: string for simple strings and
// bulk strings, int64 for integers, error for errors, nil for null, and
// []interface{} for arrays.
func (c *respClient) do(args ...string) interface{} {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write([]byte(b.String())); err != nil {
		c.t.Fatalf("write %v: %v", args, err)
	}
	return c.read()
}

func (c *respClient) read() interface{} {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '+':
		return line[1:]
	case '-':
		return fmt.Errorf("%s", line[1:])
	case ':':
		n, _ := strconv.ParseInt(line[1:], 10, 64)
		return n
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatal(err)
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		out := make([]interface{}, n)
		for i := range out {
			out[i] = c.read()
		}
		return out
	}
	c.t.Fatalf("unexpected reply line %q", line)
	return nil
}

func (c *respClient) expect(want interface{}, args ...string) {
	c.t.Helper()
	got := c.do(args...)
	if e, ok := got.(error); ok {
		got = "ERROR: " + e.Error()
	}
	if !reflect.DeepEqual(got, want) {
		c.t.Fatalf("%v: got %#v, want %#v", args, got, want)
	}
}

func errReply(msg string) string { return "ERROR: " + msg }

func TestRedisIncrDecr(t *testing.T) {
	c := newRESPClient(t, cache.New(nil))

	c.expect("OK", "SET", "x", "10")
	c.expect(int64(11), "INCR", "x")
	c.expect("11", "GET", "x")
	c.expect(int64(6), "DECRBY", "x", "5")
	c.expect(int64(-4), "INCRBY", "x", "-10")
	c.expect(int64(1), "INCR", "fresh")
	c.expect("1", "GET", "fresh")

	c.expect("OK", "SET", "s", "hello")
	c.expect(errReply("ERR value is not an integer or out of range"), "INCR", "s")
	c.expect(errReply("ERR value is not an integer or out of range"), "INCRBY", "x", "abc")

	c.expect("OK", "SET", "max", "9223372036854775807")
	c.expect(errReply("ERR increment or decrement would overflow"), "INCR", "max")
	c.expect(errReply("ERR increment or decrement would overflow"), "DECRBY", "x", "-9223372036854775808")

	c.expect(int64(5), "UINCRBY", "u", "5")
	c.expect(int64(4), "UDECR", "u")
	c.expect(errReply("ERR increment or decrement would overflow"), "UDECRBY", "u", "5")
	c.expect(errReply("ERR value is not an integer or out of range"), "UINCR", "x")
	c.expect(errReply("ERR wrong number of arguments for 'uincr' command"), "UINCR")
}

func TestRedisSetExAppend(t *testing.T) {
	c := newRESPClient(t, cache.New(nil))

	c.expect("OK", "SETEX", "k", "100", "v")
	if ttl := c.do("TTL", "k").(int64); ttl < 99 || ttl > 100 {
		t.Fatalf("SETEX ttl = %d", ttl)
	}
	c.expect(errReply("ERR invalid expire time"), "SETEX", "k", "0", "v")
	c.expect(errReply("ERR invalid expire time"), "SETEX", "k", "x", "v")
	c.expect(errReply("ERR wrong number of arguments for 'setex' command"), "SETEX", "k", "1")

	c.expect(int64(3), "APPEND", "a", "foo")
	c.expect(int64(6), "APPEND", "a", "bar")
	c.expect(int64(8), "PREPEND", "a", ">>")
	c.expect(">>foobar", "GET", "a")

	// APPEND keeps the existing TTL.
	c.expect(int64(2), "APPEND", "k", "w")
	c.expect("vw", "GET", "k")
	if ttl := c.do("TTL", "k").(int64); ttl < 99 {
		t.Fatalf("APPEND dropped TTL: %d", ttl)
	}
}

func TestRedisMGetS(t *testing.T) {
	ch := cache.New(&cache.Options{UseCAS: true})
	ch.Store([]byte("a"), []byte("1"), &cache.StoreOptions{Flags: 7, CAS: 42})
	c := newRESPClient(t, ch)

	c.expect([]interface{}{
		[]interface{}{int64(7), int64(42), "1"},
		nil,
	}, "MGETS", "a", "missing")
}

func TestRedisScan(t *testing.T) {
	ch := cache.New(&cache.Options{NumShards: 4})
	for i := 0; i < 50; i++ {
		ch.Store([]byte(fmt.Sprintf("user:%d", i)), []byte("v"), nil)
	}
	ch.Store([]byte("other"), []byte("v"), nil)
	c := newRESPClient(t, ch)

	seen := map[string]bool{}
	cursor := "0"
	for {
		reply := c.do("SCAN", cursor, "MATCH", "user:*", "COUNT", "7").([]interface{})
		keys := reply[1].([]interface{})
		if len(keys) > 7 {
			t.Fatalf("SCAN returned %d keys for COUNT 7", len(keys))
		}
		for _, k := range keys {
			seen[k.(string)] = true
		}
		cursor = reply[0].(string)
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 50 || seen["other"] {
		t.Fatalf("SCAN saw %d keys (other=%v), want 50", len(seen), seen["other"])
	}

	c.expect(errReply("ERR invalid cursor"), "SCAN", "abc")
	c.expect(errReply("ERR syntax error"), "SCAN", "0", "COUNT", "0")
	c.expect(errReply("ERR syntax error"), "SCAN", "0", "MATCH")
	c.expect(errReply("ERR unknown type name 'hash'"), "SCAN", "0", "TYPE", "hash")
}

func TestRedisAdminCommands(t *testing.T) {
	Version = "9.9.9"
	ch := cache.New(nil)
	c := newRESPClient(t, ch)

	c.expect("9.9.9", "VERSION")

	c.expect("OK", "SET", "a", "1")
	stats := c.do("STATS").([]interface{})
	found := map[string]string{}
	for _, row := range stats {
		kv := row.([]interface{})
		found[kv[0].(string)] = kv[1].(string)
	}
	if found["version"] != "9.9.9" || found["curr_items"] != "1" || found["product"] != "gopogo" {
		t.Fatalf("unexpected STATS: %v", found)
	}
	c.expect(errReply("ERR syntax error"), "STATS", "x")

	c.expect("OK", "SWEEP")
	c.expect("OK", "SWEEP", "ASYNC")
	c.expect(errReply("ERR syntax error"), "SWEEP", "bogus")
	c.expect("OK", "PURGE")
	c.expect(errReply("ERR syntax error"), "PURGE", "bogus")

	c.expect("OK", "FLUSH")
	c.expect(int64(0), "DBSIZE")
	c.expect("OK", "SET", "a", "1")
	c.expect("OK", "FLUSHALL", "SYNC")
	c.expect(int64(0), "DBSIZE")
	c.expect(errReply("ERR invalid delay argument"), "FLUSHDB", "DELAY", "-1")
	c.expect(errReply("ERR syntax error"), "FLUSHDB", "bogus")

	c.expect("OK", "SET", "a", "1")
	c.expect("OK", "FLUSHALL", "ASYNC")
	deadline := time.Now().Add(2 * time.Second)
	for ch.NumItems() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	c.expect(int64(0), "DBSIZE")
}

func TestRedisSelect(t *testing.T) {
	c := newRESPClient(t, cache.New(nil))
	c.expect("OK", "SELECT", "0")
	c.expect(errReply("ERR index is out of range"), "SELECT", "1")
	c.expect(errReply("ERR index is out of range"), "SELECT", "-1")
	c.expect(errReply("ERR value is not an integer or out of range"), "SELECT", "db")
	c.expect(errReply("ERR wrong number of arguments for 'select' command"), "SELECT")
}

func TestRedisAuth(t *testing.T) {
	c := newRESPClientAuth(t, cache.New(nil), "secret")
	c.expect(errReply("NOAUTH Authentication required."), "PING")
	c.expect(errReply("NOAUTH Authentication required."), "GET", "k")
	c.expect(errReply("WRONGPASS invalid username-password pair or user is disabled."), "AUTH", "nope")
	c.expect(errReply("WRONGPASS invalid username-password pair or user is disabled."), "AUTH", "default", "secret")
	c.expect(errReply("ERR syntax error"), "AUTH", "a", "b", "c")
	c.expect(errReply("ERR wrong number of arguments for 'auth' command"), "AUTH")
	c.expect("OK", "AUTH", "secret")
	c.expect("PONG", "PING")
}

func TestMemcacheIncrDecr(t *testing.T) {
	ch := cache.New(nil)
	server, client := net.Pipe()
	go NewMemcacheHandler(ch).Handle(server)
	defer client.Close()
	r := bufio.NewReader(client)

	do := func(cmd, want string) {
		t.Helper()
		client.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := client.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatal(err)
		}
		got, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if got != want+"\r\n" {
			t.Fatalf("%q: got %q, want %q", cmd, got, want)
		}
	}

	do("incr missing 1", "NOT_FOUND")
	do("set num 0 0 2\r\n10", "STORED")
	do("incr num 5", "15")
	do("decr num 20", "0")
	do("set s 0 0 5\r\nhello", "STORED")
	do("incr s 1", "CLIENT_ERROR cannot increment or decrement non-numeric value")
	do("incr num x", "CLIENT_ERROR invalid numeric delta argument")
	if e, _ := ch.Load([]byte("num")); string(e.Value()) != "0" {
		t.Fatalf("stored value %q, want \"0\"", e.Value())
	}
}

func TestMemcacheCAS(t *testing.T) {
	for _, useCAS := range []bool{true, false} {
		ch := cache.New(&cache.Options{UseCAS: useCAS})
		server, client := net.Pipe()
		go NewMemcacheHandler(ch).Handle(server)
		r := bufio.NewReader(client)

		send := func(cmd string) {
			client.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := client.Write([]byte(cmd + "\r\n")); err != nil {
				t.Fatal(err)
			}
		}
		line := func() string {
			l, err := r.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			return strings.TrimSuffix(l, "\r\n")
		}

		send("set k 0 0 1\r\na")
		line()
		send("gets k")
		fields := strings.Fields(line()) // VALUE k 0 1 <cas>
		line()                           // data
		line()                           // END
		token := fields[4]

		send("set k 0 0 1\r\nb") // another client's write
		line()
		send("cas k 0 0 1 " + token + "\r\nc")
		if got := line(); got != "EXISTS" {
			t.Fatalf("useCAS=%v: stale cas got %q, want EXISTS", useCAS, got)
		}
		send("cas missing 0 0 1 1\r\nc")
		if got := line(); got != "NOT_FOUND" {
			t.Fatalf("useCAS=%v: missing key cas got %q, want NOT_FOUND", useCAS, got)
		}
		if useCAS {
			send("gets k")
			token = strings.Fields(line())[4]
			line()
			line()
			send("cas k 0 0 1 " + token + "\r\nc")
			if got := line(); got != "STORED" {
				t.Fatalf("current cas got %q, want STORED", got)
			}
		}
		client.Close()
	}
}
