package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
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

	c.expect("5", "UINCRBY", "u", "5")
	c.expect("4", "UDECR", "u")
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
		[]interface{}{"7", "42", "1"},
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
	go NewMemcacheHandler(ch, "").Handle(server)
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
		go NewMemcacheHandler(ch, "").Handle(server)
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

func TestMemcacheAppendAtomic(t *testing.T) {
	ch := cache.New(nil)
	ch.Store([]byte("k"), []byte(""), &cache.StoreOptions{TTL: time.Hour, Flags: 9})

	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server, client := net.Pipe()
			go NewMemcacheHandler(ch, "").Handle(server)
			defer client.Close()
			r := bufio.NewReader(client)
			for i := 0; i < perWorker; i++ {
				client.Write([]byte("append k 0 0 1\r\nx\r\n"))
				if l, _ := r.ReadString('\n'); l != "STORED\r\n" {
					t.Errorf("append got %q", l)
					return
				}
			}
		}()
	}
	wg.Wait()

	e, _ := ch.Load([]byte("k"))
	if len(e.Value()) != workers*perWorker {
		t.Fatalf("lost appends: got %d bytes, want %d", len(e.Value()), workers*perWorker)
	}
	if e.Flags() != 9 || e.ExpireAt() == 0 {
		t.Fatalf("append dropped flags/TTL: flags=%d expireAt=%d", e.Flags(), e.ExpireAt())
	}

	server, client := net.Pipe()
	go NewMemcacheHandler(ch, "").Handle(server)
	defer client.Close()
	client.Write([]byte("prepend missing 0 0 1\r\nx\r\n"))
	if l, _ := bufio.NewReader(client).ReadString('\n'); l != "NOT_STORED\r\n" {
		t.Fatalf("prepend missing got %q", l)
	}
}

func TestRedisMonitor(t *testing.T) {
	ch := cache.New(nil)
	mon := newRESPClient(t, ch)
	mon.expect("OK", "MONITOR")

	c := newRESPClient(t, ch)
	c.expect("OK", "SET", "k", "a \"b\"\n")
	c.expect("OK", "AUTH", "") // not shown

	mon.conn.SetDeadline(time.Now().Add(5 * time.Second))
	line, err := mon.r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "+") || !strings.HasSuffix(line, `] "SET" "k" "a \"b\"\n"`+"\r\n") {
		t.Fatalf("unexpected monitor line %q", line)
	}

	// Commands from other protocols are shown too.
	server, client := net.Pipe()
	go NewMemcacheHandler(ch, "").Handle(server)
	defer client.Close()
	client.Write([]byte("get k\r\n"))
	if line, _ = mon.r.ReadString('\n'); !strings.HasSuffix(line, `"get" "k"`+"\r\n") {
		t.Fatalf("memcache command not monitored: %q", line)
	}

	mon.expect("OK", "QUIT")
}

func TestRedisDebug(t *testing.T) {
	ch := cache.New(nil)
	c := newRESPClient(t, ch)
	c.expect("OK", "DEBUG", "POPULATE", "1000", "test", "16")
	c.expect(int64(1000), "DBSIZE")
	if v := c.do("GET", "test:999"); v != strings.Repeat("\x00", 16) {
		t.Fatalf("populated value %q", v)
	}
	c.expect("OK", "DEBUG", "POPULATE", "10", "ex", "1", "100-200")
	if ttl := c.do("TTL", "ex:3").(int64); ttl < 99 || ttl > 200 {
		t.Fatalf("populate TTL %d out of range", ttl)
	}
	c.expect(errReply("ERR syntax error"), "DEBUG", "POPULATE", "x", "p", "1")
	c.expect(errReply("ERR unknown subcommand"), "DEBUG", "NOPE")
	if v, ok := c.do("DEBUG", "DETACH").(string); !ok || !strings.Contains(v, ":") {
		t.Fatalf("DEBUG DETACH got %#v", v)
	}
}

func TestMemcacheAuthRequired(t *testing.T) {
	ch := cache.New(nil)
	ch.Store([]byte("secret"), []byte("value"), nil)
	server, client := net.Pipe()
	go NewMemcacheHandler(ch, "s3cret").Handle(server)
	defer client.Close()
	r := bufio.NewReader(client)

	do := func(cmd string) string {
		t.Helper()
		client.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := client.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatal(err)
		}
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return line
	}
	const denied = "CLIENT_ERROR Authentication required\r\n"
	for _, cmd := range []string{
		"get secret",
		"set k 0 0 5\r\nhello", // data block is consumed, not read as a command
		"cas k 0 0 2 1\r\nhi",
		"delete secret",
		"incr n 1",
		"flush_all",
		"stats",
		"version",
	} {
		if got := do(cmd); got != denied {
			t.Fatalf("%q: got %q, want %q", cmd, got, denied)
		}
	}
	if _, found := ch.Load([]byte("k")); found {
		t.Fatal("unauthenticated set stored a value")
	}
	if _, found := ch.Load([]byte("secret")); !found {
		t.Fatal("unauthenticated delete removed a value")
	}
	client.Write([]byte("quit\r\n"))
	if _, err := r.ReadString('\n'); err == nil {
		t.Fatal("quit should close the connection")
	}
}

func TestMemcacheGATAndExptime(t *testing.T) {
	ch := cache.New(&cache.Options{UseCAS: true})
	server, client := net.Pipe()
	go NewMemcacheHandler(ch, "").Handle(server)
	defer client.Close()
	r := bufio.NewReader(client)

	send := func(cmd string) {
		t.Helper()
		client.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := client.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatal(err)
		}
	}
	// lines reads until END (or one line for other replies).
	reply := func(cmd string) []string {
		t.Helper()
		send(cmd)
		var out []string
		for {
			l, err := r.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			l = strings.TrimSuffix(l, "\r\n")
			out = append(out, l)
			// A non-VALUE first line is a one-line reply; otherwise read to END.
			if l == "END" || !strings.HasPrefix(out[0], "VALUE ") {
				return out
			}
		}
	}
	expireAt := func(key string) int64 {
		t.Helper()
		e, ok := ch.Load([]byte(key))
		if !ok {
			return -1 // missing or expired
		}
		return e.ExpireAt()
	}

	reply("set k 5 0 1\r\na")
	if got := reply("gat 100 k missing"); len(got) != 3 || got[0] != "VALUE k 5 1" || got[1] != "a" || got[2] != "END" {
		t.Fatalf("gat: %q", got)
	}
	if at := expireAt("k"); at <= time.Now().UnixNano() {
		t.Fatalf("gat 100 did not set a future expiry: %d", at)
	}
	got := reply("gats 0 k")
	if len(got) != 3 || !strings.HasPrefix(got[0], "VALUE k 5 1 ") {
		t.Fatalf("gats: %q", got)
	}
	if at := expireAt("k"); at != 0 {
		t.Fatalf("gats 0 should clear the expiry, got %d", at)
	}
	if got := reply("gat 100 missing"); len(got) != 1 || got[0] != "END" {
		t.Fatalf("gat missing: %q", got)
	}
	if got := reply("gat abc k"); got[0] != "CLIENT_ERROR bad command line format" {
		t.Fatalf("gat bad exptime: %q", got)
	}
	if got := reply("gat 100"); got[0] != "ERROR" {
		t.Fatalf("gat without keys: %q", got)
	}

	// Absolute Unix time more than 30 days ahead.
	future := time.Now().Add(40 * 24 * time.Hour).Unix()
	reply(fmt.Sprintf("touch k %d", future))
	if at := expireAt("k"); at != time.Unix(future, 0).UnixNano() {
		t.Fatalf("absolute touch: got %d, want %d", at, time.Unix(future, 0).UnixNano())
	}
	// Negative exptime expires at once.
	if got := reply("touch k -1"); got[0] != "TOUCHED" {
		t.Fatalf("touch -1: %q", got)
	}
	if expireAt("k") != -1 {
		t.Fatal("touch -1 should expire the key")
	}
	if got := reply("set n 0 -1 1\r\nx"); got[0] != "STORED" {
		t.Fatalf("set -1: %q", got)
	}
	if got := reply("get n"); len(got) != 1 || got[0] != "END" {
		t.Fatalf("set with negative exptime should be expired: %q", got)
	}

	if got := reply("verbosity 1"); got[0] != "OK" {
		t.Fatalf("verbosity: %q", got)
	}
	send("verbosity 1 noreply")
	if got := reply("version"); !strings.HasPrefix(got[0], "VERSION ") {
		t.Fatalf("verbosity noreply should not reply, next line was %q", got)
	}
	if got := reply("verbosity"); got[0] != "ERROR" {
		t.Fatalf("verbosity without level: %q", got)
	}
}

func TestNoEvictOutOfMemoryReplies(t *testing.T) {
	ch := cache.New(&cache.Options{NumShards: 1, MaxMemory: 2048, NoEvict: true})
	ch.Store([]byte("big"), make([]byte, 1800), nil)

	c := newRESPClient(t, ch)
	c.expect(errReply("ERR out of memory"), "SET", "k", strings.Repeat("v", 500))
	c.expect(errReply("ERR out of memory"), "APPEND", "big", strings.Repeat("v", 500))
	c.expect(errReply("ERR out of memory"), "MSET", "a", strings.Repeat("v", 500))

	server, client := net.Pipe()
	go NewMemcacheHandler(ch, "").Handle(server)
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	client.Write([]byte("set m 0 0 500\r\n" + strings.Repeat("v", 500) + "\r\n"))
	if l, _ := bufio.NewReader(client).ReadString('\n'); l != "SERVER_ERROR out of memory storing object\r\n" {
		t.Fatalf("memcache set when full: got %q", l)
	}
}
