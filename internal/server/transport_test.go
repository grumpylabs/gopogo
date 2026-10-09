package server

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
)

// These tests run real connections through the server's transport: epoll
// event loops on Linux, a goroutine per connection elsewhere.

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startServer starts a server with every protocol on a free port, changed
// by configure if given, and returns its address.
func startServer(t *testing.T, configure ...func(*Config)) string {
	t.Helper()
	port := freePort(t)
	cfg := &Config{
		Host: "127.0.0.1", Port: port, Backlog: 128, TCPNoDelay: true, Quiet: true,
		Redis: true, HTTP: true, Memcache: true, Postgres: true,
		Cache: cache.New(nil), EventLoops: true,
	}
	for _, f := range configure {
		f(cfg)
	}
	s := New(cfg)
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for i := 0; ; i++ {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			break
		}
		if i == 100 {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	loops := s.loops
	s.mu.Unlock()
	if runtime.GOOS == "linux" && loops == nil {
		t.Fatal("no event loops on Linux")
	}
	t.Cleanup(func() {
		s.Stop()
		<-done
	})
	return addr
}

type respClient struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func dialRESP(t *testing.T, addr string) *respClient {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(30 * time.Second))
	return &respClient{t: t, c: c, r: bufio.NewReader(c)}
}

func respCmd(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

func (c *respClient) send(args ...string) {
	c.t.Helper()
	if _, err := c.c.Write(respCmd(args...)); err != nil {
		c.t.Fatal(err)
	}
}

// reply reads one reply: a status or error line, or a bulk string's value.
func (c *respClient) reply() string {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatal(err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line[0] != '$' || line == "$-1" {
		return line
	}
	var n int
	fmt.Sscanf(line[1:], "%d", &n)
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		c.t.Fatal(err)
	}
	return string(buf[:n])
}

func TestTransportPipelinedLargeReplies(t *testing.T) {
	c := dialRESP(t, startServer(t))
	value := strings.Repeat("v", 1<<20)
	c.send("SET", "big", value)
	if got := c.reply(); got != "+OK" {
		t.Fatalf("SET: %q", got)
	}
	// 32 MiB of replies queued before the client reads any: the server must
	// hold back and resume as the socket drains.
	const n = 32
	var batch []byte
	for i := 0; i < n; i++ {
		batch = append(batch, respCmd("GET", "big")...)
	}
	batch = append(batch, respCmd("PING")...)
	go c.c.Write(batch)
	for i := 0; i < n; i++ {
		if got := c.reply(); got != value {
			t.Fatalf("GET %d: %d bytes", i, len(got))
		}
	}
	if got := c.reply(); got != "+PONG" {
		t.Fatalf("PING: %q", got)
	}
}

func TestTransportCommandSplitAcrossWrites(t *testing.T) {
	c := dialRESP(t, startServer(t))
	cmd := respCmd("SET", "k", strings.Repeat("x", 300000))
	for i := 0; i < len(cmd); i += 1000 {
		c.c.Write(cmd[i:min(i+1000, len(cmd))])
		if i == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if got := c.reply(); got != "+OK" {
		t.Fatalf("SET: %q", got)
	}
	c.send("STRLEN", "k")
	c.send("GET", "k")
	c.reply() // STRLEN is unknown to gopogo; any reply will do
	if got := c.reply(); len(got) != 300000 {
		t.Fatalf("GET: %d bytes", len(got))
	}
}

func TestTransportConcurrentClients(t *testing.T) {
	addr := startServer(t)
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dialRESP(t, addr)
			for i := 0; i < 200; i++ {
				key, val := fmt.Sprintf("k%d:%d", g, i), fmt.Sprintf("v%d", i)
				c.send("SET", key, val)
				c.send("GET", key)
				if got := c.reply(); got != "+OK" {
					t.Errorf("SET: %q", got)
					return
				}
				if got := c.reply(); got != val {
					t.Errorf("GET %s: %q", key, got)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestTransportQuitAndHalfClose(t *testing.T) {
	addr := startServer(t)
	c := dialRESP(t, addr)
	c.send("PING")
	c.c.(*net.TCPConn).CloseWrite()
	if got := c.reply(); got != "+PONG" {
		t.Fatalf("PING: %q", got)
	}
	if _, err := c.r.ReadByte(); err != io.EOF {
		t.Fatalf("after half close: %v, want EOF", err)
	}

	c = dialRESP(t, addr)
	c.send("QUIT")
	if got := c.reply(); got != "+OK" {
		t.Fatalf("QUIT: %q", got)
	}
	if _, err := c.r.ReadByte(); err != io.EOF {
		t.Fatalf("after QUIT: %v, want EOF", err)
	}

	c = dialRESP(t, addr)
	c.c.Write([]byte("*1\r\n$-1\r\n"))
	if got := c.reply(); got != "-invalid bulk length" {
		t.Fatalf("bad length: %q", got)
	}
}

func TestTransportMonitor(t *testing.T) {
	addr := startServer(t)
	mon := dialRESP(t, addr)
	mon.send("MONITOR")
	if got := mon.reply(); got != "+OK" {
		t.Fatalf("MONITOR: %q", got)
	}
	c := dialRESP(t, addr)
	c.send("SET", "watched", "1")
	c.reply()
	if got := mon.reply(); !strings.HasSuffix(got, `"SET" "watched" "1"`) {
		t.Fatalf("monitor line: %q", got)
	}
	mon.send("QUIT")
	if got := mon.reply(); got != "+OK" {
		t.Fatalf("QUIT: %q", got)
	}
}

func TestTransportOtherProtocols(t *testing.T) {
	addr := startServer(t)
	c := dialRESP(t, addr)
	c.send("SET", "shared", "value")
	c.reply()

	resp, err := http.Get("http://" + addr + "/shared")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "value" {
		t.Fatalf("HTTP GET: %d %q", resp.StatusCode, body)
	}

	m := dialRESP(t, addr)
	m.c.Write([]byte("get shared\r\n"))
	for _, want := range []string{"VALUE shared 0 5", "value", "END"} {
		line, err := m.r.ReadString('\n')
		if err != nil || strings.TrimSuffix(line, "\r\n") != want {
			t.Fatalf("memcache: %q, %v; want %q", line, err, want)
		}
	}

	// Postgres moves to a goroutine of its own: a startup message gets
	// AuthenticationOk.
	p := dialRESP(t, addr)
	startup := []byte{0, 0, 0, 0, 0, 3, 0, 0}
	startup = append(startup, "user\x00test\x00\x00"...)
	binary.BigEndian.PutUint32(startup, uint32(len(startup)))
	p.c.Write(startup)
	var hdr [9]byte
	if _, err := io.ReadFull(p.r, hdr[:]); err != nil || hdr[0] != 'R' || binary.BigEndian.Uint32(hdr[5:]) != 0 {
		t.Fatalf("postgres: %q, %v", hdr, err)
	}
}

func TestTransportReleasesConnections(t *testing.T) {
	addr := startServer(t)
	before := protocol.ConnStats.Curr.Load()
	for i := 0; i < 20; i++ {
		c := dialRESP(t, addr)
		c.send("PING")
		c.reply()
		c.c.Close()
	}
	deadline := time.Now().Add(5 * time.Second)
	for protocol.ConnStats.Curr.Load() != before {
		if time.Now().After(deadline) {
			t.Fatalf("open connections %d, want %d", protocol.ConnStats.Curr.Load(), before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTransportTLS checks that TLS connections, which event loops do not
// serve, work beside plain ones.
func TestTransportTLS(t *testing.T) {
	certFile, keyFile := writeTestCert(t)
	tlsPort := freePort(t)
	addr := startServer(t, func(c *Config) {
		c.TLSPort, c.TLSCert, c.TLSKey = tlsPort, certFile, keyFile
	})
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", tlsPort), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := &respClient{t: t, c: conn, r: bufio.NewReader(conn)}
	c.send("SET", "secure", "yes")
	c.send("GET", "secure")
	if got := c.reply(); got != "+OK" {
		t.Fatalf("SET: %q", got)
	}
	if got := c.reply(); got != "yes" {
		t.Fatalf("GET: %q", got)
	}
	plain := dialRESP(t, addr)
	plain.send("GET", "secure")
	if got := plain.reply(); got != "yes" {
		t.Fatalf("plain GET: %q", got)
	}
}

// writeTestCert writes a self-signed certificate for 127.0.0.1 and its key.
func writeTestCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gopogo test"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
