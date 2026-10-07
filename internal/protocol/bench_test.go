package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// Server-path benchmarks: a client on loopback TCP drives a handler through
// read, parse, execute and write, the same path as a real connection.

const benchKeys = 10000

type connHandler interface{ Handle(net.Conn) }

// benchServer serves h on a loopback listener for the life of the benchmark.
func benchServer(b *testing.B, h connHandler) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go h.Handle(conn)
		}
	}()
	return ln.Addr().String()
}

func benchCache(b *testing.B, value []byte) *cache.Cache {
	b.Helper()
	c := cache.New(nil)
	for i := 0; i < benchKeys; i++ {
		c.Store([]byte(fmt.Sprintf("key:%d", i)), value, nil)
	}
	return c
}

func respCommand(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	return sb.String()
}

// skipRESP reads one RESP reply without decoding it.
func skipRESP(r *bufio.Reader) error {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if line[0] == '$' && line[1] != '-' {
		var n int
		fmt.Sscanf(string(line[1:]), "%d", &n)
		_, err = r.Discard(n + 2)
	}
	return err
}

// benchRESP sends batches of pipeline commands per connection, one
// connection per parallel goroutine, and reads every reply.
func benchRESP(b *testing.B, cmd string, size, pipeline int) {
	value := []byte(strings.Repeat("v", size))
	addr := benchServer(b, NewRedisHandler(benchCache(b, value), "", ""))
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key:%d", i%benchKeys)
			i++
			if cmd == "SET" {
				io.WriteString(w, respCommand("SET", key, string(value)))
			} else {
				io.WriteString(w, respCommand("GET", key))
			}
			if i%pipeline != 0 {
				continue
			}
			if err := w.Flush(); err != nil {
				b.Error(err)
				return
			}
			for j := 0; j < pipeline; j++ {
				if err := skipRESP(r); err != nil {
					b.Error(err)
					return
				}
			}
		}
		// Drain a partial final batch.
		if n := i % pipeline; n != 0 {
			w.Flush()
			for j := 0; j < n; j++ {
				skipRESP(r)
			}
		}
	})
}

func BenchmarkRESP(b *testing.B) {
	for _, cmd := range []string{"GET", "SET"} {
		for _, size := range []int{64, 1024} {
			for _, pipeline := range []int{1, 16} {
				name := fmt.Sprintf("%s/%dB/P%d", cmd, size, pipeline)
				b.Run(name, func(b *testing.B) { benchRESP(b, cmd, size, pipeline) })
			}
		}
	}
}

func BenchmarkHTTPGet(b *testing.B) {
	value := []byte(strings.Repeat("v", 64))
	addr := benchServer(b, NewHTTPHandler(benchCache(b, value), ""))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		i := 0
		for pb.Next() {
			fmt.Fprintf(conn, "GET /key:%d HTTP/1.1\r\nHost: bench\r\n\r\n", i%benchKeys)
			i++
			if err := skipHTTP(r); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// skipHTTP reads one response, headers and Content-Length body.
func skipHTTP(r *bufio.Reader) error {
	length := 0
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if len(line) <= 2 {
			break
		}
		if n, _ := fmt.Sscanf(strings.ToLower(string(line)), "content-length: %d", &length); n == 1 {
			continue
		}
	}
	_, err := r.Discard(length)
	return err
}

func BenchmarkMemcacheGet(b *testing.B) {
	value := []byte(strings.Repeat("v", 64))
	addr := benchServer(b, NewMemcacheHandler(benchCache(b, value), ""))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		i := 0
		for pb.Next() {
			fmt.Fprintf(conn, "get key:%d\r\n", i%benchKeys)
			i++
			for {
				line, err := r.ReadSlice('\n')
				if err != nil {
					b.Error(err)
					return
				}
				if string(line) == "END\r\n" {
					break
				}
			}
		}
	})
}
