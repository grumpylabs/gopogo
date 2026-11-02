package main

import (
	"bufio"
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestMemcacheOps runs every memcache op against gopogo's memcache handler,
// traced, so each command is preceded by a traceparent line. Only the
// deliberate error ops may fail, and only with a server error.
func TestMemcacheOps(t *testing.T) {
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider()) // samples everything
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	server, conn := net.Pipe()
	go protocol.NewMemcacheHandler(cache.New(nil), "").Handle(server)
	defer conn.Close()
	c := &client{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn), mc: true}

	const prefix = "t:"
	key := func(r *rand.Rand) string { return prefix + strconv.Itoa(r.IntN(3)) }
	value := func(r *rand.Rand) string { return strings.Repeat("v", 1+r.IntN(8)) }
	ttl := func(r *rand.Rand, lo, n int) string { return strconv.Itoa(lo + r.IntN(n)) }
	if _, err := c.do("set", prefix+"not-a-number", "0", "0", "abc"); err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewPCG(1, 2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Twice, so ops see both misses and hits (read-through, counter, session).
	for range 2 {
		for _, o := range memcacheOps(key, value, ttl, prefix) {
			err := o.run(ctx, r, c)
			deliberate := o.name == "bogus" || o.name == "bad-counter"
			switch {
			case deliberate && !errors.As(err, new(serverError)):
				t.Errorf("%s: got %v, want a server error", o.name, err)
			case !deliberate && err != nil:
				t.Errorf("%s: %v", o.name, err)
			}
		}
	}

	// The connection is still in step: a get after all that reads cleanly.
	if got, err := c.do("get", prefix+"not-a-number"); err != nil || got != "VALUE t:not-a-number 0 3" {
		t.Fatalf("final get: %q, %v", got, err)
	}
}

func TestReadMemcacheGets(t *testing.T) {
	c := &client{mc: true, r: bufio.NewReader(strings.NewReader(
		"VALUE a 0 3 7\r\nabc\r\nVALUE b 0 1 8\r\nx\r\nEND\r\nSTORED\r\n"))}
	if got, err := c.read(); err != nil || got != "VALUE a 0 3 7" {
		t.Fatalf("gets reply: %q, %v", got, err)
	}
	if got, err := c.read(); err != nil || got != "STORED" {
		t.Fatalf("next reply: %q, %v (gets data not fully consumed)", got, err)
	}
}
