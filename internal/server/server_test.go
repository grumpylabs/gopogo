package server

import (
	"net"
	"testing"

	"github.com/grumpylabs/gopogo/internal/cache"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// panicConn panics on its first read, standing in for a parser bug.
type panicConn struct {
	net.Conn
	closed bool
}

func (c *panicConn) Read([]byte) (int, error) { panic("parser bug") }
func (c *panicConn) Close() error             { c.closed = true; return nil }
func (c *panicConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 5555}
}

func TestPanicClosesOnlyTheConnection(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	s := New(&Config{Redis: true, Cache: cache.New(nil)})
	conn := &panicConn{}
	// Admit first, as serve does: handleConnection releases the connection,
	// and an unmatched release leaves the global open-connection count at -1.
	if !s.admit(conn) {
		t.Fatal("connection not admitted")
	}
	s.handleConnection(conn) // must return, not panic

	if !conn.closed {
		t.Error("connection was not closed")
	}
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("got %d error logs, want 1", len(entries))
	}
	if got := entries[0].ContextMap()["panic"]; got != "parser bug" {
		t.Errorf("panic field = %v", got)
	}
}
