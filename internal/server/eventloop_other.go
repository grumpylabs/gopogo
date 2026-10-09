//go:build !linux

package server

import (
	"net"

	"github.com/grumpylabs/gopogo/internal/protocol"
)

// Event loops use epoll and run on Linux only; elsewhere every connection
// has its own goroutine.
type eventLoops struct{}

func newEventLoops(int, *protocol.Handlers) (*eventLoops, error) { return nil, errNoEventLoops }

func (*eventLoops) supported(net.Conn) bool    { return false }
func (*eventLoops) add(net.Conn, func()) error { return errNoEventLoops }
func (*eventLoops) close()                     {}
