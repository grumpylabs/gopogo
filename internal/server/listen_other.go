//go:build !linux && !darwin

package server

import (
	"net"
	"strconv"
)

// listenTCP falls back to net.Listen, which uses the system default backlog
// and cannot set SO_REUSEPORT.
func listenTCP(host string, port, backlog int, reusePort bool) (net.Listener, error) {
	return net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
}
