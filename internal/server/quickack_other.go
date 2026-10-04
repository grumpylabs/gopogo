//go:build !linux

package server

import "net"

// setQuickAck is a no-op: TCP_QUICKACK is Linux only.
func setQuickAck(conn *net.TCPConn) {}
