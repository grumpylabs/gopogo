package server

import (
	"net"
	"syscall"
)

// setQuickAck enables TCP_QUICKACK on conn (Linux only).
func setQuickAck(conn *net.TCPConn) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return
	}
	raw.Control(func(fd uintptr) {
		syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_QUICKACK, 1)
	})
}
