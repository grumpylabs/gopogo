//go:build linux || darwin

package server

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
)

// listenTCP opens a TCP listener with an explicit accept backlog and optional
// SO_REUSEPORT, neither of which net.Listen exposes.
func listenTCP(host string, port, backlog int, reusePort bool) (net.Listener, error) {
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	family := syscall.AF_INET6
	var sa syscall.Sockaddr
	if ip4 := addr.IP.To4(); ip4 != nil || addr.IP == nil {
		family = syscall.AF_INET
		sa4 := &syscall.SockaddrInet4{Port: addr.Port}
		copy(sa4.Addr[:], ip4)
		sa = sa4
	} else {
		sa6 := &syscall.SockaddrInet6{Port: addr.Port}
		copy(sa6.Addr[:], addr.IP.To16())
		sa = sa6
	}

	fd, err := syscall.Socket(family, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	ok := false
	defer func() {
		if !ok {
			syscall.Close(fd)
		}
	}()

	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return nil, os.NewSyscallError("setsockopt SO_REUSEADDR", err)
	}
	if reusePort {
		if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1); err != nil {
			return nil, os.NewSyscallError("setsockopt SO_REUSEPORT", err)
		}
	}
	if err := syscall.Bind(fd, sa); err != nil {
		return nil, fmt.Errorf("bind %s: %w", addr, err)
	}
	if err := syscall.Listen(fd, backlog); err != nil {
		return nil, os.NewSyscallError("listen", err)
	}

	// FileListener dups the descriptor, so the original is closed either way.
	f := os.NewFile(uintptr(fd), "tcp:"+addr.String())
	ok = true
	defer f.Close()
	return net.FileListener(f)
}
