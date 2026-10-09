//go:build linux

package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/grumpylabs/gopogo/internal/protocol"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// Plain TCP and Unix socket connections are served by event loops, as in
// pogocache: each loop is one OS thread waiting in epoll for any of its
// connections to become readable, then reading, processing and replying
// to each. Go's own poller instead parks and wakes a goroutine per request,
// which costs a failed read and scheduler work every time. Connections that
// must block (TLS, Postgres, MONITOR) run on goroutines.

// eventLoops spreads connections over its loops round robin.
type eventLoops struct {
	loops []*eventLoop
	next  atomic.Uint32
	wg    sync.WaitGroup
}

// eventLoop serves the connections registered with its epoll instance.
type eventLoop struct {
	epfd     int
	wakefd   int // eventfd that wakes the loop for new connections or stop
	handlers *protocol.Handlers
	buf      []byte // read buffer shared by the loop's connections

	mu      sync.Mutex
	pending []*loopConn // accepted, not yet registered
	stop    bool

	conns map[int]*loopConn // owned by the loop goroutine
}

// loopConn is one connection served by a loop.
type loopConn struct {
	fd      int
	pc      *protocol.Conn
	in      []byte // unconsumed input
	out     []byte // unsent output
	sent    int    // bytes of out already written
	writing bool   // waiting for the socket to accept more output
	after   protocol.Action
	release func()
}

const (
	loopEvents   = 256
	loopReadSize = 64 << 10
	connIdleKeep = 64 << 10 // larger idle buffers are freed
)

// newEventLoops starts n loops.
func newEventLoops(n int, h *protocol.Handlers) (*eventLoops, error) {
	e := &eventLoops{}
	for i := 0; i < n; i++ {
		l, err := newEventLoop(h)
		if err != nil {
			e.close()
			return nil, err
		}
		e.loops = append(e.loops, l)
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			l.run()
		}()
	}
	return e, nil
}

func newEventLoop(h *protocol.Handlers) (*eventLoop, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, os.NewSyscallError("epoll_create1", err)
	}
	wakefd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		unix.Close(epfd)
		return nil, os.NewSyscallError("eventfd", err)
	}
	ev := unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(wakefd)}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, wakefd, &ev); err != nil {
		unix.Close(epfd)
		unix.Close(wakefd)
		return nil, os.NewSyscallError("epoll_ctl", err)
	}
	return &eventLoop{
		epfd:     epfd,
		wakefd:   wakefd,
		handlers: h,
		buf:      make([]byte, loopReadSize),
		conns:    map[int]*loopConn{},
	}, nil
}

// supported reports whether nc can be served by a loop: a plain TCP or
// Unix socket connection.
func (e *eventLoops) supported(nc net.Conn) bool {
	switch nc.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	}
	return false
}

// add moves nc's socket to a loop. release runs when the connection ends.
// nc itself is closed: the loop keeps a duplicate of its descriptor. On
// error the connection is closed and released.
func (e *eventLoops) add(nc net.Conn, release func()) error {
	addr := ""
	if a := nc.RemoteAddr(); a != nil {
		addr = a.String()
	}
	fd, err := dupConn(nc)
	// Go's poller forgets the socket on Close; the duplicate stays open and
	// nonblocking, since descriptors share that flag.
	nc.Close()
	if err != nil {
		release()
		return err
	}

	l := e.loops[e.next.Add(1)%uint32(len(e.loops))]
	c := &loopConn{fd: fd, pc: protocol.NewConn(l.handlers, addr, false), release: release}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stop {
		unix.Close(fd)
		release()
		return errors.New("event loop stopped")
	}
	l.pending = append(l.pending, c)
	l.wake()
	return nil
}

// dupConn returns a duplicate of nc's descriptor.
func dupConn(nc net.Conn) (int, error) {
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return -1, errors.New("connection has no descriptor")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1, err
	}
	fd := -1
	var dupErr error
	if err := raw.Control(func(s uintptr) {
		fd, dupErr = unix.FcntlInt(s, unix.F_DUPFD_CLOEXEC, 0)
	}); err != nil {
		return -1, err
	}
	if dupErr != nil {
		return -1, os.NewSyscallError("fcntl", dupErr)
	}
	return fd, nil
}

// close stops the loops, closing their connections, and waits for them.
func (e *eventLoops) close() {
	for _, l := range e.loops {
		l.mu.Lock()
		l.stop = true
		l.wake()
		l.mu.Unlock()
	}
	e.wg.Wait()
}

// wake interrupts the loop's epoll wait. l.mu is held, so the loop has not
// closed the eventfd.
func (l *eventLoop) wake() {
	if l.wakefd >= 0 {
		one := [8]byte{1}
		unix.Write(l.wakefd, one[:])
	}
}

func (l *eventLoop) run() {
	// The loop owns its thread, as a C event loop does: no other goroutine
	// runs on it between epoll waits.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer func() {
		for _, c := range l.conns {
			l.closeConn(c)
		}
		l.mu.Lock()
		for _, c := range l.pending {
			unix.Close(c.fd)
			c.release()
		}
		l.pending = nil
		unix.Close(l.wakefd)
		l.wakefd = -1
		l.mu.Unlock()
		unix.Close(l.epfd)
	}()

	wakefd := l.wakefd // changes only when the loop exits
	events := make([]unix.EpollEvent, loopEvents)
	for {
		n, err := unix.EpollWait(l.epfd, events, -1)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			zap.L().Error("event loop failed: "+err.Error(), zap.Error(err))
			return
		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			fd := int(ev.Fd)
			if fd == wakefd {
				if l.register() {
					return
				}
				continue
			}
			c := l.conns[fd]
			if c == nil {
				continue
			}
			if c.writing {
				if ev.Events&(unix.EPOLLOUT|unix.EPOLLERR|unix.EPOLLHUP) != 0 {
					l.resume(c)
				}
				continue
			}
			l.read(c)
		}
	}
}

// register adds pending connections to the loop. It reports whether the
// loop should stop.
func (l *eventLoop) register() bool {
	var b [8]byte
	l.mu.Lock()
	unix.Read(l.wakefd, b[:])
	pending, stop := l.pending, l.stop
	l.pending = nil
	l.mu.Unlock()
	for _, c := range pending {
		ev := unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(c.fd)}
		if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, c.fd, &ev); err != nil {
			unix.Close(c.fd)
			c.release()
			continue
		}
		l.conns[c.fd] = c
	}
	return stop
}

// read reads what the socket has and processes it.
func (l *eventLoop) read(c *loopConn) {
	var n int
	var err error
	for {
		n, err = unix.Read(c.fd, l.buf)
		if err != unix.EINTR {
			break
		}
	}
	if err == unix.EAGAIN {
		return
	}
	if n <= 0 {
		l.closeConn(c)
		return
	}
	data := l.buf[:n]
	if len(c.in) > 0 {
		c.in = append(c.in, data...)
		data = c.in
	}
	l.process(c, data)
}

// process runs the protocol over data and writes the output. Input that
// remains, a partial request or requests held back while output waits, is
// kept in c.in.
func (l *eventLoop) process(c *loopConn, data []byte) {
	for {
		n, out, act, ok := l.safeProcess(c, data)
		if !ok {
			return
		}
		data = data[n:]
		c.out, c.after = out, act
		if len(c.out) > 0 && !l.flush(c) {
			// The socket is full: keep the rest of the input until the
			// output drains.
			l.keep(c, data)
			return
		}
		if l.finish(c, data) || n == 0 || len(data) == 0 {
			if l.conns[c.fd] == c {
				l.keep(c, data)
			}
			return
		}
	}
}

// safeProcess runs Process, closing the connection if it panics.
func (l *eventLoop) safeProcess(c *loopConn, data []byte) (n int, out []byte, act protocol.Action, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			logPanic(c.pc.Addr(), r)
			l.closeConn(c)
			ok = false
		}
	}()
	n, out, act = c.pc.Process(data, c.out[:0])
	return n, out, act, true
}

// finish acts on Close or Takeover once the output is written. It reports
// whether the connection left the loop.
func (l *eventLoop) finish(c *loopConn, rest []byte) bool {
	switch c.after {
	case protocol.Close:
		l.closeConn(c)
		return true
	case protocol.Takeover:
		l.takeover(c, rest)
		return true
	}
	return false
}

// keep saves unconsumed input, which may be in the loop's shared buffer.
func (l *eventLoop) keep(c *loopConn, data []byte) {
	if len(data) == 0 {
		if cap(c.in) > connIdleKeep {
			c.in = nil
		} else {
			c.in = c.in[:0]
		}
		return
	}
	// data is a suffix of c.in or of l.buf; append copies it to the front
	// of c.in either way (an overlapping copy moves down safely).
	c.in = append(c.in[:0], data...)
}

// flush writes pending output. It reports whether all of it was written;
// if not, the loop waits for the socket to become writable (or the
// connection was closed).
func (l *eventLoop) flush(c *loopConn) bool {
	for c.sent < len(c.out) {
		n, err := unix.Write(c.fd, c.out[c.sent:])
		if n > 0 {
			c.sent += n
			continue
		}
		switch err {
		case unix.EINTR:
			continue
		case unix.EAGAIN:
			if !c.writing {
				c.writing = true
				// Stop reading while output is pending: a client that
				// does not read its replies cannot grow them without bound.
				l.modify(c, unix.EPOLLOUT)
			}
			return false
		default:
			l.closeConn(c)
			return false
		}
	}
	c.sent = 0
	if cap(c.out) > connIdleKeep {
		c.out = nil
	} else {
		c.out = c.out[:0]
	}
	if c.writing {
		c.writing = false
		l.modify(c, unix.EPOLLIN)
	}
	return true
}

// resume continues a connection whose output was waiting on the socket.
func (l *eventLoop) resume(c *loopConn) {
	if !l.flush(c) {
		return
	}
	data := c.in
	if l.finish(c, data) {
		return
	}
	if len(data) > 0 {
		l.process(c, data)
	}
}

func (l *eventLoop) modify(c *loopConn, events uint32) {
	ev := unix.EpollEvent{Events: events, Fd: int32(c.fd)}
	if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, c.fd, &ev); err != nil {
		l.closeConn(c)
	}
}

func (l *eventLoop) closeConn(c *loopConn) {
	if l.conns[c.fd] != c {
		return
	}
	delete(l.conns, c.fd)
	unix.EpollCtl(l.epfd, unix.EPOLL_CTL_DEL, c.fd, nil)
	unix.Close(c.fd)
	c.pc.Close()
	c.release()
}

// takeover hands the connection to a goroutine with a blocking net.Conn.
func (l *eventLoop) takeover(c *loopConn, rest []byte) {
	delete(l.conns, c.fd)
	unix.EpollCtl(l.epfd, unix.EPOLL_CTL_DEL, c.fd, nil)
	f := os.NewFile(uintptr(c.fd), "")
	nc, err := net.FileConn(f) // a duplicate, in Go's poller
	f.Close()
	if err != nil {
		zap.L().Warn(fmt.Sprintf("handing a connection to a goroutine failed: %v", err), zap.Error(err))
		c.pc.Close()
		c.release()
		return
	}
	in := append([]byte(nil), rest...)
	go func() {
		defer c.release()
		defer nc.Close()
		defer c.pc.Close()
		defer func() {
			if r := recover(); r != nil {
				logPanic(c.pc.Addr(), r)
			}
		}()
		c.pc.Serve(nc, in)
	}()
}
