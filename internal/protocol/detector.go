package protocol

import (
	"bufio"
	"bytes"
	"io"
	"net"
)

type Type int

const (
	TypeUnknown Type = iota
	TypeRedis
	TypeHTTP
	TypeMemcache
	TypePostgres
)

// String returns the protocol name used in metrics and spans.
func (t Type) String() string {
	switch t {
	case TypeRedis:
		return "redis"
	case TypeHTTP:
		return "http"
	case TypeMemcache:
		return "memcache"
	case TypePostgres:
		return "postgres"
	}
	return "unknown"
}

type Detector struct {
	conn   net.Conn
	reader *bufio.Reader
	peeked []byte
}

func NewDetector(conn net.Conn) *Detector {
	return &Detector{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}
}

// Detect sniffs the protocol from the first bytes of a connection using
// pogocache's rules: '*' is RESP, a zero byte is Postgres, a first line ending
// in " HTTP/x.y" is HTTP, a line starting with an uppercase letter is RESP
// inline (telnet) and any other line is Memcache.
func (d *Detector) Detect() (Type, error) {
	first, err := d.reader.Peek(1)
	if err != nil {
		if err == io.EOF {
			return TypeRedis, nil
		}
		return TypeUnknown, err
	}
	switch first[0] {
	case '*':
		return TypeRedis, nil
	case 0:
		return TypePostgres, nil
	}

	line := d.peekLine()
	d.peeked = line
	if isHTTPRequestLine(line) {
		return TypeHTTP, nil
	}
	line = bytes.TrimLeft(line, " ")
	if len(line) > 0 && line[0] >= 'A' && line[0] <= 'Z' {
		return TypeRedis, nil
	}
	return TypeMemcache, nil
}

// peekLine returns the buffered bytes up to and including the first '\n',
// waiting for more input as needed. It returns what is buffered if the line
// does not fit the buffer or the connection ends first.
func (d *Detector) peekLine() []byte {
	for {
		n := d.reader.Buffered()
		buf, _ := d.reader.Peek(n)
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			return buf[:i+1]
		}
		if n == d.reader.Size() {
			return buf
		}
		if _, err := d.reader.Peek(n + 1); err != nil {
			buf, _ = d.reader.Peek(d.reader.Buffered())
			return buf
		}
	}
}

// isHTTPRequestLine reports whether line ends in " HTTP/x.y\r\n".
func isHTTPRequestLine(line []byte) bool {
	n := len(line)
	return n >= 11 && bytes.Equal(line[n-11:n-5], []byte(" HTTP/")) &&
		line[n-4] == '.' && line[n-2] == '\r'
}

func (d *Detector) Conn() net.Conn {
	return &detectorConn{
		Conn:   d.conn,
		reader: d.reader,
	}
}

type detectorConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *detectorConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}
