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

func (d *Detector) Detect() (Type, error) {
	peek, err := d.reader.Peek(8)
	if err != nil && err != io.EOF {
		return TypeUnknown, err
	}
	
	d.peeked = peek
	
	if len(peek) == 0 {
		return TypeRedis, nil
	}
	
	if peek[0] == '*' || peek[0] == '$' || peek[0] == '+' || peek[0] == '-' || peek[0] == ':' {
		return TypeRedis, nil
	}
	
	if bytes.HasPrefix(peek, []byte("GET ")) || 
	   bytes.HasPrefix(peek, []byte("POST ")) ||
	   bytes.HasPrefix(peek, []byte("PUT ")) ||
	   bytes.HasPrefix(peek, []byte("DELETE ")) ||
	   bytes.HasPrefix(peek, []byte("HEAD ")) ||
	   bytes.HasPrefix(peek, []byte("OPTIONS ")) ||
	   bytes.HasPrefix(peek, []byte("PATCH ")) {
		return TypeHTTP, nil
	}
	
	if bytes.HasPrefix(peek, []byte("get ")) ||
	   bytes.HasPrefix(peek, []byte("set ")) ||
	   bytes.HasPrefix(peek, []byte("add ")) ||
	   bytes.HasPrefix(peek, []byte("replace ")) ||
	   bytes.HasPrefix(peek, []byte("delete ")) ||
	   bytes.HasPrefix(peek, []byte("incr ")) ||
	   bytes.HasPrefix(peek, []byte("decr ")) ||
	   bytes.HasPrefix(peek, []byte("stats")) ||
	   bytes.HasPrefix(peek, []byte("flush")) ||
	   bytes.HasPrefix(peek, []byte("version")) {
		return TypeMemcache, nil
	}
	
	if len(peek) >= 8 && peek[4] == 0x00 && peek[5] == 0x03 && peek[6] == 0x00 && peek[7] == 0x00 {
		return TypePostgres, nil
	}
	
	return TypeRedis, nil
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