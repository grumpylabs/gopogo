package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// RedisHandler serves the RESP protocol (and RESP inline/telnet commands).
type RedisHandler struct {
	exec *Executor
}

// NewRedisHandler creates a Redis protocol handler. persist is the default
// path used by SAVE and LOAD; it may be empty.
func NewRedisHandler(cache *cache.Cache, auth, persist string) *RedisHandler {
	return &RedisHandler{exec: NewExecutor(cache, auth, persist)}
}

func (h *RedisHandler) Handle(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriterSize(conn, replyBufferSize)
	s := &session{
		proto:  TypeRedis,
		addr:   conn.RemoteAddr().String(),
		authed: h.exec.auth == "",
	}

	for {
		cmd, err := h.readCommand(reader)
		if err != nil {
			if err != io.EOF {
				writeRESP(writer, rv{kind: '-', s: err.Error()})
				writer.Flush()
			}
			return
		}
		if len(cmd) == 0 {
			continue
		}

		r := h.exec.exec(s, cmd)
		var lines chan string
		if r.monitor {
			// Subscribe before replying so no command after the OK is missed.
			lines = monitors.subscribe()
		}
		if r.err != "" {
			writeRESP(writer, rv{kind: '-', s: r.err})
		} else {
			writeRESP(writer, r.resp)
		}
		// Reply to a pipelined batch with one write: flush once no further
		// command is already buffered.
		if reader.Buffered() == 0 || r.quit || r.monitor {
			writer.Flush()
		}
		if r.quit {
			return
		}
		if r.monitor {
			h.monitor(reader, writer, lines)
			return
		}
	}
}

// monitor streams the subscribed command lines until the client sends QUIT
// or disconnects. Other input is ignored.
func (h *RedisHandler) monitor(reader *bufio.Reader, writer *bufio.Writer, lines chan string) {
	defer monitors.unsubscribe(lines)

	quit := make(chan bool, 1) // true: client sent QUIT
	go func() {
		for {
			cmd, err := h.readCommand(reader)
			if err != nil {
				quit <- false
				return
			}
			if len(cmd) > 0 && strings.EqualFold(cmd[0], "QUIT") {
				quit <- true
				return
			}
		}
	}()

	for {
		select {
		case line := <-lines:
			writer.WriteString(line)
			// Batch whatever else is queued into one write.
			for n := len(lines); n > 0; n-- {
				writer.WriteString(<-lines)
			}
			if writer.Flush() != nil {
				return
			}
		case sentQuit := <-quit:
			if sentQuit {
				writeRESP(writer, rvOK())
				writer.Flush()
			}
			return
		}
	}
}

// writeRESP writes v in RESP2. Unsigned values are written as simple
// strings, as pogocache does, since they may not fit a RESP integer.
func writeRESP(w *bufio.Writer, v rv) {
	switch v.kind {
	case '+', '-':
		w.WriteByte(v.kind)
		w.WriteString(v.s)
		w.WriteString("\r\n")
	case ':':
		writeRESPHeader(w, ':', v.n)
	case 'u':
		var b [24]byte
		w.Write(append(strconv.AppendUint(append(b[:0], '+'), v.u, 10), '\r', '\n'))
	case '$':
		writeRESPHeader(w, '$', int64(len(v.s)))
		w.WriteString(v.s)
		w.WriteString("\r\n")
	case '_':
		w.WriteString("$-1\r\n")
	case '*':
		writeRESPHeader(w, '*', int64(len(v.arr)))
		for _, e := range v.arr {
			writeRESP(w, e)
		}
	default:
		panic(fmt.Sprintf("writeRESP: unknown kind %q", v.kind))
	}
}

// writeRESPHeader writes a type byte, a number and CRLF without allocating.
func writeRESPHeader(w *bufio.Writer, kind byte, n int64) {
	var b [24]byte
	w.Write(append(strconv.AppendInt(append(b[:0], kind), n, 10), '\r', '\n'))
}

// readLine returns the next line, CRLF included. The slice is only valid
// until the next read, unless the line overflowed the buffer.
func readLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadSlice('\n')
	if err != bufio.ErrBufferFull {
		return line, err
	}
	long := append([]byte(nil), line...)
	rest, err := reader.ReadBytes('\n')
	return append(long, rest...), err
}

// replyBufferSize is each connection's reply buffer, Redis's 16 KiB, so a
// pipelined batch of typical replies goes out in one write.
const replyBufferSize = 16 << 10

// Limits on client-supplied lengths, as in Redis: a bad or hostile length
// gets an error instead of a huge or negative allocation.
const (
	maxArgs    = 1 << 20   // arguments in one command
	maxBulkLen = 512 << 20 // bytes in one argument or value
)

// readFull reads exactly n bytes. Memory grows with the bytes that arrive,
// so a client that claims a large length but sends little costs little.
func readFull(r io.Reader, n int) ([]byte, error) {
	const chunk = 64 << 10
	if n <= chunk {
		buf := make([]byte, n)
		_, err := io.ReadFull(r, buf)
		return buf, err
	}
	var b bytes.Buffer
	b.Grow(chunk)
	if _, err := io.CopyN(&b, r, int64(n)); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	return b.Bytes(), nil
}

// parseLen parses the decimal number after a RESP type byte.
// parseLen parses a RESP length: an optional minus sign and decimal digits.
// It does not allocate, unlike strconv.Atoi on a converted string.
func parseLen(b []byte) (int, error) {
	digits := b
	if len(digits) > 0 && digits[0] == '-' {
		digits = digits[1:]
	}
	if len(digits) == 0 || len(digits) > 18 {
		return 0, errInvalidLen
	}
	n := 0
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, errInvalidLen
		}
		n = n*10 + int(c-'0')
	}
	if len(digits) < len(b) {
		n = -n
	}
	return n, nil
}

var errInvalidLen = errors.New("invalid length")

func (h *RedisHandler) readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, nil
	}

	if line[0] == '*' {
		count, err := parseLen(line[1:])
		if err != nil || count > maxArgs {
			return nil, fmt.Errorf("invalid multibulk length")
		}
		return h.readArray(reader, count)
	}

	return strings.Fields(string(line)), nil
}

func (h *RedisHandler) readArray(reader *bufio.Reader, count int) ([]string, error) {
	if count < 0 {
		count = 0
	}
	args := make([]string, 0, count)

	for i := 0; i < count; i++ {
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}

		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}

		size, err := parseLen(line[1:])
		if err != nil || size < 0 || size > maxBulkLen {
			return nil, fmt.Errorf("invalid bulk length")
		}

		// A bulk string that fits the read buffer is copied straight out of
		// it; a larger one is read into its own buffer.
		if size+2 <= reader.Size() {
			b, err := reader.Peek(size + 2)
			if err != nil {
				return nil, err
			}
			args = append(args, string(b[:size]))
			reader.Discard(size + 2)
			continue
		}
		buf, err := readFull(reader, size+2)
		if err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}

	return args, nil
}

// handleSaveLoad implements SAVE [TO <path>] [FAST] and
// LOAD [FROM <path>] [FAST]. FAST is accepted for pogocache compatibility.
