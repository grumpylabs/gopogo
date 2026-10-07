package protocol

import (
	"bufio"
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
	writer := bufio.NewWriter(conn)
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
		w.WriteString(":" + strconv.FormatInt(v.n, 10) + "\r\n")
	case 'u':
		w.WriteString("+" + strconv.FormatUint(v.u, 10) + "\r\n")
	case '$':
		w.WriteString("$" + strconv.Itoa(len(v.s)) + "\r\n")
		w.WriteString(v.s)
		w.WriteString("\r\n")
	case '_':
		w.WriteString("$-1\r\n")
	case '*':
		w.WriteString("*" + strconv.Itoa(len(v.arr)) + "\r\n")
		for _, e := range v.arr {
			writeRESP(w, e)
		}
	default:
		panic(fmt.Sprintf("writeRESP: unknown kind %q", v.kind))
	}
}

func (h *RedisHandler) readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return nil, nil
	}

	if line[0] == '*' {
		return h.readArray(reader, line)
	}

	return strings.Fields(line), nil
}

func (h *RedisHandler) readArray(reader *bufio.Reader, line string) ([]string, error) {
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, count)

	for i := 0; i < count; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}

		size, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}

		buf := make([]byte, size+2)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		args = append(args, string(buf[:size]))
	}

	return args, nil
}

// handleSaveLoad implements SAVE [TO <path>] [FAST] and
// LOAD [FROM <path>] [FAST]. FAST is accepted for pogocache compatibility.
