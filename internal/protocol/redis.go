package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

type RedisHandler struct {
	cache        *cache.Cache
	auth         string
	authRequired bool
}

func NewRedisHandler(cache *cache.Cache, auth string) *RedisHandler {
	return &RedisHandler{
		cache:        cache,
		auth:         auth,
		authRequired: auth != "",
	}
}

func (h *RedisHandler) Handle(conn net.Conn) {
	defer conn.Close()
	
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	authenticated := !h.authRequired
	
	for {
		cmd, err := h.readCommand(reader)
		if err != nil {
			if err != io.EOF {
				h.writeError(writer, err.Error())
				writer.Flush()
			}
			return
		}
		
		if len(cmd) == 0 {
			continue
		}
		
		cmdName := strings.ToUpper(cmd[0])
		
		if !authenticated && cmdName != "AUTH" && cmdName != "PING" {
			h.writeError(writer, "NOAUTH Authentication required")
			writer.Flush()
			continue
		}
		
		switch cmdName {
		case "AUTH":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'auth' command")
			} else if cmd[1] == h.auth {
				authenticated = true
				h.writeSimpleString(writer, "OK")
			} else {
				h.writeError(writer, "ERR invalid password")
			}
			
		case "PING":
			if len(cmd) == 1 {
				h.writeSimpleString(writer, "PONG")
			} else {
				h.writeBulkString(writer, cmd[1])
			}
			
		case "GET":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'get' command")
			} else {
				h.handleGet(writer, cmd[1])
			}
			
		case "SET":
			if len(cmd) < 3 {
				h.writeError(writer, "ERR wrong number of arguments for 'set' command")
			} else {
				h.handleSet(writer, cmd[1:])
			}
			
		case "DEL":
			if len(cmd) < 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'del' command")
			} else {
				h.handleDel(writer, cmd[1:])
			}
			
		case "EXISTS":
			if len(cmd) < 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'exists' command")
			} else {
				h.handleExists(writer, cmd[1:])
			}
			
		case "INCR":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'incr' command")
			} else {
				h.handleIncr(writer, cmd[1], 1)
			}
			
		case "DECR":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'decr' command")
			} else {
				h.handleIncr(writer, cmd[1], -1)
			}
			
		case "INCRBY":
			if len(cmd) != 3 {
				h.writeError(writer, "ERR wrong number of arguments for 'incrby' command")
			} else {
				delta, err := strconv.ParseInt(cmd[2], 10, 64)
				if err != nil {
					h.writeError(writer, "ERR value is not an integer or out of range")
				} else {
					h.handleIncr(writer, cmd[1], delta)
				}
			}
			
		case "DECRBY":
			if len(cmd) != 3 {
				h.writeError(writer, "ERR wrong number of arguments for 'decrby' command")
			} else {
				delta, err := strconv.ParseInt(cmd[2], 10, 64)
				if err != nil {
					h.writeError(writer, "ERR value is not an integer or out of range")
				} else {
					h.handleIncr(writer, cmd[1], -delta)
				}
			}
			
		case "MGET":
			if len(cmd) < 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'mget' command")
			} else {
				h.handleMGet(writer, cmd[1:])
			}
			
		case "MSET":
			if len(cmd) < 3 || len(cmd)%2 == 0 {
				h.writeError(writer, "ERR wrong number of arguments for 'mset' command")
			} else {
				h.handleMSet(writer, cmd[1:])
			}
			
		case "EXPIRE":
			if len(cmd) != 3 {
				h.writeError(writer, "ERR wrong number of arguments for 'expire' command")
			} else {
				h.handleExpire(writer, cmd[1], cmd[2])
			}
			
		case "TTL":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'ttl' command")
			} else {
				h.handleTTL(writer, cmd[1])
			}
			
		case "KEYS":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'keys' command")
			} else {
				h.handleKeys(writer, cmd[1])
			}
			
		case "FLUSHDB", "FLUSHALL":
			h.cache.Clear()
			h.writeSimpleString(writer, "OK")
			
		case "DBSIZE":
			h.writeInteger(writer, int64(h.cache.NumItems()))
			
		case "INFO":
			h.handleInfo(writer)
			
		case "QUIT":
			h.writeSimpleString(writer, "OK")
			writer.Flush()
			return
			
		case "SELECT":
			h.writeSimpleString(writer, "OK")
			
		case "ECHO":
			if len(cmd) != 2 {
				h.writeError(writer, "ERR wrong number of arguments for 'echo' command")
			} else {
				h.writeBulkString(writer, cmd[1])
			}
			
		default:
			h.writeError(writer, fmt.Sprintf("ERR unknown command '%s'", cmdName))
		}
		
		writer.Flush()
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

func (h *RedisHandler) writeError(writer *bufio.Writer, msg string) {
	writer.WriteString("-")
	writer.WriteString(msg)
	writer.WriteString("\r\n")
}

func (h *RedisHandler) writeSimpleString(writer *bufio.Writer, msg string) {
	writer.WriteString("+")
	writer.WriteString(msg)
	writer.WriteString("\r\n")
}

func (h *RedisHandler) writeInteger(writer *bufio.Writer, n int64) {
	writer.WriteString(":")
	writer.WriteString(strconv.FormatInt(n, 10))
	writer.WriteString("\r\n")
}

func (h *RedisHandler) writeBulkString(writer *bufio.Writer, s string) {
	writer.WriteString("$")
	writer.WriteString(strconv.Itoa(len(s)))
	writer.WriteString("\r\n")
	writer.WriteString(s)
	writer.WriteString("\r\n")
}

func (h *RedisHandler) writeNil(writer *bufio.Writer) {
	writer.WriteString("$-1\r\n")
}

func (h *RedisHandler) writeArray(writer *bufio.Writer, items []string) {
	writer.WriteString("*")
	writer.WriteString(strconv.Itoa(len(items)))
	writer.WriteString("\r\n")
	
	for _, item := range items {
		h.writeBulkString(writer, item)
	}
}

func (h *RedisHandler) handleGet(writer *bufio.Writer, key string) {
	entry, found := h.cache.Load([]byte(key))
	if !found {
		h.writeNil(writer)
		return
	}
	
	h.writeBulkString(writer, string(entry.Value()))
}

func (h *RedisHandler) handleSet(writer *bufio.Writer, args []string) {
	key := args[0]
	value := args[1]
	
	opts := &cache.StoreOptions{}
	
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX":
			if i+1 < len(args) {
				seconds, err := strconv.Atoi(args[i+1])
				if err == nil {
					opts.TTL = time.Duration(seconds) * time.Second
				}
				i++
			}
		case "PX":
			if i+1 < len(args) {
				millis, err := strconv.Atoi(args[i+1])
				if err == nil {
					opts.TTL = time.Duration(millis) * time.Millisecond
				}
				i++
			}
		case "NX":
			if entry, _ := h.cache.Load([]byte(key)); entry != nil {
				h.writeNil(writer)
				return
			}
		case "XX":
			if entry, _ := h.cache.Load([]byte(key)); entry == nil {
				h.writeNil(writer)
				return
			}
		}
	}
	
	h.cache.Store([]byte(key), []byte(value), opts)
	h.writeSimpleString(writer, "OK")
}

func (h *RedisHandler) handleDel(writer *bufio.Writer, keys []string) {
	deleted := int64(0)
	for _, key := range keys {
		if h.cache.Delete([]byte(key)) {
			deleted++
		}
	}
	h.writeInteger(writer, deleted)
}

func (h *RedisHandler) handleExists(writer *bufio.Writer, keys []string) {
	exists := int64(0)
	for _, key := range keys {
		if entry, _ := h.cache.Load([]byte(key)); entry != nil {
			exists++
		}
	}
	h.writeInteger(writer, exists)
}

func (h *RedisHandler) handleIncr(writer *bufio.Writer, key string, delta int64) {
	newVal, err := h.cache.Increment([]byte(key), delta)
	if err != nil {
		h.writeError(writer, err.Error())
		return
	}
	h.writeInteger(writer, newVal)
}

func (h *RedisHandler) handleMGet(writer *bufio.Writer, keys []string) {
	writer.WriteString("*")
	writer.WriteString(strconv.Itoa(len(keys)))
	writer.WriteString("\r\n")
	
	for _, key := range keys {
		entry, found := h.cache.Load([]byte(key))
		if !found {
			h.writeNil(writer)
		} else {
			h.writeBulkString(writer, string(entry.Value()))
		}
	}
}

func (h *RedisHandler) handleMSet(writer *bufio.Writer, args []string) {
	for i := 0; i < len(args); i += 2 {
		h.cache.Store([]byte(args[i]), []byte(args[i+1]), nil)
	}
	h.writeSimpleString(writer, "OK")
}

func (h *RedisHandler) handleExpire(writer *bufio.Writer, key, secondsStr string) {
	seconds, err := strconv.Atoi(secondsStr)
	if err != nil {
		h.writeError(writer, "ERR value is not an integer or out of range")
		return
	}
	
	entry, found := h.cache.Load([]byte(key))
	if !found {
		h.writeInteger(writer, 0)
		return
	}
	
	entry.SetExpireAt(time.Now().Add(time.Duration(seconds) * time.Second).UnixNano())
	h.writeInteger(writer, 1)
}

func (h *RedisHandler) handleTTL(writer *bufio.Writer, key string) {
	entry, found := h.cache.Load([]byte(key))
	if !found {
		h.writeInteger(writer, -2)
		return
	}
	
	expireAt := entry.ExpireAt()
	if expireAt == 0 {
		h.writeInteger(writer, -1)
		return
	}
	
	ttl := (expireAt - time.Now().UnixNano()) / 1e9
	if ttl < 0 {
		ttl = 0
	}
	h.writeInteger(writer, ttl)
}

func (h *RedisHandler) handleKeys(writer *bufio.Writer, pattern string) {
	keys := make([]string, 0)
	
	h.cache.Iterate(func(entry *cache.Entry) bool {
		key := string(entry.Key())
		if pattern == "*" || matchPattern(pattern, key) {
			keys = append(keys, key)
		}
		return true
	})
	
	h.writeArray(writer, keys)
}

func (h *RedisHandler) handleInfo(writer *bufio.Writer) {
	stats := h.cache.Stats()
	
	info := fmt.Sprintf("# Server\r\n"+
		"redis_version:7.0.0\r\n"+
		"redis_mode:standalone\r\n"+
		"process_id:1\r\n"+
		"tcp_port:6379\r\n"+
		"\r\n"+
		"# Keyspace\r\n"+
		"db0:keys=%d,expires=0\r\n"+
		"\r\n"+
		"# Stats\r\n"+
		"total_commands_processed:%d\r\n"+
		"keyspace_hits:%d\r\n"+
		"keyspace_misses:%d\r\n"+
		"evicted_keys:%d\r\n"+
		"expired_keys:%d\r\n"+
		"\r\n"+
		"# Memory\r\n"+
		"used_memory:%d\r\n"+
		"used_memory_human:%s\r\n",
		stats["num_items"],
		stats["num_ops"],
		stats["num_hits"],
		stats["num_misses"],
		stats["num_evicted"],
		stats["num_expired"],
		stats["mem_used"],
		formatMemory(stats["mem_used"].(int64)))
	
	h.writeBulkString(writer, info)
}

func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	
	i, j := 0, 0
	for i < len(pattern) && j < len(key) {
		if pattern[i] == '*' {
			if i == len(pattern)-1 {
				return true
			}
			for j < len(key) {
				if matchPattern(pattern[i+1:], key[j:]) {
					return true
				}
				j++
			}
			return false
		} else if pattern[i] == '?' || pattern[i] == key[j] {
			i++
			j++
		} else {
			return false
		}
	}
	
	for i < len(pattern) && pattern[i] == '*' {
		i++
	}
	
	return i == len(pattern) && j == len(key)
}

func formatMemory(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}