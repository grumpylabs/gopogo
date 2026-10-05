package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

type MemcacheHandler struct {
	cache *cache.Cache
	auth  string
}

// NewMemcacheHandler creates a Memcache text protocol handler. The memcache
// protocol has no way to authenticate, so when auth is set every command is
// refused, as in pogocache; use another protocol for password-protected
// access.
func NewMemcacheHandler(cache *cache.Cache, auth string) *MemcacheHandler {
	return &MemcacheHandler{
		cache: cache,
		auth:  auth,
	}
}

// memcacheStorageCmds are the commands followed by a data block.
var memcacheStorageCmds = map[string]bool{
	"set": true, "add": true, "replace": true, "append": true, "prepend": true, "cas": true,
}

func (h *MemcacheHandler) Handle(conn net.Conn) {
	defer conn.Close()
	
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	addr := conn.RemoteAddr().String()
	
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				writer.WriteString("ERROR\r\n")
				writer.Flush()
			}
			return
		}
		
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		
		cmd := strings.ToLower(parts[0])
		if h.auth != "" {
			if cmd == "quit" {
				return
			}
			// Consume a storage command's data block so the next command
			// line is read correctly.
			if memcacheStorageCmds[cmd] && len(parts) >= 5 {
				if n, err := strconv.Atoi(parts[4]); err == nil && n >= 0 {
					if _, err := io.CopyN(io.Discard, reader, int64(n)+2); err != nil {
						return
					}
				}
			}
			writer.WriteString("CLIENT_ERROR Authentication required\r\n")
			writer.Flush()
			continue
		}
		monitors.publish(addr, parts)
		
		switch cmd {
		case "get", "gets":
			h.handleGet(reader, writer, parts[1:], cmd == "gets")
			
		case "set":
			h.handleStore(reader, writer, parts, false, false)
			
		case "add":
			h.handleStore(reader, writer, parts, true, false)
			
		case "replace":
			h.handleStore(reader, writer, parts, false, true)
			
		case "append":
			h.handleAppend(reader, writer, parts, true)
			
		case "prepend":
			h.handleAppend(reader, writer, parts, false)
			
		case "cas":
			h.handleCAS(reader, writer, parts)
			
		case "delete":
			h.handleDelete(writer, parts)
			
		case "incr":
			h.handleIncr(writer, parts, true)
			
		case "decr":
			h.handleIncr(writer, parts, false)
			
		case "touch":
			h.handleTouch(writer, parts)
			
		case "flush_all":
			h.cache.Clear()
			writer.WriteString("OK\r\n")
			
		case "stats":
			h.handleStats(writer)
			
		case "version":
			writer.WriteString("VERSION " + Version + "\r\n")
			
		case "quit":
			writer.Flush()
			return
			
		default:
			writer.WriteString("ERROR\r\n")
		}
		
		writer.Flush()
	}
}

func (h *MemcacheHandler) handleGet(reader *bufio.Reader, writer *bufio.Writer, keys []string, withCAS bool) {
	for _, key := range keys {
		entry, found := h.cache.Load([]byte(key))
		if !found {
			continue
		}
		
		if withCAS {
			fmt.Fprintf(writer, "VALUE %s %d %d %d\r\n", 
				key, entry.Flags(), len(entry.Value()), entry.CAS())
		} else {
			fmt.Fprintf(writer, "VALUE %s %d %d\r\n", 
				key, entry.Flags(), len(entry.Value()))
		}
		
		writer.Write(entry.Value())
		writer.WriteString("\r\n")
	}
	writer.WriteString("END\r\n")
}

func (h *MemcacheHandler) handleStore(reader *bufio.Reader, writer *bufio.Writer, parts []string, addOnly, replaceOnly bool) {
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	bytes, err := strconv.Atoi(parts[4])
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 5 && parts[5] == "noreply"
	
	data := make([]byte, bytes)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad data chunk\r\n")
		return
	}
	
	reader.ReadString('\n')
	
	existing, _ := h.cache.Load([]byte(key))
	
	if addOnly && existing != nil {
		if !noreply {
			writer.WriteString("NOT_STORED\r\n")
		}
		return
	}
	
	if replaceOnly && existing == nil {
		if !noreply {
			writer.WriteString("NOT_STORED\r\n")
		}
		return
	}
	
	opts := &cache.StoreOptions{
		Flags: uint32(flags),
	}
	
	if exptime > 0 {
		if exptime < 2592000 {
			opts.TTL = time.Duration(exptime) * time.Second
		} else {
			opts.TTL = time.Until(time.Unix(exptime, 0))
		}
	}
	
	h.cache.Store([]byte(key), data, opts)
	
	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (h *MemcacheHandler) handleCAS(reader *bufio.Reader, writer *bufio.Writer, parts []string) {
	if len(parts) < 6 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	bytes, err := strconv.Atoi(parts[4])
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	cas, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 6 && parts[6] == "noreply"
	
	data := make([]byte, bytes)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad data chunk\r\n")
		return
	}
	
	reader.ReadString('\n')
	
	opts := &cache.StoreOptions{
		Flags: uint32(flags),
	}
	
	if exptime > 0 {
		if exptime < 2592000 {
			opts.TTL = time.Duration(exptime) * time.Second
		} else {
			opts.TTL = time.Until(time.Unix(exptime, 0))
		}
	}
	
	success, err := h.cache.CompareAndSwap([]byte(key), data, cas, opts)
	if err != nil {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
		return
	}
	
	if !success {
		if !noreply {
			writer.WriteString("EXISTS\r\n")
		}
		return
	}
	
	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (h *MemcacheHandler) handleAppend(reader *bufio.Reader, writer *bufio.Writer, parts []string, isAppend bool) {
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	bytes, err := strconv.Atoi(parts[4])
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 5 && parts[5] == "noreply"
	
	data := make([]byte, bytes)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad data chunk\r\n")
		return
	}
	
	reader.ReadString('\n')
	
	// Update is atomic and keeps the entry's flags and TTL.
	err = h.cache.Update([]byte(key), func(cur []byte, found bool) ([]byte, error) {
		if !found {
			return nil, errMemcacheNotFound
		}
		out := make([]byte, 0, len(cur)+len(data))
		if isAppend {
			out = append(append(out, cur...), data...)
		} else {
			out = append(append(out, data...), cur...)
		}
		return out, nil
	})
	if err != nil {
		if !noreply {
			writer.WriteString("NOT_STORED\r\n")
		}
		return
	}
	
	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (h *MemcacheHandler) handleDelete(writer *bufio.Writer, parts []string) {
	if len(parts) < 2 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	noreply := len(parts) > 2 && parts[len(parts)-1] == "noreply"
	
	if h.cache.Delete([]byte(key)) {
		if !noreply {
			writer.WriteString("DELETED\r\n")
		}
	} else {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
	}
}

var (
	errMemcacheNotFound   = errors.New("not found")
	errMemcacheNonNumeric = errors.New("non-numeric")
)

// handleIncr implements incr/decr on unsigned 64-bit decimal values. Like
// memcached, a missing key is NOT_FOUND, incr wraps around at 2^64 and decr
// stops at 0.
func (h *MemcacheHandler) handleIncr(writer *bufio.Writer, parts []string, incr bool) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	key := parts[1]
	delta, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR invalid numeric delta argument\r\n")
		return
	}

	noreply := len(parts) > 3 && parts[3] == "noreply"

	var newVal uint64
	err = h.cache.Update([]byte(key), func(cur []byte, found bool) ([]byte, error) {
		if !found {
			return nil, errMemcacheNotFound
		}
		n, err := strconv.ParseUint(string(cur), 10, 64)
		if err != nil {
			return nil, errMemcacheNonNumeric
		}
		switch {
		case incr:
			newVal = n + delta
		case delta > n:
			newVal = 0
		default:
			newVal = n - delta
		}
		return strconv.AppendUint(nil, newVal, 10), nil
	})
	if noreply {
		return
	}
	switch err {
	case nil:
		fmt.Fprintf(writer, "%d\r\n", newVal)
	case errMemcacheNotFound:
		writer.WriteString("NOT_FOUND\r\n")
	default:
		writer.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
	}
}

func (h *MemcacheHandler) handleTouch(writer *bufio.Writer, parts []string) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	exptime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	noreply := len(parts) > 3 && parts[3] == "noreply"
	
	entry, found := h.cache.Load([]byte(key))
	if !found {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
		return
	}
	
	if exptime > 0 {
		if exptime < 2592000 {
			entry.SetExpireAt(time.Now().Add(time.Duration(exptime) * time.Second).UnixNano())
		} else {
			entry.SetExpireAt(time.Unix(exptime, 0).UnixNano())
		}
	} else {
		entry.SetExpireAt(0)
	}
	
	if !noreply {
		writer.WriteString("TOUCHED\r\n")
	}
}

func (h *MemcacheHandler) handleStats(writer *bufio.Writer) {
	for _, kv := range statLines(h.cache) {
		fmt.Fprintf(writer, "STAT %s %s\r\n", kv[0], kv[1])
	}
	writer.WriteString("END\r\n")
}