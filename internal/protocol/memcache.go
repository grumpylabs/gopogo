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

type MemcacheHandler struct {
	cache *cache.Cache
}

func NewMemcacheHandler(cache *cache.Cache) *MemcacheHandler {
	return &MemcacheHandler{
		cache: cache,
	}
}

func (h *MemcacheHandler) Handle(conn net.Conn) {
	defer conn.Close()
	
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	
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
			writer.WriteString("VERSION 1.6.0\r\n")
			
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

func (h *MemcacheHandler) handleAppend(reader *bufio.Reader, writer *bufio.Writer, parts []string, append bool) {
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
	
	entry, found := h.cache.Load([]byte(key))
	if !found {
		if !noreply {
			writer.WriteString("NOT_STORED\r\n")
		}
		return
	}
	
	var newValue []byte
	if append {
		newValue = make([]byte, len(entry.Value())+len(data))
		copy(newValue, entry.Value())
		copy(newValue[len(entry.Value()):], data)
	} else {
		newValue = make([]byte, len(data)+len(entry.Value()))
		copy(newValue, data)
		copy(newValue[len(data):], entry.Value())
	}
	
	h.cache.Store([]byte(key), newValue, &cache.StoreOptions{
		Flags: entry.Flags(),
	})
	
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

func (h *MemcacheHandler) handleIncr(writer *bufio.Writer, parts []string, incr bool) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	
	key := parts[1]
	delta, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR invalid numeric delta argument\r\n")
		return
	}
	
	noreply := len(parts) > 3 && parts[3] == "noreply"
	
	if !incr {
		delta = -delta
	}
	
	newVal, err := h.cache.Increment([]byte(key), delta)
	if err != nil {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
		return
	}
	
	if newVal < 0 {
		newVal = 0
		h.cache.Store([]byte(key), []byte(strconv.FormatInt(newVal, 10)), nil)
	}
	
	if !noreply {
		fmt.Fprintf(writer, "%d\r\n", newVal)
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
	stats := h.cache.Stats()
	
	fmt.Fprintf(writer, "STAT curr_items %d\r\n", stats["num_items"])
	fmt.Fprintf(writer, "STAT bytes %d\r\n", stats["mem_used"])
	fmt.Fprintf(writer, "STAT limit_maxbytes %d\r\n", stats["max_memory"])
	fmt.Fprintf(writer, "STAT cmd_get %d\r\n", stats["num_hits"].(uint64)+stats["num_misses"].(uint64))
	fmt.Fprintf(writer, "STAT get_hits %d\r\n", stats["num_hits"])
	fmt.Fprintf(writer, "STAT get_misses %d\r\n", stats["num_misses"])
	fmt.Fprintf(writer, "STAT evictions %d\r\n", stats["num_evicted"])
	fmt.Fprintf(writer, "STAT expired_unfetched %d\r\n", stats["num_expired"])
	writer.WriteString("END\r\n")
}