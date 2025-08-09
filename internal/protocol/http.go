package protocol

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

type HTTPHandler struct {
	cache *cache.Cache
	auth  string
}

func NewHTTPHandler(cache *cache.Cache, auth string) *HTTPHandler {
	return &HTTPHandler{
		cache: cache,
		auth:  auth,
	}
}

func (h *HTTPHandler) Handle(conn net.Conn) {
	defer conn.Close()
	
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			if err != io.EOF {
				h.writeError(writer, http.StatusBadRequest, err.Error())
			}
			return
		}
		
		if h.auth != "" {
			authHeader := req.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") || authHeader[7:] != h.auth {
				h.writeError(writer, http.StatusUnauthorized, "Unauthorized")
				continue
			}
		}
		
		switch req.Method {
		case http.MethodGet:
			h.handleGet(writer, req)
		case http.MethodPost, http.MethodPut:
			h.handleSet(writer, req)
		case http.MethodDelete:
			h.handleDelete(writer, req)
		case http.MethodHead:
			h.handleHead(writer, req)
		default:
			h.writeError(writer, http.StatusMethodNotAllowed, "Method not allowed")
		}
		
		writer.Flush()
		
		if req.Header.Get("Connection") == "close" {
			return
		}
	}
}

func (h *HTTPHandler) handleGet(writer *bufio.Writer, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	
	if path == "" || path == "stats" {
		h.handleStats(writer)
		return
	}
	
	if path == "keys" {
		h.handleKeys(writer, req)
		return
	}
	
	entry, found := h.cache.Load([]byte(path))
	if !found {
		h.writeError(writer, http.StatusNotFound, "Key not found")
		return
	}
	
	h.writeResponse(writer, http.StatusOK, map[string]string{
		"Content-Type":   "application/octet-stream",
		"Content-Length": strconv.Itoa(len(entry.Value())),
		"X-Flags":        strconv.FormatUint(uint64(entry.Flags()), 10),
		"X-CAS":          strconv.FormatUint(entry.CAS(), 10),
	}, entry.Value())
}

func (h *HTTPHandler) handleSet(writer *bufio.Writer, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	if path == "" {
		h.writeError(writer, http.StatusBadRequest, "Key required")
		return
	}
	
	body := make([]byte, req.ContentLength)
	_, err := io.ReadFull(req.Body, body)
	if err != nil {
		h.writeError(writer, http.StatusBadRequest, "Failed to read body")
		return
	}
	
	opts := &cache.StoreOptions{}
	
	if ttl := req.Header.Get("X-TTL"); ttl != "" {
		seconds, err := strconv.Atoi(ttl)
		if err == nil {
			opts.TTL = time.Duration(seconds) * time.Second
		}
	}
	
	if flags := req.Header.Get("X-Flags"); flags != "" {
		f, err := strconv.ParseUint(flags, 10, 32)
		if err == nil {
			opts.Flags = uint32(f)
		}
	}
	
	if cas := req.Header.Get("X-CAS"); cas != "" {
		casVal, err := strconv.ParseUint(cas, 10, 64)
		if err == nil {
			opts.CAS = casVal
			success, err := h.cache.CompareAndSwap([]byte(path), body, casVal, opts)
			if err != nil {
				h.writeError(writer, http.StatusInternalServerError, err.Error())
				return
			}
			if !success {
				h.writeError(writer, http.StatusConflict, "CAS mismatch")
				return
			}
			h.writeResponse(writer, http.StatusOK, nil, []byte("OK"))
			return
		}
	}
	
	h.cache.Store([]byte(path), body, opts)
	h.writeResponse(writer, http.StatusCreated, nil, []byte("OK"))
}

func (h *HTTPHandler) handleDelete(writer *bufio.Writer, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	if path == "" {
		h.writeError(writer, http.StatusBadRequest, "Key required")
		return
	}
	
	if h.cache.Delete([]byte(path)) {
		h.writeResponse(writer, http.StatusOK, nil, []byte("OK"))
	} else {
		h.writeError(writer, http.StatusNotFound, "Key not found")
	}
}

func (h *HTTPHandler) handleHead(writer *bufio.Writer, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	if path == "" {
		h.writeError(writer, http.StatusBadRequest, "Key required")
		return
	}
	
	entry, found := h.cache.Load([]byte(path))
	if !found {
		h.writeError(writer, http.StatusNotFound, "Key not found")
		return
	}
	
	h.writeResponse(writer, http.StatusOK, map[string]string{
		"Content-Type":   "application/octet-stream",
		"Content-Length": strconv.Itoa(len(entry.Value())),
		"X-Flags":        strconv.FormatUint(uint64(entry.Flags()), 10),
		"X-CAS":          strconv.FormatUint(entry.CAS(), 10),
	}, nil)
}

func (h *HTTPHandler) handleStats(writer *bufio.Writer) {
	stats := h.cache.Stats()
	
	body, _ := json.MarshalIndent(stats, "", "  ")
	
	h.writeResponse(writer, http.StatusOK, map[string]string{
		"Content-Type":   "application/json",
		"Content-Length": strconv.Itoa(len(body)),
	}, body)
}

func (h *HTTPHandler) handleKeys(writer *bufio.Writer, req *http.Request) {
	pattern := req.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}
	
	keys := make([]string, 0)
	h.cache.Iterate(func(entry *cache.Entry) bool {
		key := string(entry.Key())
		if pattern == "*" || matchPattern(pattern, key) {
			keys = append(keys, key)
		}
		return true
	})
	
	body, _ := json.Marshal(keys)
	
	h.writeResponse(writer, http.StatusOK, map[string]string{
		"Content-Type":   "application/json",
		"Content-Length": strconv.Itoa(len(body)),
	}, body)
}

func (h *HTTPHandler) writeResponse(writer *bufio.Writer, status int, headers map[string]string, body []byte) {
	writer.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, http.StatusText(status)))
	writer.WriteString("Server: gopogo/1.0\r\n")
	writer.WriteString("Date: " + time.Now().UTC().Format(http.TimeFormat) + "\r\n")
	
	for key, value := range headers {
		writer.WriteString(fmt.Sprintf("%s: %s\r\n", key, value))
	}
	
	if body == nil {
		writer.WriteString("Content-Length: 0\r\n")
	}
	
	writer.WriteString("\r\n")
	
	if body != nil {
		writer.Write(body)
	}
}

func (h *HTTPHandler) writeError(writer *bufio.Writer, status int, message string) {
	body := fmt.Sprintf(`{"error":"%s"}`, message)
	h.writeResponse(writer, status, map[string]string{
		"Content-Type":   "application/json",
		"Content-Length": strconv.Itoa(len(body)),
	}, []byte(body))
}