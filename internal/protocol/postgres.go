package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/grumpylabs/gopogo/internal/cache"
)

type PostgresHandler struct {
	cache *cache.Cache
	auth  string
}

func NewPostgresHandler(cache *cache.Cache, auth string) *PostgresHandler {
	return &PostgresHandler{
		cache: cache,
		auth:  auth,
	}
}

func (h *PostgresHandler) Handle(conn net.Conn) {
	defer conn.Close()
	
	if err := h.handleStartup(conn); err != nil {
		return
	}
	
	authenticated := h.auth == ""
	
	for {
		msgType, data, err := h.readMessage(conn)
		if err != nil {
			return
		}
		
		if !authenticated && msgType != 'p' {
			h.sendErrorResponse(conn, "28P01", "authentication required")
			continue
		}
		
		switch msgType {
		case 'p':
			password := string(bytes.TrimRight(data, "\x00"))
			if password == h.auth {
				authenticated = true
				h.sendAuthenticationOk(conn)
				h.sendReadyForQuery(conn)
			} else {
				h.sendErrorResponse(conn, "28P01", "authentication failed")
			}
			
		case 'Q':
			query := string(bytes.TrimRight(data, "\x00"))
			h.handleQuery(conn, query)
			
		case 'X':
			return
			
		default:
			h.sendErrorResponse(conn, "08P01", "unsupported message type")
		}
	}
}

func (h *PostgresHandler) handleStartup(conn net.Conn) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	
	length := binary.BigEndian.Uint32(buf[:4])
	version := binary.BigEndian.Uint32(buf[4:])
	
	if version != 196608 {
		return fmt.Errorf("unsupported protocol version: %d", version)
	}
	
	params := make([]byte, length-8)
	if _, err := io.ReadFull(conn, params); err != nil {
		return err
	}
	
	if h.auth != "" {
		h.sendAuthenticationCleartextPassword(conn)
	} else {
		h.sendAuthenticationOk(conn)
		h.sendReadyForQuery(conn)
	}
	
	return nil
}

func (h *PostgresHandler) handleQuery(conn net.Conn, query string) {
	query = strings.TrimSpace(strings.ToUpper(query))
	
	if strings.HasPrefix(query, "SELECT ") {
		h.handleSelect(conn, query)
	} else if strings.HasPrefix(query, "INSERT ") {
		h.handleInsert(conn, query)
	} else if strings.HasPrefix(query, "UPDATE ") {
		h.handleUpdate(conn, query)
	} else if strings.HasPrefix(query, "DELETE ") {
		h.handleDelete(conn, query)
	} else {
		h.sendErrorResponse(conn, "42601", "syntax error")
	}
	
	h.sendReadyForQuery(conn)
}

func (h *PostgresHandler) handleSelect(conn net.Conn, query string) {
	parts := strings.Fields(query)
	if len(parts) < 4 || parts[2] != "FROM" {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	table := parts[3]
	
	var key string
	whereIdx := -1
	for i, part := range parts {
		if part == "WHERE" {
			whereIdx = i
			break
		}
	}
	
	if whereIdx > 0 && whereIdx+3 < len(parts) && parts[whereIdx+2] == "=" {
		key = strings.Trim(parts[whereIdx+3], "'\"")
	}
	
	if key == "" {
		h.sendRowDescription(conn, []string{"key", "value"})
		
		count := 0
		h.cache.Iterate(func(entry *cache.Entry) bool {
			if strings.HasPrefix(string(entry.Key()), table+":") {
				h.sendDataRow(conn, [][]byte{
					entry.Key(),
					entry.Value(),
				})
				count++
			}
			return true
		})
		
		h.sendCommandComplete(conn, fmt.Sprintf("SELECT %d", count))
	} else {
		fullKey := table + ":" + key
		entry, found := h.cache.Load([]byte(fullKey))
		
		if found {
			h.sendRowDescription(conn, []string{"key", "value"})
			h.sendDataRow(conn, [][]byte{
				[]byte(key),
				entry.Value(),
			})
			h.sendCommandComplete(conn, "SELECT 1")
		} else {
			h.sendRowDescription(conn, []string{"key", "value"})
			h.sendCommandComplete(conn, "SELECT 0")
		}
	}
}

func (h *PostgresHandler) handleInsert(conn net.Conn, query string) {
	parts := strings.Fields(query)
	if len(parts) < 5 || parts[1] != "INTO" {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	table := parts[2]
	
	valuesIdx := -1
	for i, part := range parts {
		if part == "VALUES" {
			valuesIdx = i
			break
		}
	}
	
	if valuesIdx < 0 || valuesIdx+1 >= len(parts) {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	values := strings.Join(parts[valuesIdx+1:], " ")
	values = strings.Trim(values, "()")
	valueParts := strings.Split(values, ",")
	
	if len(valueParts) < 2 {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	key := strings.TrimSpace(strings.Trim(valueParts[0], "'\""))
	value := strings.TrimSpace(strings.Trim(valueParts[1], "'\""))
	
	fullKey := table + ":" + key
	h.cache.Store([]byte(fullKey), []byte(value), nil)
	
	h.sendCommandComplete(conn, "INSERT 0 1")
}

func (h *PostgresHandler) handleUpdate(conn net.Conn, query string) {
	parts := strings.Fields(query)
	if len(parts) < 6 || parts[2] != "SET" {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	table := parts[1]
	
	whereIdx := -1
	for i, part := range parts {
		if part == "WHERE" {
			whereIdx = i
			break
		}
	}
	
	if whereIdx < 0 || whereIdx+3 >= len(parts) {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	key := strings.Trim(parts[whereIdx+3], "'\"")
	setValue := strings.Join(parts[3:whereIdx], " ")
	valueParts := strings.Split(setValue, "=")
	
	if len(valueParts) < 2 {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	value := strings.TrimSpace(strings.Trim(valueParts[1], "'\""))
	
	fullKey := table + ":" + key
	entry, found := h.cache.Load([]byte(fullKey))
	
	if found {
		h.cache.Store([]byte(fullKey), []byte(value), &cache.StoreOptions{
			Flags: entry.Flags(),
		})
		h.sendCommandComplete(conn, "UPDATE 1")
	} else {
		h.sendCommandComplete(conn, "UPDATE 0")
	}
}

func (h *PostgresHandler) handleDelete(conn net.Conn, query string) {
	parts := strings.Fields(query)
	if len(parts) < 6 || parts[1] != "FROM" {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	table := parts[2]
	
	whereIdx := -1
	for i, part := range parts {
		if part == "WHERE" {
			whereIdx = i
			break
		}
	}
	
	if whereIdx < 0 || whereIdx+3 >= len(parts) {
		h.sendErrorResponse(conn, "42601", "syntax error")
		return
	}
	
	key := strings.Trim(parts[whereIdx+3], "'\"")
	fullKey := table + ":" + key
	
	if h.cache.Delete([]byte(fullKey)) {
		h.sendCommandComplete(conn, "DELETE 1")
	} else {
		h.sendCommandComplete(conn, "DELETE 0")
	}
}

func (h *PostgresHandler) readMessage(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}
	
	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:]) - 4
	
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return 0, nil, err
	}
	
	return msgType, data, nil
}

func (h *PostgresHandler) sendMessage(conn net.Conn, msgType byte, data []byte) error {
	buf := make([]byte, 5+len(data))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(data)))
	copy(buf[5:], data)
	
	_, err := conn.Write(buf)
	return err
}

func (h *PostgresHandler) sendAuthenticationOk(conn net.Conn) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 0)
	h.sendMessage(conn, 'R', data)
}

func (h *PostgresHandler) sendAuthenticationCleartextPassword(conn net.Conn) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 3)
	h.sendMessage(conn, 'R', data)
}

func (h *PostgresHandler) sendReadyForQuery(conn net.Conn) {
	h.sendMessage(conn, 'Z', []byte{'I'})
}

func (h *PostgresHandler) sendErrorResponse(conn net.Conn, code, message string) {
	var buf bytes.Buffer
	buf.WriteByte('S')
	buf.WriteString("ERROR")
	buf.WriteByte(0)
	buf.WriteByte('C')
	buf.WriteString(code)
	buf.WriteByte(0)
	buf.WriteByte('M')
	buf.WriteString(message)
	buf.WriteByte(0)
	buf.WriteByte(0)
	
	h.sendMessage(conn, 'E', buf.Bytes())
}

func (h *PostgresHandler) sendRowDescription(conn net.Conn, columns []string) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int16(len(columns)))
	
	for _, col := range columns {
		buf.WriteString(col)
		buf.WriteByte(0)
		binary.Write(&buf, binary.BigEndian, int32(0))
		binary.Write(&buf, binary.BigEndian, int16(0))
		binary.Write(&buf, binary.BigEndian, int32(25))
		binary.Write(&buf, binary.BigEndian, int16(-1))
		binary.Write(&buf, binary.BigEndian, int32(-1))
		binary.Write(&buf, binary.BigEndian, int16(0))
	}
	
	h.sendMessage(conn, 'T', buf.Bytes())
}

func (h *PostgresHandler) sendDataRow(conn net.Conn, values [][]byte) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int16(len(values)))
	
	for _, value := range values {
		binary.Write(&buf, binary.BigEndian, int32(len(value)))
		buf.Write(value)
	}
	
	h.sendMessage(conn, 'D', buf.Bytes())
}

func (h *PostgresHandler) sendCommandComplete(conn net.Conn, tag string) {
	data := append([]byte(tag), 0)
	h.sendMessage(conn, 'C', data)
}