package protocol

import (
	"net"
	"testing"
)

func TestDetect(t *testing.T) {
	tests := []struct {
		input string
		want  Type
	}{
		{"*1\r\n$4\r\nPING\r\n", TypeRedis},
		{"PING\r\n", TypeRedis},
		{"  SET k v\r\n", TypeRedis},
		{"\x00\x00\x00\x08\x04\xd2\x16\x2f", TypePostgres}, // SSLRequest
		{"\x00\x00\x00\x29\x00\x03\x00\x00user", TypePostgres},
		{"GET /key HTTP/1.1\r\nHost: x\r\n\r\n", TypeHTTP},
		{"PUT /key HTTP/1.0\r\n", TypeHTTP},
		{"OPTIONS * HTTP/1.1\r\n", TypeHTTP},
		{"get key\r\n", TypeMemcache},
		{"gets key\r\n", TypeMemcache},
		{"cas k 0 0 1 5\r\nx\r\n", TypeMemcache},
		{"append k 0 0 1\r\nx\r\n", TypeMemcache},
		{"touch k 10\r\n", TypeMemcache},
		{"quit\r\n", TypeMemcache},
	}
	for _, tt := range tests {
		server, client := net.Pipe()
		go func() {
			client.Write([]byte(tt.input))
			client.Close()
		}()
		got, err := NewDetector(server).Detect()
		server.Close()
		if err != nil {
			t.Fatalf("%q: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("%q: got %v, want %v", tt.input, got, tt.want)
		}
	}
}
