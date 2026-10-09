package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPprofRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", ":0", "10.1.2.3:6060", "example.com:6060", "localhost"} {
		if _, err := startPprof(addr); err == nil {
			t.Errorf("startPprof(%q) succeeded, want an error", addr)
		}
	}
}

func TestPprofServesProfiles(t *testing.T) {
	addr, err := startPprof("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + addr.String() + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "goroutine profile") {
		t.Fatalf("status %d, body %.80q", resp.StatusCode, body)
	}
}
