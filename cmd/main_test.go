package main

import (
	"testing"

	"github.com/grumpylabs/gopogo/internal/sysmem"
)

func TestParseMemorySize(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"0", 0},
		{"unlimited", 0},
		{"1024", 1024},
		{"512MB", 512 << 20},
		{"512mb", 512 << 20},
		{"512m", 512 << 20},
		{"1.5GB", 3 << 29},
		{"2 g", 2 << 30},
		{"1T", 1 << 40},
		{"4k", 4096},
	}
	for _, tt := range tests {
		got, err := parseMemorySize(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("%q: got %d, %v; want %d", tt.in, got, err, tt.want)
		}
	}
	for _, bad := range []string{"abc", "-1GB", "10XB", "%", "1.2.3"} {
		if _, err := parseMemorySize(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
	if avail := sysmem.Available(); avail > 0 {
		got, err := parseMemorySize("50%")
		if err != nil || got != avail/2 {
			t.Errorf("50%%: got %d, %v; want %d", got, err, avail/2)
		}
	}
}
