package protocol

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// monitorHub fans commands out to MONITOR clients, for every protocol, in
// pogocache's format:
//
//	+1712345678.123456 [0 127.0.0.1:52345] "SET" "key" "value"
type monitorHub struct {
	active atomic.Int32
	mu     sync.Mutex
	subs   map[chan string]struct{}
}

var monitors = &monitorHub{subs: map[chan string]struct{}{}}

// monitorQueue is how many lines a slow MONITOR client may fall behind before
// lines are dropped; publishing never blocks command execution.
const monitorQueue = 1024

func (m *monitorHub) subscribe() chan string {
	ch := make(chan string, monitorQueue)
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	m.mu.Unlock()
	m.active.Add(1)
	return ch
}

func (m *monitorHub) unsubscribe(ch chan string) {
	m.mu.Lock()
	delete(m.subs, ch)
	m.mu.Unlock()
	m.active.Add(-1)
}

// publish reports a command. AUTH, QUIT and MONITOR are never shown.
func (m *monitorHub) publish(addr string, args []string) {
	if m.active.Load() == 0 || len(args) == 0 {
		return
	}
	switch strings.ToUpper(args[0]) {
	case "AUTH", "QUIT", "MONITOR":
		return
	}
	line := formatMonitorLine(time.Now(), addr, args)
	m.mu.Lock()
	for ch := range m.subs {
		select {
		case ch <- line:
		default: // subscriber is behind: drop
		}
	}
	m.mu.Unlock()
}

func formatMonitorLine(now time.Time, addr string, args []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "+%d.%06d [0 %s]", now.Unix(), now.Nanosecond()/1000, addr)
	for _, a := range args {
		b.WriteString(` "`)
		for i := 0; i < len(a); i++ {
			switch c := a[i]; c {
			case '\n':
				b.WriteString(`\n`)
			case '\r':
				b.WriteString(`\r`)
			case '\t':
				b.WriteString(`\t`)
			case '"':
				b.WriteString(`\"`)
			case '\\':
				b.WriteString(`\\`)
			default:
				if c < 32 || c >= 127 {
					b.WriteString(`\x` + strings.ToUpper(strconv.FormatUint(uint64(c)|0x100, 16)[1:]))
				} else {
					b.WriteByte(c)
				}
			}
		}
		b.WriteByte('"')
	}
	b.WriteString("\r\n")
	return b.String()
}
