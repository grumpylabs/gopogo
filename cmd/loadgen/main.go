// Command loadgen exercises a gopogo server over RESP with a mix of reads,
// writes, counters, expirations, misses and a few deliberate errors, at a
// target rate, printing throughput and latency as it runs.
//
//	gopogo-loadgen --addr 127.0.0.1:6379 --auth s3cret --rate 500 --duration 2m
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type op struct {
	name   string
	weight int
	args   func(r *rand.Rand) []string
}

var rootCmd = &cobra.Command{
	Use:   "gopogo-loadgen",
	Short: "Drive a gopogo server with a mixed RESP workload",
	Long: `gopogo-loadgen drives a gopogo server over RESP with a weighted mix of
SET, SET EX, GET, MGET, INCR, APPEND, EXPIRE, DEL, TTL and a few deliberate
errors, at a target rate, printing throughput and latency every 5 seconds.

Flags can also be set as GOPOGO_LOADGEN_<FLAG> environment variables
(e.g. GOPOGO_LOADGEN_RATE); the password also falls back to GOPOGO_AUTH.`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			return fmt.Errorf("unexpected argument %q", args[0])
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) { run() },
}

func init() {
	f := rootCmd.Flags()
	f.String("addr", "127.0.0.1:6379", "server address")
	f.String("auth", "", "password (default $GOPOGO_LOADGEN_AUTH, else $GOPOGO_AUTH)")
	f.Int("workers", 8, "concurrent connections")
	f.Int("rate", 500, "total commands per second (0 = as fast as possible)")
	f.Duration("duration", time.Minute, "how long to run (0 = until interrupted)")
	f.Int("keys", 10000, "key space size")
	f.Int("min-value", 16, "minimum value size in bytes")
	f.Int("max-value", 1024, "maximum value size in bytes")
	f.String("prefix", "load:", "key prefix")
	viper.BindPFlags(f)
	viper.SetEnvPrefix("GOPOGO_LOADGEN")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run() {
	addr := viper.GetString("addr")
	auth := viper.GetString("auth")
	if auth == "" {
		auth = os.Getenv("GOPOGO_AUTH")
	}
	workers := viper.GetInt("workers")
	rate := viper.GetInt("rate")
	duration := viper.GetDuration("duration")
	keys := viper.GetInt("keys")
	minVal := viper.GetInt("min-value")
	maxVal := viper.GetInt("max-value")
	prefix := viper.GetString("prefix")
	if workers < 1 || keys < 1 || rate < 0 || minVal < 0 || maxVal < minVal {
		fmt.Fprintln(os.Stderr, "invalid settings: need workers>=1, keys>=1, rate>=0, 0<=min-value<=max-value")
		os.Exit(1)
	}

	key := func(r *rand.Rand) string { return prefix + strconv.Itoa(r.IntN(keys)) }
	value := func(r *rand.Rand) string {
		n := minVal
		if maxVal > minVal {
			n += r.IntN(maxVal - minVal)
		}
		return strings.Repeat(string(rune('a'+r.IntN(26))), n)
	}
	ops := []op{
		{"SET", 28, func(r *rand.Rand) []string { return []string{"SET", key(r), value(r)} }},
		{"SET EX", 6, func(r *rand.Rand) []string {
			return []string{"SET", key(r), value(r), "EX", strconv.Itoa(5 + r.IntN(120))}
		}},
		{"GET", 30, func(r *rand.Rand) []string { return []string{"GET", key(r)} }},
		{"MGET", 5, func(r *rand.Rand) []string {
			return []string{"MGET", key(r), key(r), key(r), key(r), key(r)}
		}},
		{"INCR", 8, func(r *rand.Rand) []string {
			return []string{"INCR", prefix + "counter:" + strconv.Itoa(r.IntN(100))}
		}},
		{"APPEND", 4, func(r *rand.Rand) []string { return []string{"APPEND", key(r), "+"} }},
		{"EXPIRE", 4, func(r *rand.Rand) []string { return []string{"EXPIRE", key(r), strconv.Itoa(10 + r.IntN(60))} }},
		{"DEL", 5, func(r *rand.Rand) []string { return []string{"DEL", key(r)} }},
		{"TTL", 3, func(r *rand.Rand) []string { return []string{"TTL", key(r)} }},
		// Deliberate errors: INCR on a non-numeric value and an unknown command.
		{"INCR (bad)", 1, func(r *rand.Rand) []string { return []string{"INCR", prefix + "not-a-number"} }},
		{"BOGUS", 1, func(r *rand.Rand) []string { return []string{"BOGUS", key(r)} }},
	}
	total := 0
	for _, o := range ops {
		total += o.weight
	}
	pick := func(r *rand.Rand) op {
		n := r.IntN(total)
		for _, o := range ops {
			if n < o.weight {
				return o
			}
			n -= o.weight
		}
		return ops[0]
	}

	var sent, failed, connErrs atomic.Int64
	var mu sync.Mutex
	var latencies []time.Duration

	stop := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	var timeout <-chan time.Time
	if duration > 0 {
		timeout = time.After(duration)
	}
	go func() {
		select {
		case <-interrupt:
		case <-timeout:
		}
		close(stop)
	}()

	// Seed the non-numeric key used for INCR errors.
	if c, err := dial(addr, auth); err == nil {
		c.do("SET", prefix+"not-a-number", "abc")
		c.close()
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
			var tick <-chan time.Time
			if rate > 0 {
				per := time.Duration(float64(time.Second) * float64(workers) / float64(rate))
				t := time.NewTicker(per)
				defer t.Stop()
				tick = t.C
			}
			var c *client
			for {
				select {
				case <-stop:
					if c != nil {
						c.close()
					}
					return
				default:
				}
				if tick != nil {
					select {
					case <-stop:
						if c != nil {
							c.close()
						}
						return
					case <-tick:
					}
				}
				if c == nil {
					var err error
					if c, err = dial(addr, auth); err != nil {
						connErrs.Add(1)
						time.Sleep(time.Second)
						continue
					}
				}
				o := pick(r)
				start := time.Now()
				reply, err := c.do(o.args(r)...)
				lat := time.Since(start)
				if err != nil && !errors.As(err, new(serverError)) {
					connErrs.Add(1)
					c.close()
					c = nil
					continue
				}
				sent.Add(1)
				if err != nil || strings.HasPrefix(reply, "-") {
					failed.Add(1)
				}
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}(uint64(w) + uint64(time.Now().UnixNano()))
	}

	start := time.Now()
	report := func(final bool) {
		mu.Lock()
		l := latencies
		latencies = nil
		mu.Unlock()
		slices.Sort(l)
		pct := func(p float64) time.Duration {
			if len(l) == 0 {
				return 0
			}
			return l[min(len(l)-1, int(float64(len(l))*p))]
		}
		elapsed := time.Since(start).Round(time.Second)
		label := "   "
		if final {
			label = "end"
		}
		fmt.Printf("%s %6s  sent=%-8d errors=%-6d conn_errs=%-4d interval: n=%-6d p50=%-8s p99=%-8s max=%s\n",
			label, elapsed, sent.Load(), failed.Load(), connErrs.Load(), len(l),
			pct(0.5).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(1).Round(time.Microsecond))
	}
	fmt.Printf("loadgen: %s, %d workers, rate %d/s, %s, %d keys\n", addr, workers, rate, duration, keys)
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		select {
		case <-t.C:
			report(false)
		case <-done:
			report(true)
			return
		}
	}
}

// serverError is an error reply from the server; the connection is fine.
type serverError string

func (e serverError) Error() string { return string(e) }

type client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func dial(addr, auth string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c := &client{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}
	if auth != "" {
		if _, err := c.do("AUTH", auth); err != nil {
			conn.Close()
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	return c, nil
}

func (c *client) close() { c.conn.Close() }

// do sends a command and reads one reply, returned in a compact text form.
func (c *client) do(args ...string) (string, error) {
	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
	if err := c.w.Flush(); err != nil {
		return "", err
	}
	return c.read()
}

func (c *client) read() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return "", io.ErrUnexpectedEOF
	}
	switch line[0] {
	case '+', ':':
		return line, nil
	case '-':
		return line, serverError(line[1:])
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "$-1", nil
		}
		if _, err := io.CopyN(io.Discard, c.r, int64(n)+2); err != nil {
			return "", err
		}
		return line, nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		for i := 0; i < n; i++ {
			if _, err := c.read(); err != nil && !errors.As(err, new(serverError)) {
				return "", err
			}
		}
		return line, nil
	}
	return "", fmt.Errorf("unexpected reply %q", line)
}
