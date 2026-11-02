// Command loadgen exercises a gopogo server over RESP or the memcache text
// protocol with a mix of reads,
// writes, counters, expirations, misses and a few deliberate errors, at a
// target rate, printing throughput and latency as it runs.
//
//	gopogo-loadgen --addr 127.0.0.1:6379 --auth s3cret --rate 500 --duration 2m
//
// With --telemetry each operation is a trace: a root span for the operation
// (e.g. read-through), a client span per command, and gopogo's server span
// under each, joined by pipelining TRACEPARENT ahead of every command.
package main

import (
	"bufio"
	"context"
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

	"github.com/grumpylabs/gopogo/internal/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// Set by the Makefile's -ldflags.
var (
	version = "dev"
	commit  = "dev"
)

const instrumentationName = "github.com/grumpylabs/gopogo/cmd/loadgen"

// An op is one unit of load: one or more commands, traced together.
type op struct {
	name   string
	weight int
	run    func(ctx context.Context, r *rand.Rand, c *client) error
}

var rootCmd = &cobra.Command{
	Use:   "gopogo-loadgen",
	Short: "Drive a gopogo server with a mixed RESP or memcache workload",
	Long: `gopogo-loadgen drives a gopogo server over RESP (--protocol redis) or the
memcache text protocol (--protocol memcache) with a weighted mix of
operations (read-through GET then SET on a miss, writes, batch reads,
counters, sessions, expirations, deletes and a few deliberate errors) at a
target rate, printing throughput and latency every 5 seconds.

With --telemetry each operation is exported as a trace: a span for the
operation, a client span per command, and the server's span for each command
beneath it (gopogo continues the trace from a pipelined TRACEPARENT, or a
traceparent line over memcache). memcache cannot authenticate, so --auth
applies to redis only.

Flags can also be set as GOPOGO_LOADGEN_<FLAG> environment variables
(e.g. GOPOGO_LOADGEN_RATE); the password also falls back to GOPOGO_AUTH.
The standard OTEL_EXPORTER_OTLP_* and OTEL_RESOURCE_ATTRIBUTES variables apply.`,
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
	f.String("protocol", "redis", "wire protocol: redis or memcache")
	f.String("auth", "", "password (default $GOPOGO_LOADGEN_AUTH, else $GOPOGO_AUTH)")
	f.Int("workers", 8, "concurrent connections")
	f.Int("rate", 500, "total operations per second, each one to four commands (0 = as fast as possible)")
	f.Duration("duration", time.Minute, "how long to run (0 = until interrupted)")
	f.Int("keys", 10000, "key space size")
	f.Int("min-value", 16, "minimum value size in bytes")
	f.Int("max-value", 1024, "maximum value size in bytes")
	f.String("prefix", "load:", "key prefix")
	f.Bool("telemetry", false, "Trace every operation and export the spans")
	f.String("traces-exporter", "otlp", "Traces exporter: otlp or stdout")
	f.String("otlp-protocol", "", "OTLP protocol: grpc or http (default OTEL_EXPORTER_OTLP_PROTOCOL, else grpc)")
	f.String("otlp-endpoint", "", "OTLP endpoint: host:port, or a base URL such as https://collector/prefix (default OTEL_EXPORTER_OTLP_ENDPOINT, else localhost)")
	f.Bool("otlp-insecure", true, "Plaintext OTLP to a host:port endpoint; a URL endpoint's scheme decides")
	f.String("otlp-headers", "", "OTLP request headers as key=value,... (default OTEL_EXPORTER_OTLP_HEADERS)")
	f.String("telemetry-environment", "", "deployment.environment resource attribute")
	f.Float64("trace-sample-ratio", 1.0, "Fraction of operations traced (0-1)")
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
	proto := viper.GetString("protocol")
	if proto != "redis" && proto != "memcache" {
		fmt.Fprintf(os.Stderr, "invalid --protocol %q: want redis or memcache\n", proto)
		os.Exit(1)
	}
	if proto == "memcache" {
		auth = "" // memcache has no way to authenticate
	}
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
	ttl := func(r *rand.Rand, lo, n int) string { return strconv.Itoa(lo + r.IntN(n)) }
	// Each op's commands go out one at a time, so a later command can
	// depend on an earlier reply, as in a real cache client.
	ops := []op{
		// Cache-aside: GET, and on a miss fill the key with a TTL.
		{"read-through", 30, func(ctx context.Context, r *rand.Rand, c *client) error {
			k := key(r)
			reply, err := c.cmd(ctx, "GET", k)
			if err != nil || reply != "$-1" {
				return err
			}
			_, err = c.cmd(ctx, "SET", k, value(r), "EX", ttl(r, 5, 120))
			return err
		}},
		{"write", 24, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "SET", key(r), value(r))
			return err
		}},
		{"write-ttl", 6, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "SET", key(r), value(r), "EX", ttl(r, 5, 120))
			return err
		}},
		{"batch-read", 5, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "MGET", key(r), key(r), key(r), key(r), key(r))
			return err
		}},
		{"counter", 8, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "INCR", prefix+"counter:"+strconv.Itoa(r.IntN(100)))
			return err
		}},
		// A user session: load it (creating it on a miss), count the hit
		// and keep the counter alive while the session is.
		{"session", 6, func(ctx context.Context, r *rand.Rand, c *client) error {
			user := prefix + "user:" + strconv.Itoa(r.IntN(500))
			reply, err := c.cmd(ctx, "GET", user+":session")
			if err != nil {
				return err
			}
			if reply == "$-1" {
				if _, err := c.cmd(ctx, "SET", user+":session", value(r), "EX", "300"); err != nil {
					return err
				}
			}
			if _, err := c.cmd(ctx, "INCR", user+":hits"); err != nil {
				return err
			}
			_, err = c.cmd(ctx, "EXPIRE", user+":hits", "300")
			return err
		}},
		{"append", 4, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "APPEND", key(r), "+")
			return err
		}},
		// Extend a key's life and read back what is left.
		{"refresh", 6, func(ctx context.Context, r *rand.Rand, c *client) error {
			k := key(r)
			if _, err := c.cmd(ctx, "EXPIRE", k, ttl(r, 10, 60)); err != nil {
				return err
			}
			_, err := c.cmd(ctx, "TTL", k)
			return err
		}},
		{"invalidate", 5, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "DEL", key(r))
			return err
		}},
		// Deliberate errors: INCR on a non-numeric value and an unknown command.
		{"bad-counter", 1, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "INCR", prefix+"not-a-number")
			return err
		}},
		{"bogus", 1, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "BOGUS", key(r))
			return err
		}},
	}
	notNumber := []string{"SET", prefix + "not-a-number", "abc"}
	if proto == "memcache" {
		ops = memcacheOps(key, value, ttl, prefix)
		notNumber = []string{"set", prefix + "not-a-number", "0", "0", "abc"}
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

	headers, err := telemetry.ParseHeaders(viper.GetString("otlp-headers"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --otlp-headers: %v\n", err)
		os.Exit(1)
	}
	tracer, err := telemetry.NewTracer(context.Background(), &telemetry.Config{
		Enabled:        viper.GetBool("telemetry"),
		TracesExporter: viper.GetString("traces-exporter"),
		Protocol:       viper.GetString("otlp-protocol"),
		OTLPEndpoint:   viper.GetString("otlp-endpoint"),
		Insecure:       viper.GetBool("otlp-insecure"),
		Headers:        headers,
		ServiceName:    "gopogo-loadgen",
		ServiceVersion: version,
		Environment:    viper.GetString("telemetry-environment"),
		SampleRatio:    viper.GetFloat64("trace-sample-ratio"),
		SampleRatioSet: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "telemetry: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tracer.Shutdown(ctx)
	}()
	opTracer := otel.Tracer(instrumentationName)

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
	if c, err := dial(addr, proto, auth); err == nil {
		c.do(notNumber...)
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
					if c, err = dial(addr, proto, auth); err != nil {
						connErrs.Add(1)
						time.Sleep(time.Second)
						continue
					}
				}
				o := pick(r)
				start := time.Now()
				ctx, span := opTracer.Start(context.Background(), o.name)
				err := o.run(ctx, r, c)
				lat := time.Since(start)
				if err != nil {
					span.SetStatus(codes.Error, err.Error())
				}
				span.End()
				if err != nil && !errors.As(err, new(serverError)) {
					connErrs.Add(1)
					c.close()
					c = nil
					continue
				}
				sent.Add(1)
				if err != nil {
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
		fmt.Printf("%s %6s  ops=%-8d errors=%-6d conn_errs=%-4d interval: n=%-6d p50=%-8s p99=%-8s max=%s\n",
			label, elapsed, sent.Load(), failed.Load(), connErrs.Load(), len(l),
			pct(0.5).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(1).Round(time.Microsecond))
	}
	fmt.Printf("loadgen: %s %s, %d workers, rate %d/s, %s, %d keys\n", proto, addr, workers, rate, duration, keys)
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
	host string // server.address and server.port for client spans
	port int
	mc   bool // memcache text protocol, else RESP
}

// splitAddr splits host:port for span attributes.
func splitAddr(addr string) (string, int) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	n, _ := strconv.Atoi(p)
	return h, n
}

func dial(addr, proto, auth string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c := &client{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}
	c.host, c.port = splitAddr(addr)
	c.mc = proto == "memcache"
	if auth != "" {
		if _, err := c.do("AUTH", auth); err != nil {
			conn.Close()
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	return c, nil
}

func (c *client) close() { c.conn.Close() }

// cmd runs one command of an op in a client span under ctx. When the span
// is sampled, TRACEPARENT is pipelined ahead of the command so the server's
// span joins the trace; its own reply is read and dropped.
func (c *client) cmd(ctx context.Context, args ...string) (string, error) {
	name, protoName := strings.ToUpper(args[0]), "redis"
	if c.mc {
		name, protoName = args[0], "memcache"
	}
	attrs := []attribute.KeyValue{
		semconv.DBSystemNameKey.String("gopogo"),
		semconv.DBOperationName(name),
		semconv.NetworkProtocolName(protoName),
		semconv.ServerAddress(c.host),
	}
	if c.port != 0 {
		attrs = append(attrs, semconv.ServerPort(c.port))
	}
	ctx, span := otel.Tracer(instrumentationName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	defer span.End()

	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	traced := span.SpanContext().IsSampled()
	if traced {
		carrier := propagation.MapCarrier{}
		propagation.TraceContext{}.Inject(ctx, carrier)
		if c.mc {
			c.write("traceparent", carrier["traceparent"])
		} else {
			c.write("TRACEPARENT", carrier["traceparent"])
		}
	}
	c.write(args...)
	if err := c.w.Flush(); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	if traced {
		// A server without TRACEPARENT answers with an error; the command
		// still runs, untraced on that side.
		if _, err := c.read(); err != nil && !errors.As(err, new(serverError)) {
			span.SetStatus(codes.Error, err.Error())
			return "", err
		}
	}
	reply, err := c.read()
	if err != nil {
		var se serverError
		if errors.As(err, &se) {
			span.SetAttributes(semconv.ErrorTypeKey.String(strings.SplitN(string(se), " ", 2)[0]))
		}
		span.SetStatus(codes.Error, err.Error())
	}
	return reply, err
}

// do sends a command and reads one reply, returned in a compact text form.
func (c *client) do(args ...string) (string, error) {
	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	c.write(args...)
	if err := c.w.Flush(); err != nil {
		return "", err
	}
	return c.read()
}

// write buffers one command. A memcache storage command's last argument is
// its data block: set <key> <flags> <exptime> <data>.
func (c *client) write(args ...string) {
	if c.mc {
		if mcStorage[args[0]] {
			data := args[len(args)-1]
			fmt.Fprintf(c.w, "%s %d\r\n%s\r\n", strings.Join(args[:len(args)-1], " "), len(data), data)
		} else {
			fmt.Fprintf(c.w, "%s\r\n", strings.Join(args, " "))
		}
		return
	}
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
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
	if c.mc {
		return c.readMemcache(line)
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

// mcStorage are the memcache commands followed by a data block.
var mcStorage = map[string]bool{"set": true, "add": true, "replace": true, "append": true, "prepend": true, "cas": true}

// readMemcache reads the rest of a memcache reply whose first line is line.
// A retrieval returns its first line: "VALUE ..." on a hit, "END" on a miss.
func (c *client) readMemcache(line string) (string, error) {
	if line == "ERROR" || strings.HasPrefix(line, "CLIENT_ERROR") || strings.HasPrefix(line, "SERVER_ERROR") {
		return line, serverError(line)
	}
	first := line
	for strings.HasPrefix(line, "VALUE ") {
		f := strings.Fields(line)
		n, err := strconv.Atoi(f[len(f)-1])
		if len(f) == 5 { // gets: VALUE <key> <flags> <bytes> <cas>
			n, err = strconv.Atoi(f[3])
		}
		if err != nil {
			return "", fmt.Errorf("bad VALUE line %q", line)
		}
		if _, err := io.CopyN(io.Discard, c.r, int64(n)+2); err != nil {
			return "", err
		}
		if line, err = c.r.ReadString('\n'); err != nil {
			return "", err
		}
		line = strings.TrimSuffix(line, "\r\n")
	}
	return first, nil
}

// memcacheOps is the redis workload's mix over the memcache text protocol.
// memcache has no TTL query, and incr on a missing key is NOT_FOUND, so
// counters are created with add on a miss.
func memcacheOps(key, value func(*rand.Rand) string, ttl func(*rand.Rand, int, int) string, prefix string) []op {
	incr := func(ctx context.Context, c *client, k, exptime string) error {
		reply, err := c.cmd(ctx, "incr", k, "1")
		if err != nil || reply != "NOT_FOUND" {
			return err
		}
		_, err = c.cmd(ctx, "add", k, "0", exptime, "1")
		return err
	}
	return []op{
		{"read-through", 30, func(ctx context.Context, r *rand.Rand, c *client) error {
			k := key(r)
			reply, err := c.cmd(ctx, "get", k)
			if err != nil || reply != "END" {
				return err
			}
			_, err = c.cmd(ctx, "set", k, "0", ttl(r, 5, 120), value(r))
			return err
		}},
		{"write", 24, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "set", key(r), "0", "0", value(r))
			return err
		}},
		{"write-ttl", 6, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "set", key(r), "0", ttl(r, 5, 120), value(r))
			return err
		}},
		{"batch-read", 5, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "get", key(r), key(r), key(r), key(r), key(r))
			return err
		}},
		{"counter", 8, func(ctx context.Context, r *rand.Rand, c *client) error {
			return incr(ctx, c, prefix+"counter:"+strconv.Itoa(r.IntN(100)), "0")
		}},
		{"session", 6, func(ctx context.Context, r *rand.Rand, c *client) error {
			user := prefix + "user:" + strconv.Itoa(r.IntN(500))
			reply, err := c.cmd(ctx, "get", user+":session")
			if err != nil {
				return err
			}
			if reply == "END" {
				if _, err := c.cmd(ctx, "set", user+":session", "0", "300", value(r)); err != nil {
					return err
				}
			}
			if err := incr(ctx, c, user+":hits", "300"); err != nil {
				return err
			}
			_, err = c.cmd(ctx, "touch", user+":hits", "300")
			return err
		}},
		{"append", 4, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "append", key(r), "0", "0", "+")
			return err
		}},
		// Extend a key's life and read it back.
		{"refresh", 6, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "gat", ttl(r, 10, 60), key(r))
			return err
		}},
		{"invalidate", 5, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "delete", key(r))
			return err
		}},
		// Deliberate errors: incr on a non-numeric value and an unknown command.
		{"bad-counter", 1, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "incr", prefix+"not-a-number", "1")
			return err
		}},
		{"bogus", 1, func(ctx context.Context, r *rand.Rand, c *client) error {
			_, err := c.cmd(ctx, "bogus", key(r))
			return err
		}},
	}
}
