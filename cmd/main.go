package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"runtime"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
	"github.com/grumpylabs/gopogo/internal/server"
	"github.com/grumpylabs/gopogo/internal/sysmem"
	"github.com/grumpylabs/gopogo/internal/telemetry"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version = "1.0.0"
	commit  = "dev"
)

var rootCmd = &cobra.Command{
	Use:   "gopogo",
	Short: "High-performance caching server",
	Long: `Gopogo is a fast caching software built from scratch with a focus
on low latency and cpu efficiency. It supports multiple protocols including
HTTP, Redis, Memcache, and Postgres.`,
	Run: runServer,
	// Reject stray arguments: with pogocache-style "--cas no", cobra parses
	// --cas as true and "no" as an argument, which would be silently ignored.
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			return fmt.Errorf("unexpected argument %q: boolean flags take =true or =false, e.g. --cas=false", args[0])
		}
		return nil
	},
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().String("host", "127.0.0.1", "Listening hostname")
	rootCmd.PersistentFlags().IntP("port", "p", 6379, "Listening port")
	rootCmd.PersistentFlags().StringP("socket", "s", "", "Unix socket path")
	rootCmd.PersistentFlags().String("auth", "", "Authentication password")
	rootCmd.PersistentFlags().String("persist", "", "Persistence file to load at startup and save at shutdown")

	rootCmd.PersistentFlags().Int("threads", 0, "Number of OS threads running Go code (GOMAXPROCS); 0 uses Go's default, which honors container CPU limits")
	rootCmd.PersistentFlags().Int("shards", 16, "Number of cache shards")
	rootCmd.PersistentFlags().String("maxmemory", "80%", "Maximum memory: bytes with k/m/g/t suffix (e.g. 1GB), a percentage of available memory (e.g. 80%), or 0 for unlimited")
	rootCmd.PersistentFlags().String("evict", "yes", "Evict keys when maxmemory is reached (yes/no); no rejects writes instead")
	rootCmd.PersistentFlags().Bool("autosweep", true, "Enable automatic background sweeping of evicted entries")
	rootCmd.PersistentFlags().Duration("sweepinterval", 10*time.Second, "Interval for automatic background sweeping")

	rootCmd.PersistentFlags().Int("tlsport", 0, "TLS listening port")
	rootCmd.PersistentFlags().String("tlscert", "", "TLS certificate file")
	rootCmd.PersistentFlags().String("tlskey", "", "TLS key file")
	rootCmd.PersistentFlags().String("tlscacert", "", "TLS CA certificate file for verifying client certificates")
	rootCmd.PersistentFlags().Int("maxconns", 1024, "Maximum client connections")
	rootCmd.PersistentFlags().Int("backlog", 1024, "Listen accept backlog")
	rootCmd.PersistentFlags().Bool("reuseport", false, "Set SO_REUSEPORT on listening sockets")
	rootCmd.PersistentFlags().Bool("tcpnodelay", true, "Disable Nagle's algorithm")
	rootCmd.PersistentFlags().Bool("quickack", false, "Enable TCP quick acks (Linux)")

	rootCmd.PersistentFlags().Bool("http", false, "Enable HTTP protocol")
	rootCmd.PersistentFlags().Bool("memcache", false, "Enable Memcache protocol")
	rootCmd.PersistentFlags().Bool("postgres", false, "Enable Postgres protocol")
	rootCmd.PersistentFlags().Bool("redis", true, "Enable Redis protocol")

	rootCmd.PersistentFlags().String("config", "", "Config file path")
	rootCmd.PersistentFlags().Bool("quiet", false, "Quiet mode")
	rootCmd.PersistentFlags().Bool("verbose", false, "Verbose output, including every telemetry export")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn or error. debug logs every command (see --debug-log-sample) and cache stats every 30s")
	rootCmd.PersistentFlags().Float64("debug-log-sample", 1.0, "Fraction of commands logged at debug level (0-1)")
	rootCmd.PersistentFlags().Bool("version", false, "Show version")

	rootCmd.PersistentFlags().Bool("telemetry", false, "Enable OpenTelemetry metrics, traces and logs")
	rootCmd.PersistentFlags().String("telemetry-exporter", "otlp", "Telemetry exporter (otlp, stdout)")
	rootCmd.PersistentFlags().String("otlp-protocol", "", "OTLP protocol: grpc or http (default OTEL_EXPORTER_OTLP_PROTOCOL, else grpc)")
	rootCmd.PersistentFlags().String("otlp-endpoint", "", "OTLP endpoint: host:port, or a base URL such as https://collector/prefix (default OTEL_EXPORTER_OTLP_ENDPOINT, else localhost)")
	rootCmd.PersistentFlags().Bool("otlp-insecure", true, "Plaintext OTLP to a host:port endpoint; a URL endpoint's scheme decides")
	rootCmd.PersistentFlags().String("otlp-headers", "", "OTLP request headers as key=value,... e.g. \"Authorization=Bearer <token>\" (default OTEL_EXPORTER_OTLP_HEADERS)")
	rootCmd.PersistentFlags().String("telemetry-environment", "", "deployment.environment resource attribute")
	rootCmd.PersistentFlags().Float64("trace-sample-ratio", 1.0, "Fraction of new traces to sample (0-1); a caller's sampling decision is respected")
	rootCmd.PersistentFlags().Bool("noevict", false, "Same as --evict=no")
	rootCmd.PersistentFlags().Bool("nosixpack", false, "Disable sixpack key compression")
	rootCmd.PersistentFlags().Int("loadfactor", 75, "Hashmap load factor percent (55-95)")
	rootCmd.PersistentFlags().Bool("cas", false, "Assign compare-and-swap tokens on every write")

	viper.BindPFlags(rootCmd.PersistentFlags())
}

func initConfig() {
	if cfgFile := viper.GetString("config"); cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("gopogo")
		viper.SetConfigType("yaml")
		viper.AddConfigPath("/etc/gopogo/")
		viper.AddConfigPath("$HOME/.gopogo")
		viper.AddConfigPath(".")
	}

	viper.SetEnvPrefix("GOPOGO")
	// Flags with hyphens read GOPOGO_ variables with underscores, e.g.
	// --telemetry-exporter from GOPOGO_TELEMETRY_EXPORTER.
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil && !viper.GetBool("quiet") {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func runServer(cmd *cobra.Command, args []string) {
	if viper.GetBool("version") {
		fmt.Printf("gopogo version %s (commit: %s)\n", version, commit)
		os.Exit(0)
	}

	protocol.Version = version
	protocol.Commit = commit
	validateFlags()
	protocol.ConnStats.Max = int64(viper.GetInt("maxconns"))
	viper.Set("loadfactor", loadFactorPercent())
	if n := viper.GetInt("threads"); n > 0 {
		runtime.GOMAXPROCS(n)
	}
	noEvict := viper.GetBool("noevict")
	switch strings.ToLower(viper.GetString("evict")) {
	case "yes", "true":
	case "no", "false":
		noEvict = true
	default:
		fmt.Fprintln(os.Stderr, "Option --evict is invalid: use yes or no")
		os.Exit(1)
	}
	maxMemory, err := parseMemorySize(viper.GetString("maxmemory"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Option --maxmemory is invalid: %v\n", err)
		os.Exit(1)
	}

	c := cache.New(&cache.Options{
		NumShards:  viper.GetInt("shards"),
		MaxMemory:  maxMemory,
		LoadFactor: float64(viper.GetInt("loadfactor")) / 100,
		NoSixpack:  viper.GetBool("nosixpack"),
		NoEvict:    noEvict,
		UseCAS:     viper.GetBool("cas"),
	})

	// Initialize telemetry
	otlpHeaders, err := telemetry.ParseHeaders(viper.GetString("otlp-headers"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Option --otlp-headers is invalid: %v\n", err)
		os.Exit(1)
	}
	telemetryCfg := &telemetry.Config{
		Enabled:        viper.GetBool("telemetry"),
		ExporterType:   viper.GetString("telemetry-exporter"),
		Protocol:       viper.GetString("otlp-protocol"),
		OTLPEndpoint:   viper.GetString("otlp-endpoint"),
		Headers:        otlpHeaders,
		Insecure:       viper.GetBool("otlp-insecure"),
		ServiceName:    "gopogo",
		ServiceVersion: version,
		Environment:    viper.GetString("telemetry-environment"),
		SampleRatio:    viper.GetFloat64("trace-sample-ratio"),
		Debug:          viper.GetBool("verbose"),
	}
	metrics, err := telemetry.NewMetrics(context.Background(), telemetryCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize telemetry: %v\n", err)
		os.Exit(1)
	}
	tracer, err := telemetry.NewTracer(context.Background(), telemetryCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize tracing: %v\n", err)
		os.Exit(1)
	}
	logger, err := telemetry.NewLogger(context.Background(), telemetryCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize log export: %v\n", err)
		os.Exit(1)
	}
	// Log output goes to stderr and, with telemetry, to OTLP logs.
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(viper.GetString("log-level"))); err != nil {
		fmt.Fprintf(os.Stderr, "Option --log-level is invalid: %v\n", err)
		os.Exit(1)
	}
	logger.Install(level)
	if viper.GetBool("verbose") {
		zap.L().Info(telemetryCfg.Describe())
	}
	protocol.SetDebugLogSample(viper.GetFloat64("debug-log-sample"))
	// Flush metrics, spans and logs on every exit path after this point.
	// Logs go last so the other flushes' errors are exported too.
	shutdownTelemetry := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracer.Shutdown(ctx); err != nil {
			zap.L().Warn("telemetry: flushing traces", zap.Error(err))
		}
		if err := metrics.Shutdown(ctx); err != nil {
			zap.L().Warn("telemetry: flushing metrics", zap.Error(err))
		}
		if err := logger.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "telemetry: flushing logs: %v\n", err)
		}
		zap.L().Sync()
	}
	if telemetryCfg.Enabled {
		protocol.EnableTracing()
	}
	c.SetMetrics(metrics)
	metrics.RegisterGauges(
		func() int64 { return c.MemUsed() },
		func() int64 { return int64(c.NumItems()) },
	)
	if err := metrics.RegisterServerMetrics(c); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize telemetry: %v\n", err)
		os.Exit(1)
	}

	persist := viper.GetString("persist")
	if persist != "" {
		loadPersist(c, persist)
	}

	srv := server.New(&server.Config{
		Host:     viper.GetString("host"),
		Port:     viper.GetInt("port"),
		Socket:   viper.GetString("socket"),
		Auth:     viper.GetString("auth"),
		Persist:  persist,
		TLSPort:  viper.GetInt("tlsport"),
		TLSCert:  viper.GetString("tlscert"),
		TLSKey:   viper.GetString("tlskey"),
		TLSCACert:  viper.GetString("tlscacert"),
		MaxConns:   viper.GetInt("maxconns"),
		Backlog:    viper.GetInt("backlog"),
		ReusePort:  viper.GetBool("reuseport"),
		TCPNoDelay: viper.GetBool("tcpnodelay"),
		QuickAck:   viper.GetBool("quickack"),
		HTTP:     viper.GetBool("http"),
		Memcache: viper.GetBool("memcache"),
		Postgres: viper.GetBool("postgres"),
		Redis:    viper.GetBool("redis"),
		Quiet:    viper.GetBool("quiet"),
		Verbose:  viper.GetBool("verbose"),
		Cache:        c,
		AutoSweep:    viper.GetBool("autosweep"),
		SweepInterval: viper.GetDuration("sweepinterval"),
	})

	if !viper.GetBool("quiet") {
		printStartupBanner(c, maxMemory)
	}

	if level <= zapcore.DebugLevel {
		go func() {
			for range time.Tick(30 * time.Second) {
				protocol.LogStats(c)
			}
		}()
	}
	zap.L().Info("gopogo starting",
		zap.String("version", version), zap.String("commit", commit),
		zap.Int("port", viper.GetInt("port")), zap.Strings("protocols", enabledProtocols()))
	if err := srv.Start(); err != nil {
		zap.L().Error("gopogo failed to start", zap.Error(err))
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		shutdownTelemetry()
		os.Exit(1)
	}
	zap.L().Info("gopogo stopped")

	if persist != "" {
		if !viper.GetBool("quiet") {
			fmt.Printf("Saving data to %s, please wait...\n", persist)
		}
		_, span := otel.Tracer("github.com/grumpylabs/gopogo/cmd").Start(context.Background(), "persist.save",
			trace.WithAttributes(attribute.String("file.path", persist)))
		err := c.Save(persist)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Save failed: %v\n", err)
			shutdownTelemetry()
			os.Exit(1)
		}
	}
	shutdownTelemetry()
}

// validateFlags exits with a message for flag values the server cannot start
// with, matching pogocache's startup checks.
func validateFlags() {
	for _, name := range []string{"port", "tlsport"} {
		if p := viper.GetInt(name); p < 0 || p > 65535 {
			fmt.Fprintf(os.Stderr, "Option --%s is invalid\n", name)
			os.Exit(1)
		}
	}
	if viper.GetInt("port") == 0 && viper.GetInt("tlsport") == 0 && viper.GetString("socket") == "" {
		fmt.Fprintln(os.Stderr, "Need to specify at least one valid port, tlsport or socket option")
		os.Exit(1)
	}
	for _, name := range []string{"maxconns", "backlog"} {
		if viper.GetInt(name) < 1 {
			fmt.Fprintf(os.Stderr, "Option --%s is invalid\n", name)
			os.Exit(1)
		}
	}
	if viper.GetInt("tlsport") > 0 && (viper.GetString("tlscert") == "" || viper.GetString("tlskey") == "") {
		fmt.Fprintln(os.Stderr, "Option --tlsport requires --tlscert and --tlskey")
		os.Exit(1)
	}
}

// loadFactorPercent returns --loadfactor clamped to 55-95, as pogocache does.
func loadFactorPercent() int {
	lf := viper.GetInt("loadfactor")
	switch {
	case lf < 55:
		lf = 55
		fmt.Println("loadfactor minimum set to 55")
	case lf > 95:
		lf = 95
		fmt.Println("loadfactor maximum set to 95")
	}
	return lf
}

// loadPersist removes stale work files and loads path into c if it exists.
// Any failure is fatal so a bad file is never overwritten at shutdown.
func loadPersist(c *cache.Cache, path string) {
	quiet := viper.GetBool("quiet")
	removed, err := cache.CleanWorkFiles(path)
	for _, f := range removed {
		if !quiet {
			fmt.Printf("Deleted work file %s\n", f)
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to clean work files: %v\n", err)
		os.Exit(1)
	}

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return
	}
	if !quiet {
		fmt.Printf("Loading data from %s, please wait...\n", path)
	}
	start := time.Now()
	_, span := otel.Tracer("github.com/grumpylabs/gopogo/cmd").Start(context.Background(), "persist.load",
		trace.WithAttributes(attribute.String("file.path", path)))
	stats, err := c.LoadFromFile(path)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(attribute.Int("gopogo.entries.loaded", stats.Inserted),
			attribute.Int("gopogo.entries.expired", stats.Expired))
	}
	span.End()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Load failed: %v\n", err)
		os.Exit(1)
	}
	if !quiet {
		fmt.Printf("Loaded %d entries (%d expired) (%s in %.3f secs)\n",
			stats.Inserted, stats.Expired, formatBytes(stats.CompressedSize),
			time.Since(start).Seconds())
	}
}

// parseMemorySize parses --maxmemory like pogocache: a number of bytes with an
// optional k, m, g or t suffix (an optional trailing b, any case), a
// percentage of available memory such as "80%", or "unlimited". 0 means
// unlimited. Available memory is the container memory limit when one is set.
func parseMemorySize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" || strings.EqualFold(s, "unlimited") {
		return 0, nil
	}
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil || n <= 0 || math.IsInf(n, 0) {
		return 0, fmt.Errorf("invalid maxmemory %q", s)
	}
	unit := strings.ToLower(strings.TrimSpace(s[i:]))
	if unit == "%" {
		avail := sysmem.Available()
		if avail == 0 {
			// Unknown platform: run unlimited rather than refuse to start.
			fmt.Fprintf(os.Stderr, "maxmemory %s: cannot determine available memory, using unlimited\n", s)
			return 0, nil
		}
		return int64(n / 100 * float64(avail)), nil
	}
	unit = strings.TrimSuffix(unit, "b")
	mult := map[string]float64{"": 1, "k": 1 << 10, "m": 1 << 20, "g": 1 << 30, "t": 1 << 40}[unit]
	if mult == 0 {
		return 0, fmt.Errorf("invalid maxmemory %q", s)
	}
	return int64(n * mult), nil
}

// enabledProtocols lists the protocols turned on by flags.
func enabledProtocols() []string {
	var out []string
	for _, p := range []string{"redis", "http", "memcache", "postgres"} {
		if viper.GetBool(p) {
			out = append(out, p)
		}
	}
	return out
}

func printStartupBanner(c *cache.Cache, maxMemory int64) {
	fmt.Printf("Version: %s (commit: %s)\n", version, commit)
	fmt.Printf("Host: %s:%d\n", viper.GetString("host"), viper.GetInt("port"))
	fmt.Printf("Threads: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("Shards: %d\n", viper.GetInt("shards"))
	fmt.Printf("Load factor: %d%%, CAS: %v\n", viper.GetInt("loadfactor"), viper.GetBool("cas"))
	if p := viper.GetString("persist"); p != "" {
		fmt.Printf("Persist: %s\n", p)
	}

	if maxMemory > 0 {
		fmt.Printf("Max Memory: %s\n", formatBytes(maxMemory))
	} else {
		fmt.Println("Max Memory: unlimited")
	}

	protocols := []string{}
	if viper.GetBool("redis") {
		protocols = append(protocols, "Redis")
	}
	if viper.GetBool("http") {
		protocols = append(protocols, "HTTP")
	}
	if viper.GetBool("memcache") {
		protocols = append(protocols, "Memcache")
	}
	if viper.GetBool("postgres") {
		protocols = append(protocols, "Postgres")
	}

	if len(protocols) > 0 {
		fmt.Printf("Protocols: %v\n", protocols)
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
