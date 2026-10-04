package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
	"github.com/grumpylabs/gopogo/internal/server"
	"github.com/grumpylabs/gopogo/internal/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().String("host", "127.0.0.1", "Listening hostname")
	rootCmd.PersistentFlags().IntP("port", "p", 6379, "Listening port")
	rootCmd.PersistentFlags().StringP("socket", "s", "", "Unix socket path")
	rootCmd.PersistentFlags().String("auth", "", "Authentication password")
	rootCmd.PersistentFlags().String("persist", "", "Persistence file to load at startup and save at shutdown")

	rootCmd.PersistentFlags().Int("threads", runtime.NumCPU(), "Number of threads")
	rootCmd.PersistentFlags().Int("shards", 16, "Number of cache shards")
	rootCmd.PersistentFlags().String("maxmemory", "0", "Maximum memory (e.g., 1GB, 512MB)")
	rootCmd.PersistentFlags().String("evict", "2random", "Eviction policy (noevict, 2random, lru)")
	rootCmd.PersistentFlags().Bool("autosweep", true, "Enable automatic background sweeping of evicted entries")
	rootCmd.PersistentFlags().Duration("sweepinterval", 10*time.Second, "Interval for automatic background sweeping")

	rootCmd.PersistentFlags().Int("tlsport", 0, "TLS listening port")
	rootCmd.PersistentFlags().String("tlscert", "", "TLS certificate file")
	rootCmd.PersistentFlags().String("tlskey", "", "TLS key file")

	rootCmd.PersistentFlags().Bool("http", false, "Enable HTTP protocol")
	rootCmd.PersistentFlags().Bool("memcache", false, "Enable Memcache protocol")
	rootCmd.PersistentFlags().Bool("postgres", false, "Enable Postgres protocol")
	rootCmd.PersistentFlags().Bool("redis", true, "Enable Redis protocol")

	rootCmd.PersistentFlags().String("config", "", "Config file path")
	rootCmd.PersistentFlags().Bool("quiet", false, "Quiet mode")
	rootCmd.PersistentFlags().Bool("verbose", false, "Verbose output")
	rootCmd.PersistentFlags().Bool("version", false, "Show version")

	rootCmd.PersistentFlags().Bool("telemetry", false, "Enable OpenTelemetry metrics")
	rootCmd.PersistentFlags().String("telemetry-exporter", "otlp", "Telemetry exporter (otlp, stdout)")
	rootCmd.PersistentFlags().String("otlp-endpoint", "localhost:4317", "OTLP gRPC endpoint")
	rootCmd.PersistentFlags().Bool("noevict", false, "Disable eviction (reject writes when full)")
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
	validateFlags()
	viper.Set("loadfactor", loadFactorPercent())
	maxMemory := parseMemorySize(viper.GetString("maxmemory"))

	c := cache.New(&cache.Options{
		NumShards:  viper.GetInt("shards"),
		MaxMemory:  maxMemory,
		LoadFactor: float64(viper.GetInt("loadfactor")) / 100,
		NoSixpack:  viper.GetBool("nosixpack"),
		NoEvict:    viper.GetBool("noevict"),
		UseCAS:     viper.GetBool("cas"),
	})

	// Initialize telemetry
	metrics, err := telemetry.NewMetrics(context.Background(), &telemetry.Config{
		Enabled:        viper.GetBool("telemetry"),
		ExporterType:   viper.GetString("telemetry-exporter"),
		OTLPEndpoint:   viper.GetString("otlp-endpoint"),
		ServiceName:    "gopogo",
		ServiceVersion: version,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize telemetry: %v\n", err)
		os.Exit(1)
	}
	c.SetMetrics(metrics)
	metrics.RegisterGauges(
		func() int64 { return c.MemUsed() },
		func() int64 { return int64(c.NumItems()) },
	)

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
		Threads:  viper.GetInt("threads"),
		TLSPort:  viper.GetInt("tlsport"),
		TLSCert:  viper.GetString("tlscert"),
		TLSKey:   viper.GetString("tlskey"),
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

	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}

	if persist != "" {
		if !viper.GetBool("quiet") {
			fmt.Printf("Saving data to %s, please wait...\n", persist)
		}
		if err := c.Save(persist); err != nil {
			fmt.Fprintf(os.Stderr, "Save failed: %v\n", err)
			os.Exit(1)
		}
	}
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
	stats, err := c.LoadFromFile(path)
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

func parseMemorySize(s string) int64 {
	if s == "" || s == "0" {
		return 0
	}

	var size int64
	var unit string

	fmt.Sscanf(s, "%d%s", &size, &unit)

	switch unit {
	case "KB", "kb", "K", "k":
		return size * 1024
	case "MB", "mb", "M", "m":
		return size * 1024 * 1024
	case "GB", "gb", "G", "g":
		return size * 1024 * 1024 * 1024
	case "TB", "tb", "T", "t":
		return size * 1024 * 1024 * 1024 * 1024
	default:
		return size
	}
}

func printStartupBanner(c *cache.Cache, maxMemory int64) {
	fmt.Printf("Version: %s (commit: %s)\n", version, commit)
	fmt.Printf("Host: %s:%d\n", viper.GetString("host"), viper.GetInt("port"))
	fmt.Printf("Threads: %d\n", viper.GetInt("threads"))
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
