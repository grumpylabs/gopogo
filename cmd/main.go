package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/server"
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

	rootCmd.PersistentFlags().Int("threads", runtime.NumCPU(), "Number of threads")
	rootCmd.PersistentFlags().Int("shards", 16, "Number of cache shards")
	rootCmd.PersistentFlags().String("maxmemory", "0", "Maximum memory (e.g., 1GB, 512MB)")
	rootCmd.PersistentFlags().String("evict", "2random", "Eviction policy (noevict, 2random, lru)")

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

	maxMemory := parseMemorySize(viper.GetString("maxmemory"))

	c := cache.New(
		viper.GetInt("shards"),
		maxMemory,
	)

	srv := server.New(&server.Config{
		Host:     viper.GetString("host"),
		Port:     viper.GetInt("port"),
		Socket:   viper.GetString("socket"),
		Auth:     viper.GetString("auth"),
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
		Cache:    c,
	})

	if !viper.GetBool("quiet") {
		printStartupBanner(c, maxMemory)
	}

	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
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
