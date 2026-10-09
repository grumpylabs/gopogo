package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
	"go.uber.org/zap"
)

type Config struct {
	Host          string
	Port          int
	Socket        string
	Auth          string
	Persist       string
	TLSPort       int
	TLSCert       string
	TLSKey        string
	TLSCACert     string // CA bundle for verifying client certificates
	MaxConns      int    // connections beyond this are closed on accept
	Backlog       int    // listen backlog (Linux and macOS)
	ReusePort     bool   // set SO_REUSEPORT (Linux and macOS)
	TCPNoDelay    bool   // disable Nagle's algorithm
	QuickAck      bool   // set TCP_QUICKACK (Linux)
	HTTP          bool
	Memcache      bool
	Postgres      bool
	Redis         bool
	Quiet         bool
	Verbose       bool
	Cache         *cache.Cache
	AutoSweep     bool
	SweepInterval time.Duration
	// EventLoops serves plain connections from per-thread epoll loops on
	// Linux, one per GOMAXPROCS, instead of a goroutine each.
	EventLoops bool
}

type Server struct {
	config    *Config
	cache     *cache.Cache
	listeners []net.Listener
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	handlers  protocol.Handlers

	mu      sync.Mutex  // guards listeners and loops between Start and Stop
	loops   *eventLoops // nil: a goroutine per connection
	stopped bool
}

func New(config *Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	
	s := &Server{
		config: config,
		cache:  config.Cache,
		ctx:    ctx,
		cancel: cancel,
	}
	
	if config.Redis {
		s.handlers.Redis = protocol.NewRedisHandler(config.Cache, config.Auth, config.Persist)
	}
	if config.HTTP {
		s.handlers.HTTP = protocol.NewHTTPHandler(config.Cache, config.Auth)
	}
	if config.Memcache {
		s.handlers.Memcache = protocol.NewMemcacheHandler(config.Cache, config.Auth)
	}
	if config.Postgres {
		s.handlers.Postgres = protocol.NewPostgresHandler(config.Cache, config.Auth, config.Persist)
	}
	
	return s
}

func (s *Server) Start() error {
	s.mu.Lock()
	if err := s.start(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

// start sets up listeners, event loops and the sweeper, and starts
// accepting. s.mu is held.
func (s *Server) start() error {
	if s.stopped {
		return errors.New("server stopped")
	}
	if err := s.setupListeners(); err != nil {
		return err
	}
	if s.config.EventLoops {
		n := runtime.GOMAXPROCS(0)
		loops, err := newEventLoops(n, &s.handlers)
		if err != nil && err != errNoEventLoops {
			return fmt.Errorf("failed to start event loops: %w", err)
		}
		if loops != nil {
			// One loop per thread, and one more thread for everything
			// else. With every thread waiting in a loop, Go would see no
			// idle thread and keep reclaiming them from epoll_wait, waking
			// other threads that find nothing to do.
			runtime.GOMAXPROCS(n + 1)
		}
		s.loops = loops
	}
	
	if s.config.AutoSweep {
		s.startSweeper()
	}
	
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigCh
		if !s.config.Quiet {
			fmt.Println("\nShutting down server...")
		}
		s.Stop()
	}()
	
	for _, listener := range s.listeners {
		s.wg.Add(1)
		go s.serve(listener, s.loops)
	}
	return nil
}

// Stop closes the listeners, waits for accepting to end, and stops the
// event loops, closing their connections.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.cancel()
	for _, listener := range s.listeners {
		listener.Close()
	}
	loops := s.loops
	s.mu.Unlock()

	s.wg.Wait()
	if loops != nil {
		loops.close()
	}
}

func (s *Server) setupListeners() error {
	if s.config.Socket != "" {
		listener, err := net.Listen("unix", s.config.Socket)
		if err != nil {
			return fmt.Errorf("failed to listen on unix socket %s: %w", s.config.Socket, err)
		}
		s.listeners = append(s.listeners, listener)
		
		if !s.config.Quiet {
			fmt.Printf("Listening on unix socket: %s\n", s.config.Socket)
		}
	}
	
	if s.config.Port > 0 {
		addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
		listener, err := listenTCP(s.config.Host, s.config.Port, s.config.Backlog, s.config.ReusePort)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, listener)
		
		if !s.config.Quiet {
			fmt.Printf("Listening on: %s\n", addr)
		}
	}
	
	tlsConfig, err := s.tlsConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil && s.handlers.Postgres != nil {
		// Postgres clients upgrade the plain port to TLS with an SSLRequest.
		s.handlers.Postgres.SetTLSConfig(tlsConfig)
	}

	if s.config.TLSPort > 0 && tlsConfig != nil {
		addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.TLSPort)
		ln, err := listenTCP(s.config.Host, s.config.TLSPort, s.config.Backlog, s.config.ReusePort)
		if err != nil {
			return fmt.Errorf("failed to listen on TLS %s: %w", addr, err)
		}
		listener := tls.NewListener(ln, tlsConfig)
		s.listeners = append(s.listeners, listener)
		
		if !s.config.Quiet {
			fmt.Printf("TLS listening on: %s\n", addr)
		}
	}
	
	if len(s.listeners) == 0 {
		return fmt.Errorf("no listeners configured")
	}
	
	return nil
}

// tlsConfig loads --tlscert/--tlskey (and --tlscacert), or returns nil when no
// certificate is configured.
func (s *Server) tlsConfig() (*tls.Config, error) {
	if s.config.TLSCert == "" || s.config.TLSKey == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(s.config.TLSCert, s.config.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	if s.config.TLSCACert != "" {
		// Like pogocache (SSL_VERIFY_PEER), verify a client certificate
		// when one is presented.
		pem, err := os.ReadFile(s.config.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read TLS CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", s.config.TLSCACert)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

func (s *Server) serve(listener net.Listener, loops *eventLoops) {
	defer s.wg.Done()
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				if s.config.Verbose {
					zap.L().Warn("accept failed: "+err.Error(), zap.Error(err))
				}
				continue
			}
		}
		
		if !s.admit(conn) {
			continue
		}
		if loops != nil && loops.supported(conn) {
			if err := loops.add(conn, release); err != nil {
				zap.L().Warn("event loop refused a connection: "+err.Error(), zap.Error(err))
			}
			continue
		}
		go s.handleConnection(conn)
	}
}

// admit applies the connection limit and TCP options to a new connection.
// It closes the connection and returns false when the limit is reached.
func (s *Server) admit(conn net.Conn) bool {
	stats := &protocol.ConnStats
	if n := stats.Curr.Add(1); s.config.MaxConns > 0 && n > int64(s.config.MaxConns) {
		stats.Curr.Add(-1)
		stats.Rejected.Add(1)
		conn.Close()
		return false
	}
	stats.Total.Add(1)

	tcp, _ := conn.(*net.TCPConn)
	if tlsConn, ok := conn.(*tls.Conn); ok {
		tcp, _ = tlsConn.NetConn().(*net.TCPConn)
	}
	if tcp != nil {
		tcp.SetNoDelay(s.config.TCPNoDelay)
		if s.config.QuickAck {
			setQuickAck(tcp)
		}
	}
	return true
}

func (s *Server) handleConnection(conn net.Conn) {
	defer release()
	defer conn.Close()
	// A bug reached by one client's input closes that connection, not the
	// whole server.
	defer func() {
		if r := recover(); r != nil {
			logPanic(conn.RemoteAddr().String(), r)
		}
	}()
	protocol.ServeConn(conn, &s.handlers)
}

// errNoEventLoops is newEventLoops's error where epoll is unavailable.
var errNoEventLoops = errors.New("event loops need Linux")

// release counts a connection admitted by admit as closed.
func release() { protocol.ConnStats.Curr.Add(-1) }

// logPanic reports a panic that closed a client's connection.
func logPanic(addr string, r any) {
	zap.L().Error(fmt.Sprintf("connection from %s closed after a panic: %v", addr, r),
		zap.String("client.address", addr),
		zap.String("panic", fmt.Sprint(r)), zap.Stack("stack"))
}

func (s *Server) startSweeper() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		
		ticker := time.NewTicker(s.config.SweepInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				expired := s.cache.Sweep()
				evicted := s.cache.SweepEvicted()
				if (expired > 0 || evicted > 0) && s.config.Verbose {
					zap.L().Info(fmt.Sprintf("sweep removed %d expired and %d evicted entries", expired, evicted),
						zap.Int("expired", expired), zap.Int("evicted", evicted))
				}
			}
		}
	}()
}