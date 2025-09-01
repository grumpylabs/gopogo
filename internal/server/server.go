package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"github.com/grumpylabs/gopogo/internal/protocol"
)

type Config struct {
	Host          string
	Port          int
	Socket        string
	Auth          string
	Threads       int
	TLSPort       int
	TLSCert       string
	TLSKey        string
	HTTP          bool
	Memcache      bool
	Postgres      bool
	Redis         bool
	Quiet         bool
	Verbose       bool
	Cache         *cache.Cache
	AutoSweep     bool
	SweepInterval time.Duration
}

type Server struct {
	config    *Config
	cache     *cache.Cache
	listeners []net.Listener
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	
	redisHandler    *protocol.RedisHandler
	httpHandler     *protocol.HTTPHandler
	memcacheHandler *protocol.MemcacheHandler
	postgresHandler *protocol.PostgresHandler
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
		s.redisHandler = protocol.NewRedisHandler(config.Cache, config.Auth)
	}
	if config.HTTP {
		s.httpHandler = protocol.NewHTTPHandler(config.Cache, config.Auth)
	}
	if config.Memcache {
		s.memcacheHandler = protocol.NewMemcacheHandler(config.Cache)
	}
	if config.Postgres {
		s.postgresHandler = protocol.NewPostgresHandler(config.Cache, config.Auth)
	}
	
	return s
}

func (s *Server) Start() error {
	if err := s.setupListeners(); err != nil {
		return err
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
		go s.serve(listener)
	}
	
	s.wg.Wait()
	return nil
}

func (s *Server) Stop() {
	s.cancel()
	
	for _, listener := range s.listeners {
		listener.Close()
	}
	
	s.wg.Wait()
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
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, listener)
		
		if !s.config.Quiet {
			fmt.Printf("Listening on: %s\n", addr)
		}
	}
	
	if s.config.TLSPort > 0 && s.config.TLSCert != "" && s.config.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(s.config.TLSCert, s.config.TLSKey)
		if err != nil {
			return fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		
		addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.TLSPort)
		listener, err := tls.Listen("tcp", addr, tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to listen on TLS %s: %w", addr, err)
		}
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

func (s *Server) serve(listener net.Listener) {
	defer s.wg.Done()
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				if s.config.Verbose {
					log.Printf("Accept error: %v", err)
				}
				continue
			}
		}
		
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	detector := protocol.NewDetector(conn)
	protoType, err := detector.Detect()
	if err != nil {
		if s.config.Verbose {
			log.Printf("Protocol detection error: %v", err)
		}
		return
	}
	
	switch protoType {
	case protocol.TypeRedis:
		if s.redisHandler != nil {
			s.redisHandler.Handle(detector.Conn())
		}
	case protocol.TypeHTTP:
		if s.httpHandler != nil {
			s.httpHandler.Handle(detector.Conn())
		}
	case protocol.TypeMemcache:
		if s.memcacheHandler != nil {
			s.memcacheHandler.Handle(detector.Conn())
		}
	case protocol.TypePostgres:
		if s.postgresHandler != nil {
			s.postgresHandler.Handle(detector.Conn())
		}
	default:
		if s.redisHandler != nil {
			s.redisHandler.Handle(detector.Conn())
		}
	}
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
					log.Printf("Swept %d expired and %d evicted entries", expired, evicted)
				}
			}
		}
	}()
}