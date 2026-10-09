package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"go.uber.org/zap"
)

// startPprof serves Go's runtime profiles under /debug/pprof/ on addr. The
// profiles describe the server's internals and the heap holds cached
// values, so it refuses an address that is not a loopback one; reach it
// with kubectl port-forward or an SSH tunnel. It returns the address it
// listens on.
func startPprof(addr string) (net.Addr, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("address %q is not a loopback address", addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil {
			zap.L().Warn("pprof server stopped: "+err.Error(), zap.Error(err))
		}
	}()
	zap.L().Info("serving profiles on http://"+ln.Addr().String()+"/debug/pprof/",
		zap.String("pprof.address", ln.Addr().String()))
	return ln.Addr(), nil
}
