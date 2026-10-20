package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/drito256/avry-proxy/internal/balancer"
)

// config holds configuration parameters for the reverse proxy server.
type Config struct {
	Balancer            balancer.Balancer
	UpstreamTimeout     time.Duration
	MaxIdleConnsPerHost int
}

// server is an l7 reverse proxy that routes requests across upstream backends.
type Server struct {
	balancer        balancer.Balancer
	upstreamTimeout time.Duration
	transport       *http.Transport
	proxies         map[string]*httputil.ReverseProxy
}

// newserver constructs a reverse proxy server with a high-concurrency http transport.
func NewServer(cfg Config) *Server {
	if cfg.UpstreamTimeout <= 0 {
		cfg.UpstreamTimeout = 2 * time.Second
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = 256
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	s := &Server{
		balancer:        cfg.Balancer,
		upstreamTimeout: cfg.UpstreamTimeout,
		transport:       transport,
		proxies:         make(map[string]*httputil.ReverseProxy),
	}

	for _, b := range cfg.Balancer.Backends() {
		s.proxies[b.ID] = s.buildReverseProxy(b)
	}

	return s
}

func (s *Server) buildReverseProxy(b *balancer.Backend) *httputil.ReverseProxy {
	target := b.URL
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = target.Host
			pr.SetXForwarded()
		},
		Transport: s.transport,
		ErrorHandler: func(rw http.ResponseWriter, r *http.Request, err error) {
			if rec, ok := rw.(*statusRecorder); ok {
				rec.transportErr = true
			}
			slog.Warn("upstream proxy error",
				"backend_id", b.ID,
				"target", target.String(),
				"path", r.URL.Path,
				"error", err,
			)
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusBadGateway)
			_, _ = fmt.Fprintf(rw, `{"error":"bad gateway","backend_id":%q}`+"\n", b.ID)
		},
	}
	return rp
}

// handler returns an http.handler exposing /healthz and proxying all other routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.Handle("/", s)
	return mux
}

func (s *Server) handleHealthz(rw http.ResponseWriter, _ *http.Request) {
	type nodeStatus struct {
		ID         string `json:"id"`
		URL        string `json:"url"`
		Healthy    bool   `json:"healthy"`
		Inflight   int64  `json:"inflight"`
		EWMAMicros int64  `json:"ewma_micros"`
	}
	backends := s.balancer.Backends()
	nodes := make([]nodeStatus, 0, len(backends))
	for _, b := range backends {
		nodes = append(nodes, nodeStatus{
			ID:         b.ID,
			URL:        b.URL.String(),
			Healthy:    b.IsHealthy(),
			Inflight:   b.Inflight(),
			EWMAMicros: b.EWMAMicros(),
		})
	}
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(map[string]any{
		"status":   "healthy",
		"strategy": s.balancer.Name(),
		"backends": nodes,
	})
}

// servehttp selects a backend via the configured load balancer and forwards the http request.
func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	backend, err := s.balancer.Select()
	if err != nil {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusServiceUnavailable)
		_, _ = rw.Write([]byte(`{"error":"no healthy backends available"}` + "\n"))
		return
	}

	rp := s.proxies[backend.ID]
	if rp == nil {
		rp = s.buildReverseProxy(backend)
	}

	backend.IncInflight()
	defer backend.DecInflight()

	ctx, cancel := context.WithTimeout(r.Context(), s.upstreamTimeout)
	defer cancel()

	rw.Header().Set("X-Avry-Backend", backend.ID)
	rw.Header().Set("X-Avry-Strategy", s.balancer.Name())

	rec := &statusRecorder{
		ResponseWriter: rw,
		statusCode:     http.StatusOK,
	}

	start := time.Now()
	rp.ServeHTTP(rec, r.WithContext(ctx))
	elapsed := time.Since(start)

	failed := rec.transportErr || rec.statusCode >= http.StatusInternalServerError
	s.balancer.RecordOutcome(backend, elapsed, failed)
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode   int
	wroteHeader  bool
	transportErr bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.statusCode = code
		r.wroteHeader = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
