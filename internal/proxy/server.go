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
	"github.com/drito256/avry-proxy/internal/resilience"
	"github.com/drito256/avry-proxy/internal/telemetry"
)

// config holds configuration parameters for the reverse proxy server and its resilience pipeline.
type Config struct {
	Balancer            balancer.Balancer
	UpstreamTimeout     time.Duration
	MaxIdleConnsPerHost int
	RateLimiter         *resilience.RateLimiter
	Shedder             *resilience.Shedder
	Breakers            *resilience.BreakerRegistry
	Telemetry           *telemetry.Collector
}

// server is an l7 reverse proxy that routes requests across upstream backends with
// rate limiting, priority load shedding, and per-node circuit breaking.
type Server struct {
	balancer        balancer.Balancer
	upstreamTimeout time.Duration
	transport       *http.Transport
	proxies         map[string]*httputil.ReverseProxy
	rateLimiter     *resilience.RateLimiter
	shedder         *resilience.Shedder
	breakers        *resilience.BreakerRegistry
	telemetry       *telemetry.Collector
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
		rateLimiter:     cfg.RateLimiter,
		shedder:         cfg.Shedder,
		breakers:        cfg.Breakers,
		telemetry:       cfg.Telemetry,
	}

	if s.telemetry == nil {
		s.telemetry = telemetry.NewCollector(cfg.Balancer.Backends, s.breakers, s.shedder)
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

// handler returns an http.handler exposing /healthz, /metrics, and proxying all other routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/metrics", s.telemetry.Handler())
	mux.Handle("/", s)
	return mux
}

func (s *Server) handleHealthz(rw http.ResponseWriter, _ *http.Request) {
	type nodeStatus struct {
		ID           string `json:"id"`
		URL          string `json:"url"`
		Healthy      bool   `json:"healthy"`
		Inflight     int64  `json:"inflight"`
		EWMAMicros   int64  `json:"ewma_micros"`
		BreakerState string `json:"breaker_state,omitempty"`
	}
	backends := s.balancer.Backends()
	nodes := make([]nodeStatus, 0, len(backends))
	for _, b := range backends {
		st := "disabled"
		if s.breakers != nil {
			st = s.breakers.Get(b.ID).State().String()
		}
		nodes = append(nodes, nodeStatus{
			ID:           b.ID,
			URL:          b.URL.String(),
			Healthy:      b.IsHealthy(),
			Inflight:     b.Inflight(),
			EWMAMicros:   b.EWMAMicros(),
			BreakerState: st,
		})
	}
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(map[string]any{
		"status":   "healthy",
		"strategy": s.balancer.Name(),
		"backends": nodes,
	})
}

// selectbackendwithbreaker selects a candidate backend and consults its per-node circuit breaker.
// if a node's breaker is open, it excludes that node and immediately tries fallback candidates.
func (s *Server) selectBackendWithBreaker() (*balancer.Backend, bool, error) {
	if s.breakers == nil {
		b, err := s.balancer.Select()
		return b, false, err
	}

	var excludedBuf [16]*balancer.Backend
	excluded := excludedBuf[:0]
	totalBackends := len(s.balancer.Backends())
	trippedAny := false

	for attempt := 0; attempt < totalBackends; attempt++ {
		b, err := s.balancer.Select(excluded...)
		if err != nil {
			return nil, trippedAny, err
		}
		cb := s.breakers.Get(b.ID)
		if cb.Allow() {
			return b, trippedAny, nil
		}
		trippedAny = true
		excluded = append(excluded, b)
	}

	return nil, trippedAny, balancer.ErrNoHealthyBackends
}

// servehttp executes the full request pipeline:
// rate limiter -> priority load shedder -> balancer + circuit breaker -> upstream worker.
func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	prio := resilience.ExtractPriority(r)

	// 1. token-bucket rate limiter
	if s.rateLimiter != nil {
		clientKey := resilience.ExtractClientKey(r)
		if !s.rateLimiter.Allow(clientKey) {
			s.telemetry.RecordRequest("none", http.StatusTooManyRequests, prio, "ratelimited", 0)
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusTooManyRequests)
			_, _ = rw.Write([]byte(`{"error":"rate limit exceeded"}` + "\n"))
			return
		}
	}

	// 2. priority-aware adaptive load shedder
	if s.shedder != nil {
		if !s.shedder.Allow(prio) {
			s.telemetry.RecordRequest("none", http.StatusServiceUnavailable, prio, "shed", 0)
			rw.Header().Set("Content-Type", "application/json")
			rw.Header().Set("X-Avry-Shed", string(prio))
			rw.WriteHeader(http.StatusServiceUnavailable)
			_, _ = rw.Write([]byte(`{"error":"service overloaded: request shed by priority policy"}` + "\n"))
			return
		}
	}

	start := time.Now()
	if s.shedder != nil {
		defer func() {
			s.shedder.Release(time.Since(start))
		}()
	}

	// 3. load balancer + per-node circuit breaker with fast failover
	backend, trippedAny, err := s.selectBackendWithBreaker()
	if err != nil {
		action := "no_healthy_backend"
		if trippedAny {
			action = "circuit_broken"
		}
		s.telemetry.RecordRequest("none", http.StatusServiceUnavailable, prio, action, 0)
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

	// 4. forward to upstream worker and record telemetry
	reqStart := time.Now()
	rp.ServeHTTP(rec, r.WithContext(ctx))
	elapsed := time.Since(reqStart)

	failed := rec.transportErr || rec.statusCode >= http.StatusInternalServerError
	s.balancer.RecordOutcome(backend, elapsed, failed)
	if s.breakers != nil {
		s.breakers.Get(backend.ID).RecordResult(failed)
	}
	s.telemetry.RecordRequest(backend.ID, rec.statusCode, prio, "forwarded", elapsed)
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
