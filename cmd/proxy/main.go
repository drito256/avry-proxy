package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/drito256/avry-proxy/internal/balancer"
	"github.com/drito256/avry-proxy/internal/proxy"
	"github.com/drito256/avry-proxy/internal/resilience"
)

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt64OrDefault(key string, fallback int64) int64 {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envFloat64OrDefault(key string, fallback float64) float64 {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func parseBackends(raw string) ([]*balancer.Backend, error) {
	parts := strings.Split(raw, ",")
	backends := make([]*balancer.Backend, 0, len(parts))
	for i, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}
		id := fmt.Sprintf("worker-%d", i+1)
		targetURL := entry
		if eqIdx := strings.Index(entry, "="); eqIdx > 0 {
			id = strings.TrimSpace(entry[:eqIdx])
			targetURL = strings.TrimSpace(entry[eqIdx+1:])
		}
		b, err := balancer.NewBackend(id, targetURL)
		if err != nil {
			return nil, fmt.Errorf("invalid backend %q: %w", entry, err)
		}
		backends = append(backends, b)
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("at least one backend must be specified")
	}
	return backends, nil
}

func buildBalancer(strategy string, backends []*balancer.Backend) (balancer.Balancer, error) {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "p2c-ewma", "p2c":
		return balancer.NewP2CEWMA(backends), nil
	case "round-robin", "rr":
		return balancer.NewRoundRobin(backends), nil
	default:
		return nil, fmt.Errorf("unknown load balancing strategy %q (supported: p2c-ewma, round-robin)", strategy)
	}
}

func main() {
	defaultBackends := "worker-1=http://localhost:8081,worker-2=http://localhost:8082,worker-3=http://localhost:8083,worker-4=http://localhost:8084"

	listenAddr := flag.String("listen", envOrDefault("PROXY_LISTEN", ":8080"), "HTTP listen address for avry-proxy")
	backendsRaw := flag.String("backends", envOrDefault("PROXY_BACKENDS", defaultBackends), "Comma-separated list of upstream backends (id=url or url)")
	strategy := flag.String("strategy", envOrDefault("PROXY_STRATEGY", "p2c-ewma"), "Load balancing strategy: p2c-ewma or round-robin")
	upstreamTimeout := flag.Duration("upstream-timeout", 2*time.Second, "Per-request upstream timeout")

	enableCircuitBreaker := flag.Bool("circuit-breaker", envBoolOrDefault("PROXY_CIRCUIT_BREAKER", true), "Enable per-node circuit breakers")
	enableShedder := flag.Bool("load-shedding", envBoolOrDefault("PROXY_LOAD_SHEDDING", true), "Enable priority-aware adaptive load shedding")
	maxConcurrency := flag.Int64("max-concurrency", envInt64OrDefault("PROXY_MAX_CONCURRENCY", 256), "Global maximum in-flight requests before shedding")

	enableRateLimit := flag.Bool("rate-limit", envBoolOrDefault("PROXY_RATE_LIMIT", true), "Enable token-bucket rate limiting")
	rateLimitRPS := flag.Float64("rate-limit-rps", envFloat64OrDefault("PROXY_RATE_LIMIT_RPS", 10000), "Per-client token refill rate per second")
	rateLimitBurst := flag.Float64("rate-limit-burst", envFloat64OrDefault("PROXY_RATE_LIMIT_BURST", 2500), "Per-client token bucket burst capacity")
	redisAddr := flag.String("redis-addr", envOrDefault("PROXY_REDIS_ADDR", ""), "Optional Redis address (host:port) for distributed Lua token bucket")

	flag.Parse()

	backends, err := parseBackends(*backendsRaw)
	if err != nil {
		slog.Error("failed to parse backends", "error", err)
		os.Exit(1)
	}

	lb, err := buildBalancer(*strategy, backends)
	if err != nil {
		slog.Error("failed to initialize load balancer", "error", err)
		os.Exit(1)
	}

	var breakers *resilience.BreakerRegistry
	if *enableCircuitBreaker {
		breakers = resilience.NewBreakerRegistry(resilience.DefaultCircuitBreakerConfig())
	}

	var shedder *resilience.Shedder
	if *enableShedder {
		shedCfg := resilience.DefaultShedderConfig()
		shedCfg.MaxConcurrency = *maxConcurrency
		shedder = resilience.NewShedder(shedCfg)
	}

	var rateLimiter *resilience.RateLimiter
	if *enableRateLimit {
		rlCfg := resilience.DefaultRateLimiterConfig()
		rlCfg.Rate = *rateLimitRPS
		rlCfg.Burst = *rateLimitBurst
		rlCfg.RedisAddr = *redisAddr
		rateLimiter = resilience.NewRateLimiter(rlCfg)
	}

	proxyServer := proxy.NewServer(proxy.Config{
		Balancer:        lb,
		UpstreamTimeout: *upstreamTimeout,
		RateLimiter:     rateLimiter,
		Shedder:         shedder,
		Breakers:        breakers,
	})

	httpServer := &http.Server{
		Addr:              *listenAddr,
		Handler:           proxyServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("starting avry-proxy",
			"listen", *listenAddr,
			"strategy", lb.Name(),
			"backends_count", len(backends),
			"circuit_breaker", *enableCircuitBreaker,
			"load_shedding", *enableShedder,
			"rate_limit", *enableRateLimit,
			"upstream_timeout", upstreamTimeout.String(),
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("avry-proxy server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down avry-proxy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}
