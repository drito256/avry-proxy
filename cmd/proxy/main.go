package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/drito256/avry-proxy/internal/balancer"
	"github.com/drito256/avry-proxy/internal/proxy"
)

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
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

	proxyServer := proxy.NewServer(proxy.Config{
		Balancer:        lb,
		UpstreamTimeout: *upstreamTimeout,
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
