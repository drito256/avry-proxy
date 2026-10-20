package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

// chaosconfig represents the json payload for viewing or updating chaos injection parameters.
type ChaosConfig struct {
	AddedLatencyMs  int64   `json:"added_latency_ms"`
	JitterMs        int64   `json:"jitter_ms"`
	ErrorRate       float64 `json:"error_rate"`
	ErrorStatusCode int     `json:"error_status_code,omitempty"`
}

// workerserver simulates an upstream service with lock-free live chaos injection.
type WorkerServer struct {
	id                   string
	baseLatencyMs        int64
	addedLatencyMs       atomic.Int64
	jitterMs             atomic.Int64
	errorRateBasisPoints atomic.Int64 // 0..10000 (10000 == 100.0%)
	errorStatusCode      atomic.Int64
	inflight             atomic.Int64
	totalRequests        atomic.Int64
}

// newworkerserver initializes a workerserver with baseline latency.
func NewWorkerServer(id string, baseLatencyMs int64) *WorkerServer {
	w := &WorkerServer{
		id:            id,
		baseLatencyMs: baseLatencyMs,
	}
	w.errorStatusCode.Store(http.StatusInternalServerError)
	return w
}

// handler returns the http mux for the worker service.
func (w *WorkerServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/chaos", w.handleChaos)
	mux.HandleFunc("/healthz", w.handleHealthz)
	mux.HandleFunc("/work", w.handleWork)
	mux.HandleFunc("/", w.handleWork)
	return mux
}

func (w *WorkerServer) handleChaos(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.writeChaosState(rw)
	case http.MethodPost, http.MethodPut:
		var cfg ChaosConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(rw, `{"error":"invalid JSON payload"}`, http.StatusBadRequest)
			return
		}
		if cfg.AddedLatencyMs < 0 || cfg.JitterMs < 0 || cfg.ErrorRate < 0 || cfg.ErrorRate > 1.0 {
			http.Error(rw, `{"error":"parameters out of valid range"}`, http.StatusBadRequest)
			return
		}
		statusCode := cfg.ErrorStatusCode
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		if statusCode < 400 || statusCode > 599 {
			http.Error(rw, `{"error":"error_status_code must be between 400 and 599"}`, http.StatusBadRequest)
			return
		}

		w.addedLatencyMs.Store(cfg.AddedLatencyMs)
		w.jitterMs.Store(cfg.JitterMs)
		w.errorRateBasisPoints.Store(int64(cfg.ErrorRate * 10000))
		w.errorStatusCode.Store(int64(statusCode))

		slog.Info("updated chaos configuration",
			"worker_id", w.id,
			"added_latency_ms", cfg.AddedLatencyMs,
			"jitter_ms", cfg.JitterMs,
			"error_rate", cfg.ErrorRate,
			"error_status_code", statusCode,
		)
		w.writeChaosState(rw)
	default:
		http.Error(rw, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (w *WorkerServer) writeChaosState(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", "application/json")
	cfg := ChaosConfig{
		AddedLatencyMs:  w.addedLatencyMs.Load(),
		JitterMs:        w.jitterMs.Load(),
		ErrorRate:       float64(w.errorRateBasisPoints.Load()) / 10000.0,
		ErrorStatusCode: int(w.errorStatusCode.Load()),
	}
	_ = json.NewEncoder(rw).Encode(cfg)
}

func (w *WorkerServer) handleHealthz(rw http.ResponseWriter, _ *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(map[string]any{
		"worker_id":      w.id,
		"status":         "healthy",
		"inflight":       w.inflight.Load(),
		"total_requests": w.totalRequests.Load(),
	})
}

func (w *WorkerServer) handleWork(rw http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.inflight.Add(1)
	defer w.inflight.Add(-1)
	w.totalRequests.Add(1)

	delayMs := w.baseLatencyMs + w.addedLatencyMs.Load()
	if jitter := w.jitterMs.Load(); jitter > 0 {
		delayMs += rand.Int64N(jitter + 1)
	}

	if delayMs > 0 {
		timer := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}

	rw.Header().Set("X-Worker-ID", w.id)
	rw.Header().Set("Content-Type", "application/json")

	errBp := w.errorRateBasisPoints.Load()
	if errBp > 0 && rand.Int64N(10000) < errBp {
		code := int(w.errorStatusCode.Load())
		rw.WriteHeader(code)
		_ = json.NewEncoder(rw).Encode(map[string]any{
			"worker_id":   w.id,
			"status":      "error",
			"injected":    true,
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return
	}

	rw.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(rw).Encode(map[string]any{
		"worker_id":   w.id,
		"status":      "ok",
		"duration_ms": time.Since(start).Milliseconds(),
	})
}

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
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

func main() {
	listenAddr := flag.String("listen", envOrDefault("WORKER_LISTEN", ":8081"), "HTTP listen address")
	workerID := flag.String("id", envOrDefault("WORKER_ID", "worker-1"), "Unique worker identifier")
	baseLatency := flag.Int64("base-latency-ms", envInt64OrDefault("WORKER_BASE_LATENCY_MS", 5), "Baseline processing latency in milliseconds")
	flag.Parse()

	worker := NewWorkerServer(*workerID, *baseLatency)
	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           worker.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("starting backend worker", "worker_id", *workerID, "listen", *listenAddr, "base_latency_ms", *baseLatency)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("worker server failed", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down backend worker", "worker_id", *workerID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
