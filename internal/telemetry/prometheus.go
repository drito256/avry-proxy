package telemetry

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drito256/avry-proxy/internal/balancer"
	"github.com/drito256/avry-proxy/internal/resilience"
)

// defaultbuckets defines the latency histogram bucket upper bounds in seconds:
// 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s.
var defaultBuckets = []float64{0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000}

type counterKey struct {
	backend  string
	status   string
	priority string
	action   string
}

type histogramSeries struct {
	buckets [8]atomic.Uint64
	count   atomic.Uint64
	sumNs   atomic.Uint64
}

// collector aggregates lock-free prometheus metrics for the avry-proxy pipeline.
type Collector struct {
	mu         sync.RWMutex
	counters   map[counterKey]*atomic.Uint64
	histograms map[string]*histogramSeries

	backends func() []*balancer.Backend
	breakers *resilience.BreakerRegistry
	shedder  *resilience.Shedder
}

// newcollector constructs a prometheus telemetry collector wired to the proxy's live state.
func NewCollector(backendsFn func() []*balancer.Backend, breakers *resilience.BreakerRegistry, shedder *resilience.Shedder) *Collector {
	return &Collector{
		counters:   make(map[counterKey]*atomic.Uint64),
		histograms: make(map[string]*histogramSeries),
		backends:   backendsFn,
		breakers:   breakers,
		shedder:    shedder,
	}
}

// recordrequest updates request counters and latency histograms for a completed request.
func (c *Collector) RecordRequest(backend string, statusCode int, priority resilience.Priority, action string, duration time.Duration) {
	if backend == "" {
		backend = "none"
	}
	if priority == "" {
		priority = resilience.PriorityHigh
	}
	if action == "" {
		action = "forwarded"
	}

	key := counterKey{
		backend:  backend,
		status:   strconv.Itoa(statusCode),
		priority: string(priority),
		action:   action,
	}

	c.getCounter(key).Add(1)

	if action == "forwarded" {
		h := c.getHistogram(backend)
		sec := duration.Seconds()
		for i, bound := range defaultBuckets {
			if sec <= bound {
				h.buckets[i].Add(1)
			}
		}
		h.count.Add(1)
		if ns := duration.Nanoseconds(); ns > 0 {
			h.sumNs.Add(uint64(ns))
		}
	}
}

func (c *Collector) getCounter(key counterKey) *atomic.Uint64 {
	c.mu.RLock()
	ctr, ok := c.counters[key]
	c.mu.RUnlock()
	if ok {
		return ctr
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if ctr, ok = c.counters[key]; ok {
		return ctr
	}
	ctr = &atomic.Uint64{}
	c.counters[key] = ctr
	return ctr
}

func (c *Collector) getHistogram(backend string) *histogramSeries {
	c.mu.RLock()
	h, ok := c.histograms[backend]
	c.mu.RUnlock()
	if ok {
		return h
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok = c.histograms[backend]; ok {
		return h
	}
	h = &histogramSeries{}
	c.histograms[backend] = h
	return h
}

// handler returns an http.handlerFunc that renders metrics in prometheus text exposition format.
func (c *Collector) Handler() http.HandlerFunc {
	return func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var sb strings.Builder

		// 1. avry_requests_total
		sb.WriteString("# HELP avry_requests_total Total number of HTTP requests processed by avry-proxy.\n")
		sb.WriteString("# TYPE avry_requests_total counter\n")

		c.mu.RLock()
		keys := make([]counterKey, 0, len(c.counters))
		for k := range c.counters {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].backend != keys[j].backend {
				return keys[i].backend < keys[j].backend
			}
			if keys[i].action != keys[j].action {
				return keys[i].action < keys[j].action
			}
			if keys[i].priority != keys[j].priority {
				return keys[i].priority < keys[j].priority
			}
			return keys[i].status < keys[j].status
		})
		for _, k := range keys {
			val := c.counters[k].Load()
			fmt.Fprintf(&sb, "avry_requests_total{backend=%q,status=%q,priority=%q,action=%q} %d\n",
				k.backend, k.status, k.priority, k.action, val)
		}

		// 2. avry_request_duration_seconds
		sb.WriteString("# HELP avry_request_duration_seconds Histogram of upstream HTTP request latencies in seconds.\n")
		sb.WriteString("# TYPE avry_request_duration_seconds histogram\n")

		histBackends := make([]string, 0, len(c.histograms))
		for b := range c.histograms {
			histBackends = append(histBackends, b)
		}
		sort.Strings(histBackends)
		for _, b := range histBackends {
			h := c.histograms[b]
			for i, bound := range defaultBuckets {
				fmt.Fprintf(&sb, "avry_request_duration_seconds_bucket{backend=%q,le=\"%g\"} %d\n",
					b, bound, h.buckets[i].Load())
			}
			count := h.count.Load()
			sumSec := float64(h.sumNs.Load()) / 1e9
			fmt.Fprintf(&sb, "avry_request_duration_seconds_bucket{backend=%q,le=\"+Inf\"} %d\n", b, count)
			fmt.Fprintf(&sb, "avry_request_duration_seconds_sum{backend=%q} %.6f\n", b, sumSec)
			fmt.Fprintf(&sb, "avry_request_duration_seconds_count{backend=%q} %d\n", b, count)
		}
		c.mu.RUnlock()

		// 3. per-backend gauges
		if c.backends != nil {
			nodes := c.backends()
			sb.WriteString("# HELP avry_backend_ewma_latency_ms Current EWMA latency per backend node in milliseconds.\n")
			sb.WriteString("# TYPE avry_backend_ewma_latency_ms gauge\n")
			for _, b := range nodes {
				ewmaMs := float64(b.EWMAMicros()) / 1000.0
				fmt.Fprintf(&sb, "avry_backend_ewma_latency_ms{backend=%q} %.3f\n", b.ID, ewmaMs)
			}

			sb.WriteString("# HELP avry_backend_inflight_requests Current number of active in-flight requests per backend node.\n")
			sb.WriteString("# TYPE avry_backend_inflight_requests gauge\n")
			for _, b := range nodes {
				fmt.Fprintf(&sb, "avry_backend_inflight_requests{backend=%q} %d\n", b.ID, b.Inflight())
			}

			if c.breakers != nil {
				sb.WriteString("# HELP avry_circuit_breaker_state Current circuit breaker state per backend (0=closed, 1=half-open, 2=open).\n")
				sb.WriteString("# TYPE avry_circuit_breaker_state gauge\n")
				for _, b := range nodes {
					st := int(c.breakers.Get(b.ID).State())
					fmt.Fprintf(&sb, "avry_circuit_breaker_state{backend=%q} %d\n", b.ID, st)
				}
			}
		}

		// 4. global shedder gauges
		if c.shedder != nil {
			sb.WriteString("# HELP avry_shedder_inflight_requests Global active in-flight concurrency tracked by the load shedder.\n")
			sb.WriteString("# TYPE avry_shedder_inflight_requests gauge\n")
			fmt.Fprintf(&sb, "avry_shedder_inflight_requests %d\n", c.shedder.Inflight())
		}

		_, _ = rw.Write([]byte(sb.String()))
	}
}
