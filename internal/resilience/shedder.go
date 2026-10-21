package resilience

import (
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// priority represents the traffic criticality tier extracted from request headers.
type Priority string

const (
	// prioritycritical is reserved for slo-critical requests that must only be dropped at absolute capacity.
	PriorityCritical Priority = "critical"
	// priorityhigh represents standard interactive user traffic.
	PriorityHigh Priority = "high"
	// prioritylow represents background, batch, or best-effort traffic shed first under overload.
	PriorityLow Priority = "low"
)

// extractpriority inspects x-traffic-priority and x-priority headers and returns a normalized priority tier.
// defaults to priorityhigh when unspecified or unrecognized.
func ExtractPriority(r *http.Request) Priority {
	if r == nil {
		return PriorityHigh
	}
	raw := r.Header.Get("X-Traffic-Priority")
	if raw == "" {
		raw = r.Header.Get("X-Priority")
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "critical", "crit", "p0":
		return PriorityCritical
	case "low", "background", "batch", "p2":
		return PriorityLow
	case "high", "normal", "default", "p1":
		return PriorityHigh
	default:
		return PriorityHigh
	}
}

// shedderconfig defines capacity watermarks and latency thresholds for priority load shedding.
type ShedderConfig struct {
	// maxconcurrency is the hard global in-flight request limit across the proxy.
	MaxConcurrency int64
	// lowpriorityratio is the fraction of maxconcurrency above which low-priority traffic is shed (e.g. 0.75).
	LowPriorityRatio float64
	// highpriorityratio is the fraction of maxconcurrency above which high-priority traffic is shed,
	// reserving the remaining capacity exclusively for critical requests (e.g. 0.90).
	HighPriorityRatio float64
	// latencythreshold triggers shedding of low-priority traffic when global ewma latency exceeds this duration.
	LatencyThreshold time.Duration
	// ewmaalpha controls the smoothing factor for global latency tracking.
	EWMAAlpha float64
}

// defaultshedderconfig returns production-tuned defaults for adaptive priority load shedding.
func DefaultShedderConfig() ShedderConfig {
	return ShedderConfig{
		MaxConcurrency:    256,
		LowPriorityRatio:  0.75,
		HighPriorityRatio: 0.90,
		LatencyThreshold:  150 * time.Millisecond,
		EWMAAlpha:         0.20,
	}
}

// shedder implements lock-free adaptive load shedding based on traffic priority,
// active global concurrency, and rolling ewma latency.
type Shedder struct {
	maxConcurrency     int64
	lowWatermark       int64
	highWatermark      int64
	latencyThresholdUs int64
	alpha              float64

	inflight   atomic.Int64
	ewmaMicros atomic.Int64
	shedTotal  atomic.Int64
}

// newshedder constructs a priority-aware load shedder from the provided configuration.
func NewShedder(cfg ShedderConfig) *Shedder {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 256
	}
	if cfg.LowPriorityRatio <= 0 || cfg.LowPriorityRatio >= 1.0 {
		cfg.LowPriorityRatio = 0.75
	}
	if cfg.HighPriorityRatio <= cfg.LowPriorityRatio || cfg.HighPriorityRatio >= 1.0 {
		cfg.HighPriorityRatio = 0.90
	}
	if cfg.LatencyThreshold <= 0 {
		cfg.LatencyThreshold = 150 * time.Millisecond
	}
	if cfg.EWMAAlpha <= 0 || cfg.EWMAAlpha > 1.0 {
		cfg.EWMAAlpha = 0.20
	}

	lowMark := int64(float64(cfg.MaxConcurrency) * cfg.LowPriorityRatio)
	if lowMark < 1 {
		lowMark = 1
	}
	highMark := int64(float64(cfg.MaxConcurrency) * cfg.HighPriorityRatio)
	if highMark <= lowMark {
		highMark = lowMark + 1
	}

	s := &Shedder{
		maxConcurrency:     cfg.MaxConcurrency,
		lowWatermark:       lowMark,
		highWatermark:      highMark,
		latencyThresholdUs: cfg.LatencyThreshold.Microseconds(),
		alpha:              cfg.EWMAAlpha,
	}
	s.ewmaMicros.Store(5000) // initial 5ms baseline
	return s
}

// allow evaluates whether a request of the given priority should be admitted.
// if admitted, it increments the active in-flight counter and returns true; the caller
// must invoke release when the request finishes.
func (s *Shedder) Allow(prio Priority) bool {
	current := s.inflight.Add(1)

	// hard cap: shed all traffic if global concurrency exceeds maxconcurrency.
	if current > s.maxConcurrency {
		s.inflight.Add(-1)
		s.shedTotal.Add(1)
		return false
	}

	switch prio {
	case PriorityLow:
		// shed low-priority traffic if concurrency exceeds low watermark or global latency spikes.
		if current > s.lowWatermark || s.ewmaMicros.Load() > s.latencyThresholdUs {
			s.inflight.Add(-1)
			s.shedTotal.Add(1)
			return false
		}
	case PriorityHigh:
		// shed high-priority traffic above the high watermark to protect critical slo capacity.
		if current > s.highWatermark {
			s.inflight.Add(-1)
			s.shedTotal.Add(1)
			return false
		}
	case PriorityCritical:
		// critical traffic is admitted up to 100% of maxconcurrency.
	default:
		if current > s.highWatermark {
			s.inflight.Add(-1)
			s.shedTotal.Add(1)
			return false
		}
	}

	return true
}

// release decrements the active in-flight counter and updates the rolling global ewma latency.
func (s *Shedder) Release(latency time.Duration) {
	s.inflight.Add(-1)
	sampleMicros := latency.Microseconds()
	if sampleMicros <= 0 {
		sampleMicros = 1
	}
	for {
		oldVal := s.ewmaMicros.Load()
		newVal := int64(s.alpha*float64(sampleMicros) + (1.0-s.alpha)*float64(oldVal))
		if newVal <= 0 {
			newVal = 1
		}
		if s.ewmaMicros.CompareAndSwap(oldVal, newVal) {
			return
		}
	}
}

// inflight returns the current global in-flight concurrency tracked by the shedder.
func (s *Shedder) Inflight() int64 {
	return s.inflight.Load()
}

// ewmamicros returns the rolling global ewma latency in microseconds.
func (s *Shedder) EWMAMicros() int64 {
	return s.ewmaMicros.Load()
}

// shedtotal returns the cumulative count of requests shed since startup.
func (s *Shedder) ShedTotal() int64 {
	return s.shedTotal.Load()
}
