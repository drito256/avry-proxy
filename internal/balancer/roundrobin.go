package balancer

import (
	"sync/atomic"
	"time"
)

const defaultEWMAAlpha = 0.2

// roundrobin implements a lock-free atomic round-robin load balancer.
type RoundRobin struct {
	backends []*Backend
	counter  atomic.Uint64
}

// newroundrobin creates a roundrobin balancer across the provided backends.
func NewRoundRobin(backends []*Backend) *RoundRobin {
	return &RoundRobin{
		backends: backends,
	}
}

// name returns the strategy name.
func (rr *RoundRobin) Name() string {
	return "round-robin"
}

// select picks the next healthy, non-excluded backend in sequential round-robin order.
func (rr *RoundRobin) Select(exclude ...*Backend) (*Backend, error) {
	n := uint64(len(rr.backends))
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}

	start := rr.counter.Add(1) - 1
	for i := uint64(0); i < n; i++ {
		idx := (start + i) % n
		candidate := rr.backends[idx]
		if candidate.IsHealthy() && !isExcluded(candidate, exclude) {
			return candidate, nil
		}
	}

	return nil, ErrNoHealthyBackends
}

// recordoutcome updates the backend's ewma latency for observability even though roundrobin
// does not use latency for routing decisions.
func (rr *RoundRobin) RecordOutcome(b *Backend, latency time.Duration, _ bool) {
	if b != nil {
		b.UpdateEWMA(latency, defaultEWMAAlpha)
	}
}

// backends returns all backends managed by this balancer.
func (rr *RoundRobin) Backends() []*Backend {
	return rr.backends
}
