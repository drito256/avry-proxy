package balancer

import (
	"errors"
	"net/url"
	"sync/atomic"
	"time"
)

// errnohealthybackends is returned when all candidate upstream nodes are unavailable or excluded.
var ErrNoHealthyBackends = errors.New("balancer: no healthy backends available")

const defaultInitialEWMAMicros = 5000 // 5ms initial estimate

// backend represents a single upstream worker node and its real-time load/latency state.
type Backend struct {
	ID              string
	URL             *url.URL
	inflight        atomic.Int64
	ewmaMicros      atomic.Int64
	lastUpdatedNano atomic.Int64
	healthy         atomic.Bool
}

// newbackend creates a backend from a target url string.
func NewBackend(id, rawURL string) (*Backend, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	b := &Backend{
		ID:  id,
		URL: parsed,
	}
	b.ewmaMicros.Store(defaultInitialEWMAMicros)
	b.lastUpdatedNano.Store(time.Now().UnixNano())
	b.healthy.Store(true)
	return b, nil
}

// incinflight atomically increments and returns the number of active requests on this node.
func (b *Backend) IncInflight() int64 {
	return b.inflight.Add(1)
}

// decinflight atomically decrements and returns the number of active requests on this node.
func (b *Backend) DecInflight() int64 {
	return b.inflight.Add(-1)
}

// inflight returns the current number of active in-flight requests.
func (b *Backend) Inflight() int64 {
	return b.inflight.Load()
}

// ewmamicros returns the exponentially weighted moving average latency in microseconds.
func (b *Backend) EWMAMicros() int64 {
	return b.ewmaMicros.Load()
}

// ewmaduration returns the current ewma latency as a time.duration.
func (b *Backend) EWMADuration() time.Duration {
	return time.Duration(b.ewmaMicros.Load()) * time.Microsecond
}

// updateewma performs a lock-free compare-and-swap update of the node's ewma latency.
func (b *Backend) UpdateEWMA(sample time.Duration, alpha float64) {
	sampleMicros := sample.Microseconds()
	if sampleMicros <= 0 {
		sampleMicros = 1
	}
	b.lastUpdatedNano.Store(time.Now().UnixNano())
	for {
		oldVal := b.ewmaMicros.Load()
		newVal := int64(alpha*float64(sampleMicros) + (1.0-alpha)*float64(oldVal))
		if newVal <= 0 {
			newVal = 1
		}
		if b.ewmaMicros.CompareAndSwap(oldVal, newVal) {
			return
		}
	}
}

// lastupdatednano returns the unix timestamp in nanoseconds of the most recent ewma update.
func (b *Backend) LastUpdatedNano() int64 {
	return b.lastUpdatedNano.Load()
}

// ishealthy reports whether the backend is currently considered routable.
func (b *Backend) IsHealthy() bool {
	return b.healthy.Load()
}

// sethealthy updates the backend's health flag.
func (b *Backend) SetHealthy(healthy bool) {
	b.healthy.Store(healthy)
}

// balancer defines the strategy interface for selecting upstream backends and recording telemetry.
type Balancer interface {
	// name returns the identifier of the load balancing algorithm (e.g. "round-robin", "p2c-ewma").
	Name() string
	// select chooses a healthy backend, optionally excluding already-attempted or circuit-broken nodes.
	Select(exclude ...*Backend) (*Backend, error)
	// recordoutcome updates backend metrics after a proxied request completes.
	RecordOutcome(b *Backend, latency time.Duration, failed bool)
	// backends returns the slice of configured backends.
	Backends() []*Backend
}

func isExcluded(candidate *Backend, exclude []*Backend) bool {
	for _, ex := range exclude {
		if ex == candidate {
			return true
		}
	}
	return false
}
