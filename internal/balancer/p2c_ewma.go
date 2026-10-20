package balancer

import (
	"math/rand/v2"
	"time"
)

const (
	// defaultewmaalpha weights recent observations at 30% so latency spikes are detected within a few requests.
	DefaultEWMAAlpha = 0.30
	// defaultfailurepenaltylatency prevents fast http 5xx responses from tricking ewma into favoring a broken node.
	DefaultFailurePenaltyLatency = 500 * time.Millisecond
	// idledecaythreshold is the idle duration after which a degraded node's score slowly decays back toward baseline.
	idleDecayThreshold = 2 * time.Second
)

// p2cewma implements power of two choices (p2c) load balancing weighted by
// lock-free exponentially weighted moving average (ewma) latency and active in-flight concurrency.
type P2CEWMA struct {
	backends       []*Backend
	alpha          float64
	failurePenalty time.Duration
}

// newp2cewma creates a p2cewma load balancer with default smoothing parameters.
func NewP2CEWMA(backends []*Backend) *P2CEWMA {
	return NewP2CEWMAWithConfig(backends, DefaultEWMAAlpha, DefaultFailurePenaltyLatency)
}

// newp2cewmawithconfig creates a p2cewma load balancer with custom ewma alpha and error penalty.
func NewP2CEWMAWithConfig(backends []*Backend, alpha float64, failurePenalty time.Duration) *P2CEWMA {
	if alpha <= 0 || alpha > 1.0 {
		alpha = DefaultEWMAAlpha
	}
	if failurePenalty <= 0 {
		failurePenalty = DefaultFailurePenaltyLatency
	}
	return &P2CEWMA{
		backends:       backends,
		alpha:          alpha,
		failurePenalty: failurePenalty,
	}
}

// name returns the strategy identifier.
func (p *P2CEWMA) Name() string {
	return "p2c-ewma"
}

// select picks two random healthy, non-excluded backends and returns the one with the lower load score:
//
//	score = effective_ewma_micros * (inflight + 1)
func (p *P2CEWMA) Select(exclude ...*Backend) (*Backend, error) {
	var stackBuf [16]*Backend
	candidates := stackBuf[:0]
	for _, b := range p.backends {
		if b.IsHealthy() && !isExcluded(b, exclude) {
			candidates = append(candidates, b)
		}
	}

	count := len(candidates)
	if count == 0 {
		return nil, ErrNoHealthyBackends
	}
	if count == 1 {
		return candidates[0], nil
	}

	// pick two distinct candidates at random (power of two choices).
	i := rand.IntN(count)
	j := (i + 1 + rand.IntN(count-1)) % count

	c1, c2 := candidates[i], candidates[j]
	if p.loadScore(c1) <= p.loadScore(c2) {
		return c1, nil
	}
	return c2, nil
}

// loadscore computes ewma_micros * (inflight + 1), applying gentle recovery decay if a node has been idle.
func (p *P2CEWMA) loadScore(b *Backend) int64 {
	ewma := b.EWMAMicros()
	idleNanos := time.Now().UnixNano() - b.LastUpdatedNano()
	if idleNanos > int64(idleDecayThreshold) && ewma > defaultInitialEWMAMicros {
		// decay elevated ewma toward baseline as idle seconds accumulate so healed nodes can be probed again.
		idlePeriods := idleNanos / int64(idleDecayThreshold)
		if idlePeriods > 10 {
			idlePeriods = 10
		}
		ewma = defaultInitialEWMAMicros + ((ewma - defaultInitialEWMAMicros) >> idlePeriods)
	}
	inflight := b.Inflight()
	if inflight < 0 {
		inflight = 0
	}
	return ewma * (inflight + 1)
}

// recordoutcome updates the backend's ewma latency. if the request failed, a minimum penalty
// latency is applied so fast-failing nodes are deprioritized immediately.
func (p *P2CEWMA) RecordOutcome(b *Backend, latency time.Duration, failed bool) {
	if b == nil {
		return
	}
	sample := latency
	if failed && sample < p.failurePenalty {
		sample = p.failurePenalty
	}
	b.UpdateEWMA(sample, p.alpha)
}

// backends returns the slice of configured backends.
func (p *P2CEWMA) Backends() []*Backend {
	return p.backends
}
