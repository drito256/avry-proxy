package resilience

import (
	"sync"
	"sync/atomic"
	"time"
)

// breakerstate represents the state of a per-node circuit breaker.
type BreakerState int32

const (
	// stateclosed allows normal traffic and monitors error rates.
	StateClosed BreakerState = 0
	// statehalfopen allows a single probe request to test if the node has recovered.
	StateHalfOpen BreakerState = 1
	// stateopen rejects requests immediately until the cooldown period expires.
	StateOpen BreakerState = 2
)

// string returns the lowercase name of the circuit breaker state.
func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// circuitbreakerconfig defines thresholds and timings for a node circuit breaker.
type CircuitBreakerConfig struct {
	MinRequests          int
	FailureRateThreshold float64
	ConsecutiveFailures  int
	OpenCooldown         time.Duration
	WindowDuration       time.Duration
}

// defaultcircuitbreakerconfig returns production-tuned defaults for grey failure isolation.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MinRequests:          20,
		FailureRateThreshold: 0.50,
		ConsecutiveFailures:  8,
		OpenCooldown:         5 * time.Second,
		WindowDuration:       10 * time.Second,
	}
}

// circuitbreaker implements a thread-safe state machine (closed -> open -> half-open).
type CircuitBreaker struct {
	cfg           CircuitBreakerConfig
	state         atomic.Int32
	openedAtNano  atomic.Int64
	probeInFlight atomic.Bool

	mu                  sync.Mutex
	windowStart         time.Time
	requests            int
	failures            int
	consecutiveFailures int
}

// newcircuitbreaker constructs a circuitbreaker with the given configuration.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.MinRequests <= 0 {
		cfg.MinRequests = 20
	}
	if cfg.FailureRateThreshold <= 0 || cfg.FailureRateThreshold > 1.0 {
		cfg.FailureRateThreshold = 0.50
	}
	if cfg.ConsecutiveFailures <= 0 {
		cfg.ConsecutiveFailures = 8
	}
	if cfg.OpenCooldown <= 0 {
		cfg.OpenCooldown = 5 * time.Second
	}
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = 10 * time.Second
	}

	cb := &CircuitBreaker{
		cfg:         cfg,
		windowStart: time.Now(),
	}
	cb.state.Store(int32(StateClosed))
	return cb
}

// state returns the current state of the circuit breaker, automatically evaluating
// whether an open breaker's cooldown has elapsed.
func (cb *CircuitBreaker) State() BreakerState {
	st := BreakerState(cb.state.Load())
	if st == StateOpen {
		openedAt := cb.openedAtNano.Load()
		if time.Now().UnixNano()-openedAt >= int64(cb.cfg.OpenCooldown) {
			return StateHalfOpen
		}
	}
	return st
}

// allow checks whether a request is permitted to proceed to the backend node.
// when transitioning from open to half-open, only a single concurrent probe is allowed.
func (cb *CircuitBreaker) Allow() bool {
	st := BreakerState(cb.state.Load())
	switch st {
	case StateClosed:
		return true
	case StateOpen:
		openedAt := cb.openedAtNano.Load()
		if time.Now().UnixNano()-openedAt < int64(cb.cfg.OpenCooldown) {
			return false
		}
		// cooldown expired: transition to half-open and allow one probe request.
		if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
			cb.probeInFlight.Store(true)
			return true
		}
		return cb.allowHalfOpen()
	case StateHalfOpen:
		return cb.allowHalfOpen()
	default:
		return true
	}
}

func (cb *CircuitBreaker) allowHalfOpen() bool {
	return cb.probeInFlight.CompareAndSwap(false, true)
}

// recordresult updates the circuit breaker counters and state machine after a request finishes.
func (cb *CircuitBreaker) RecordResult(failed bool) {
	st := BreakerState(cb.state.Load())

	if st == StateHalfOpen {
		cb.probeInFlight.Store(false)
		if failed {
			cb.tripOpen()
		} else {
			cb.resetClosed()
		}
		return
	}

	if st == StateOpen {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if now.Sub(cb.windowStart) >= cb.cfg.WindowDuration {
		cb.windowStart = now
		cb.requests = 0
		cb.failures = 0
	}

	cb.requests++
	if failed {
		cb.failures++
		cb.consecutiveFailures++
	} else {
		cb.consecutiveFailures = 0
	}

	shouldTrip := false
	if cb.consecutiveFailures >= cb.cfg.ConsecutiveFailures {
		shouldTrip = true
	} else if cb.requests >= cb.cfg.MinRequests {
		rate := float64(cb.failures) / float64(cb.requests)
		if rate >= cb.cfg.FailureRateThreshold {
			shouldTrip = true
		}
	}

	if shouldTrip {
		cb.openedAtNano.Store(now.UnixNano())
		cb.state.Store(int32(StateOpen))
		cb.probeInFlight.Store(false)
	}
}

func (cb *CircuitBreaker) tripOpen() {
	cb.mu.Lock()
	cb.openedAtNano.Store(time.Now().UnixNano())
	cb.state.Store(int32(StateOpen))
	cb.probeInFlight.Store(false)
	cb.mu.Unlock()
}

func (cb *CircuitBreaker) resetClosed() {
	cb.mu.Lock()
	cb.windowStart = time.Now()
	cb.requests = 0
	cb.failures = 0
	cb.consecutiveFailures = 0
	cb.state.Store(int32(StateClosed))
	cb.probeInFlight.Store(false)
	cb.mu.Unlock()
}

// breakerregistry manages per-node circuit breakers indexed by backend id.
type BreakerRegistry struct {
	cfg      CircuitBreakerConfig
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// newbreakerregistry creates a registry of per-backend circuit breakers.
func NewBreakerRegistry(cfg CircuitBreakerConfig) *BreakerRegistry {
	return &BreakerRegistry{
		cfg:      cfg,
		breakers: make(map[string]*CircuitBreaker),
	}
}

// get returns the circuit breaker for the given backend id, creating one if needed.
func (r *BreakerRegistry) Get(backendID string) *CircuitBreaker {
	r.mu.RLock()
	cb, ok := r.breakers[backendID]
	r.mu.RUnlock()
	if ok {
		return cb
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if cb, ok = r.breakers[backendID]; ok {
		return cb
	}
	cb = NewCircuitBreaker(r.cfg)
	r.breakers[backendID] = cb
	return cb
}
