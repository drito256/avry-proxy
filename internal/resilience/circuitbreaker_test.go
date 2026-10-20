package resilience

import (
	"testing"
	"time"
)

func TestCircuitBreakerStateTransitions(t *testing.T) {
	cfg := CircuitBreakerConfig{
		MinRequests:          10,
		FailureRateThreshold: 0.50,
		ConsecutiveFailures:  100, // disable consecutive shortcut to test rolling ratio
		OpenCooldown:         40 * time.Millisecond,
		WindowDuration:       5 * time.Second,
	}
	cb := NewCircuitBreaker(cfg)

	if cb.State() != StateClosed {
		t.Fatalf("expected initial state closed, got %s", cb.State())
	}

	// record 5 successes and 5 failures (10 requests, 50% error rate) -> should trip open.
	for i := 0; i < 5; i++ {
		if !cb.Allow() {
			t.Fatal("expected closed breaker to allow request")
		}
		cb.RecordResult(false)
	}
	for i := 0; i < 5; i++ {
		if !cb.Allow() {
			t.Fatal("expected closed breaker to allow request before threshold")
		}
		cb.RecordResult(true)
	}

	if cb.State() != StateOpen {
		t.Fatalf("expected breaker to trip open at 50%% failure rate, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected open breaker to reject traffic during cooldown")
	}

	// wait for cooldown to expire; exactly one probe request should be allowed in half-open.
	time.Sleep(50 * time.Millisecond)
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state half-open after cooldown, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected first probe request in half-open to be allowed")
	}
	if cb.Allow() {
		t.Fatal("expected concurrent second request in half-open to be rejected while probe is in flight")
	}

	// failed probe should immediately re-open the breaker.
	cb.RecordResult(true)
	if cb.State() != StateOpen {
		t.Fatalf("expected failed probe to return breaker to open, got %s", cb.State())
	}

	// wait for cooldown again and succeed on the probe -> should close the breaker.
	time.Sleep(50 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected probe request to be allowed after second cooldown")
	}
	cb.RecordResult(false)
	if cb.State() != StateClosed {
		t.Fatalf("expected successful probe to close breaker, got %s", cb.State())
	}
}
