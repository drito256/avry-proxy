package resilience

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestExtractPriority(t *testing.T) {
	req := httptest.NewRequest("GET", "/work", nil)
	if got := ExtractPriority(req); got != PriorityHigh {
		t.Fatalf("expected default priority high, got %s", got)
	}

	req.Header.Set("X-Traffic-Priority", "CRITICAL")
	if got := ExtractPriority(req); got != PriorityCritical {
		t.Fatalf("expected priority critical, got %s", got)
	}

	req.Header.Del("X-Traffic-Priority")
	req.Header.Set("X-Priority", "low")
	if got := ExtractPriority(req); got != PriorityLow {
		t.Fatalf("expected priority low from X-Priority fallback, got %s", got)
	}
}

func TestShedderConcurrencyWatermarks(t *testing.T) {
	s := NewShedder(ShedderConfig{
		MaxConcurrency:    10,
		LowPriorityRatio:  0.60, // low watermark = 6
		HighPriorityRatio: 0.80, // high watermark = 8
		LatencyThreshold:  500 * time.Millisecond,
		EWMAAlpha:         0.20,
	})

	// occupy 6 slots -> low, high, and critical should all succeed up to 6.
	for i := 0; i < 6; i++ {
		if !s.Allow(PriorityLow) {
			t.Fatalf("expected low priority to be admitted at slot %d", i+1)
		}
	}

	// slot 7 exceeds low watermark (6): low priority must be shed, high priority must be admitted.
	if s.Allow(PriorityLow) {
		t.Fatal("expected low priority request to be shed above low watermark")
	}
	if !s.Allow(PriorityHigh) {
		t.Fatal("expected high priority request to be admitted at slot 7")
	}
	if !s.Allow(PriorityHigh) {
		t.Fatal("expected high priority request to be admitted at slot 8")
	}

	// slot 9 exceeds high watermark (8): high priority must be shed, critical must be admitted.
	if s.Allow(PriorityHigh) {
		t.Fatal("expected high priority request to be shed above high watermark")
	}
	if !s.Allow(PriorityCritical) {
		t.Fatal("expected critical priority request to be admitted at slot 9")
	}
	if !s.Allow(PriorityCritical) {
		t.Fatal("expected critical priority request to be admitted at slot 10")
	}

	// slot 11 exceeds max concurrency (10): even critical must be shed.
	if s.Allow(PriorityCritical) {
		t.Fatal("expected critical priority request to be shed above max concurrency")
	}

	// release all 10 active requests.
	for i := 0; i < 10; i++ {
		s.Release(5 * time.Millisecond)
	}
	if s.Inflight() != 0 {
		t.Fatalf("expected 0 inflight after releasing all slots, got %d", s.Inflight())
	}
}

func TestShedderLatencySpikeDropsLowPriority(t *testing.T) {
	s := NewShedder(ShedderConfig{
		MaxConcurrency:    100,
		LowPriorityRatio:  0.75,
		HighPriorityRatio: 0.90,
		LatencyThreshold:  100 * time.Millisecond,
		EWMAAlpha:         0.50,
	})

	// simulate latency spike by recording several slow releases (400ms).
	for i := 0; i < 5; i++ {
		if !s.Allow(PriorityCritical) {
			t.Fatal("expected critical traffic to be allowed")
		}
		s.Release(400 * time.Millisecond)
	}

	// global ewma is now well above 100ms threshold: low-priority traffic must be shed even at 0 concurrency,
	// while high and critical traffic continue to be served.
	if s.Allow(PriorityLow) {
		t.Fatal("expected low priority traffic to be shed during global latency spike")
	}
	if !s.Allow(PriorityHigh) {
		t.Fatal("expected high priority traffic to remain admitted when concurrency is low")
	}
	s.Release(5 * time.Millisecond)
}
