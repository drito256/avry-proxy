package balancer

import (
	"testing"
	"time"
)

func TestP2CEWMAAvoidsDegradedBackend(t *testing.T) {
	b1, _ := NewBackend("healthy-1", "http://127.0.0.1:8081")
	b2, _ := NewBackend("healthy-2", "http://127.0.0.1:8082")
	b3, _ := NewBackend("healthy-3", "http://127.0.0.1:8083")
	b4, _ := NewBackend("degraded-4", "http://127.0.0.1:8084")

	nodes := []*Backend{b1, b2, b3, b4}
	lb := NewP2CEWMA(nodes)

	// simulate grey failure: healthy nodes respond in 5ms, degraded-4 responds in 450ms.
	counts := make(map[string]int)
	const totalRequests = 1000

	for i := 0; i < totalRequests; i++ {
		selected, err := lb.Select()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[selected.ID]++

		if selected.ID == "degraded-4" {
			lb.RecordOutcome(selected, 450*time.Millisecond, false)
		} else {
			lb.RecordOutcome(selected, 5*time.Millisecond, false)
		}
	}

	degradedShare := float64(counts["degraded-4"]) / float64(totalRequests)
	// under round-robin, degraded-4 would receive 25.0% (250 requests).
	// under p2c+ewma, once degraded-4 is sampled a couple of times, it loses almost every p2c pair comparison.
	if degradedShare > 0.03 {
		t.Fatalf("expected P2C+EWMA to send <3%% of traffic to degraded-4, but sent %.2f%% (%d/%d)",
			degradedShare*100, counts["degraded-4"], totalRequests)
	}
}

func TestP2CEWMAPenalizesFastFailuresAndInflightLoad(t *testing.T) {
	b1, _ := NewBackend("node-1", "http://127.0.0.1:8081")
	b2, _ := NewBackend("node-2", "http://127.0.0.1:8082")
	lb := NewP2CEWMA([]*Backend{b1, b2})

	// suppose node-2 fails fast (1ms) with http 500.
	for i := 0; i < 5; i++ {
		lb.RecordOutcome(b1, 5*time.Millisecond, false)
		lb.RecordOutcome(b2, 1*time.Millisecond, true)
	}

	// because only 2 nodes exist, p2c always compares node-1 and node-2 directly.
	selected, err := lb.Select()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected.ID != "node-1" {
		t.Fatalf("expected healthy node-1 to beat fast-failing node-2, got %s", selected.ID)
	}
}
