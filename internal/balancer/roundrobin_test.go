package balancer

import (
	"sync"
	"testing"
)

func mustBackends(t *testing.T) []*Backend {
	t.Helper()
	b1, err := NewBackend("node-1", "http://127.0.0.1:8081")
	if err != nil {
		t.Fatal(err)
	}
	b2, err := NewBackend("node-2", "http://127.0.0.1:8082")
	if err != nil {
		t.Fatal(err)
	}
	b3, err := NewBackend("node-3", "http://127.0.0.1:8083")
	if err != nil {
		t.Fatal(err)
	}
	return []*Backend{b1, b2, b3}
}

func TestRoundRobinSequentialAndExclusions(t *testing.T) {
	nodes := mustBackends(t)
	rr := NewRoundRobin(nodes)

	counts := make(map[string]int)
	for i := 0; i < 90; i++ {
		selected, err := rr.Select()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[selected.ID]++
	}

	for _, n := range nodes {
		if counts[n.ID] != 30 {
			t.Fatalf("expected 30 selections for %s, got %d", n.ID, counts[n.ID])
		}
	}

	// mark node-2 unhealthy and exclude node-1; only node-3 should be selected.
	nodes[1].SetHealthy(false)
	for i := 0; i < 10; i++ {
		selected, err := rr.Select(nodes[0])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selected.ID != "node-3" {
			t.Fatalf("expected node-3, got %s", selected.ID)
		}
	}
}

func TestRoundRobinConcurrentSafety(t *testing.T) {
	nodes := mustBackends(t)
	rr := NewRoundRobin(nodes)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				b, err := rr.Select()
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				b.IncInflight()
				b.DecInflight()
			}
		}()
	}
	wg.Wait()
}
