package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/drito256/avry-proxy/internal/balancer"
)

func startMockWorker(t *testing.T, id string, delay time.Duration, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		rw.Header().Set("X-Worker-ID", id)
		rw.WriteHeader(statusCode)
		_, _ = fmt.Fprintf(rw, `{"worker_id":%q}`, id)
	}))
}

func TestProxyP2CEWMARoutesAwayFromSlowNode(t *testing.T) {
	w1 := startMockWorker(t, "worker-1", 2*time.Millisecond, http.StatusOK)
	defer w1.Close()
	w2 := startMockWorker(t, "worker-2", 2*time.Millisecond, http.StatusOK)
	defer w2.Close()
	w3 := startMockWorker(t, "worker-3-slow", 80*time.Millisecond, http.StatusOK)
	defer w3.Close()

	b1, _ := balancer.NewBackend("worker-1", w1.URL)
	b2, _ := balancer.NewBackend("worker-2", w2.URL)
	b3, _ := balancer.NewBackend("worker-3-slow", w3.URL)

	lb := balancer.NewP2CEWMA([]*balancer.Backend{b1, b2, b3})
	proxySrv := NewServer(Config{
		Balancer:        lb,
		UpstreamTimeout: 1 * time.Second,
	})
	ts := httptest.NewServer(proxySrv.Handler())
	defer ts.Close()

	counts := make(map[string]int)
	const totalRequests = 60

	for i := 0; i < totalRequests; i++ {
		resp, err := http.Get(ts.URL + "/work")
		if err != nil {
			t.Fatalf("unexpected proxy request error: %v", err)
		}
		backendID := resp.Header.Get("X-Avry-Backend")
		counts[backendID]++
		_ = resp.Body.Close()
	}

	// verify slow worker-3 received only a tiny fraction of traffic compared to healthy workers.
	if counts["worker-3-slow"] > 6 {
		t.Fatalf("expected worker-3-slow to receive <= 6 requests out of %d, got %d (counts=%v)",
			totalRequests, counts["worker-3-slow"], counts)
	}
}
