package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/drito256/avry-proxy/internal/balancer"
	"github.com/drito256/avry-proxy/internal/resilience"
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

func TestProxyFullResiliencePipelineAndMetrics(t *testing.T) {
	w1 := startMockWorker(t, "worker-1-ok", 1*time.Millisecond, http.StatusOK)
	defer w1.Close()
	w2 := startMockWorker(t, "worker-2-err", 1*time.Millisecond, http.StatusInternalServerError)
	defer w2.Close()

	b1, _ := balancer.NewBackend("worker-1-ok", w1.URL)
	b2, _ := balancer.NewBackend("worker-2-err", w2.URL)

	// use round-robin so every 2nd request hits worker-2-err until its circuit breaker trips.
	lb := balancer.NewRoundRobin([]*balancer.Backend{b1, b2})
	breakers := resilience.NewBreakerRegistry(resilience.CircuitBreakerConfig{
		MinRequests:          4,
		FailureRateThreshold: 0.50,
		ConsecutiveFailures:  3,
		OpenCooldown:         5 * time.Second,
		WindowDuration:       10 * time.Second,
	})

	proxySrv := NewServer(Config{
		Balancer:        lb,
		UpstreamTimeout: 1 * time.Second,
		Breakers:        breakers,
		RateLimiter: resilience.NewRateLimiter(resilience.RateLimiterConfig{
			Rate:  1000,
			Burst: 50,
		}),
		Shedder: resilience.NewShedder(resilience.DefaultShedderConfig()),
	})
	ts := httptest.NewServer(proxySrv.Handler())
	defer ts.Close()

	// send 10 requests -> after 3 errors on worker-2-err, its breaker opens and all subsequent
	// traffic is automatically rerouted to worker-1-ok with status 200.
	errCount := 0
	for i := 0; i < 12; i++ {
		resp, err := http.Get(ts.URL + "/work")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode == http.StatusInternalServerError {
			errCount++
		}
		_ = resp.Body.Close()
	}

	if errCount != 3 {
		t.Fatalf("expected exactly 3 500 responses before circuit breaker tripped, got %d", errCount)
	}

	// verify /metrics endpoint exposes expected prometheus series.
	mResp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("failed to scrape /metrics: %v", err)
	}
	defer mResp.Body.Close()
	bodyBytes, _ := io.ReadAll(mResp.Body)
	metricsText := string(bodyBytes)

	expectedSnippets := []string{
		"avry_requests_total{backend=\"worker-1-ok\",status=\"200\",priority=\"high\",action=\"forwarded\"} 9",
		"avry_requests_total{backend=\"worker-2-err\",status=\"500\",priority=\"high\",action=\"forwarded\"} 3",
		"avry_circuit_breaker_state{backend=\"worker-2-err\"} 2",
		"avry_request_duration_seconds_bucket{backend=\"worker-1-ok\"",
		"avry_backend_ewma_latency_ms{backend=\"worker-1-ok\"}",
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(metricsText, snippet) {
			t.Fatalf("expected /metrics output to contain %q, got:\n%s", snippet, metricsText)
		}
	}
}
