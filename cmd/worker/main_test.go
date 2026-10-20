package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWorkerBaselineAndChaosInjection(t *testing.T) {
	worker := NewWorkerServer("worker-test", 5)
	ts := httptest.NewServer(worker.Handler())
	defer ts.Close()

	// 1. baseline /work request should succeed quickly with x-worker-id header.
	resp, err := http.Get(ts.URL + "/work")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Worker-ID"); got != "worker-test" {
		t.Fatalf("expected X-Worker-ID=worker-test, got %q", got)
	}

	// 2. inject chaos: 40ms added latency and 100% error rate (http 503).
	chaosPayload, _ := json.Marshal(ChaosConfig{
		AddedLatencyMs:  40,
		JitterMs:        5,
		ErrorRate:       1.0,
		ErrorStatusCode: http.StatusServiceUnavailable,
	})
	chaosResp, err := http.Post(ts.URL+"/chaos", "application/json", bytes.NewReader(chaosPayload))
	if err != nil {
		t.Fatalf("failed to post chaos config: %v", err)
	}
	defer chaosResp.Body.Close()
	if chaosResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /chaos, got %d", chaosResp.StatusCode)
	}

	// 3. verify degraded /work behavior reflects injected latency and error code.
	start := time.Now()
	degradedResp, err := http.Get(ts.URL + "/work")
	if err != nil {
		t.Fatalf("unexpected error on degraded request: %v", err)
	}
	defer degradedResp.Body.Close()
	elapsed := time.Since(start)

	if degradedResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", degradedResp.StatusCode)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected latency >= 40ms, got %v", elapsed)
	}
}
