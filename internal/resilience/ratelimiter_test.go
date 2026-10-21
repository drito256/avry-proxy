package resilience

import (
	"bufio"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLocalRateLimiterBurstAndRefill(t *testing.T) {
	rl := NewRateLimiter(RateLimiterConfig{
		Rate:  100, // 100 tokens/sec -> 1 token every 10ms
		Burst: 3,
	})

	// burst of 3 should be allowed immediately for client-a.
	for i := 0; i < 3; i++ {
		if !rl.Allow("client-a") {
			t.Fatalf("expected request %d within burst to be allowed", i+1)
		}
	}

	// 4th immediate request for client-a must be rejected.
	if rl.Allow("client-a") {
		t.Fatal("expected request exceeding burst to be rate limited")
	}

	// different client key should have an independent bucket.
	if !rl.Allow("client-b") {
		t.Fatal("expected independent client key to be allowed")
	}

	// wait 15ms for at least 1 token to refill on client-a.
	time.Sleep(15 * time.Millisecond)
	if !rl.Allow("client-a") {
		t.Fatal("expected request to be allowed after token refill")
	}
}

func TestRedisRateLimiterAndFallback(t *testing.T) {
	// start a lightweight mock resp server simulating redis eval responses.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		callCount := 0
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "*") {
				callCount++
				// first 2 calls succeed (:1), 3rd call rate limits (:0).
				if callCount <= 2 {
					_, _ = io.WriteString(conn, ":1\r\n")
				} else {
					_, _ = io.WriteString(conn, ":0\r\n")
				}
			}
		}
	}()

	rl := NewRateLimiter(RateLimiterConfig{
		Rate:         50,
		Burst:        2,
		RedisAddr:    ln.Addr().String(),
		RedisTimeout: 50 * time.Millisecond,
	})

	if !rl.Allow("client-redis") {
		t.Fatal("expected 1st redis request to be allowed")
	}
	if !rl.Allow("client-redis") {
		t.Fatal("expected 2nd redis request to be allowed")
	}
	if rl.Allow("client-redis") {
		t.Fatal("expected 3rd redis request to be rejected by redis script")
	}

	// now test automatic fallback when redis is unreachable.
	rlFallback := NewRateLimiter(RateLimiterConfig{
		Rate:         10,
		Burst:        2,
		RedisAddr:    "127.0.0.1:1", // unreachable port
		RedisTimeout: 10 * time.Millisecond,
	})

	if !rlFallback.Allow("client-fallback") {
		t.Fatal("expected limiter to fail open to local bucket when redis is unreachable")
	}
	if rlFallback.RedisFallbacks() == 0 {
		t.Fatal("expected redis fallback counter to be incremented")
	}
}

func TestExtractClientKey(t *testing.T) {
	req := httptest.NewRequest("GET", "/work", nil)
	req.RemoteAddr = "10.0.0.42:54321"
	if got := ExtractClientKey(req); got != "ip:10.0.0.42" {
		t.Fatalf("expected ip:10.0.0.42, got %s", got)
	}

	req.Header.Set("X-Forwarded-For", "203.0.113.5, 10.0.0.42")
	if got := ExtractClientKey(req); got != "ip:203.0.113.5" {
		t.Fatalf("expected ip:203.0.113.5, got %s", got)
	}

	req.Header.Set("X-API-Key", "prod-token-99")
	if got := ExtractClientKey(req); got != "key:prod-token-99" {
		t.Fatalf("expected key:prod-token-99, got %s", got)
	}
}
