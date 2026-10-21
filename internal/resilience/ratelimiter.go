package resilience

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// tokenbucketscript is the atomic redis lua script for token-bucket rate limiting.
// keys[1] = bucket key
// argv[1] = rate (tokens per second)
// argv[2] = burst capacity
// argv[3] = current timestamp in microseconds
// returns 1 if allowed, 0 if rate-limited.
const tokenBucketScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now_us = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "last_us")
local tokens = tonumber(data[1])
local last_us = tonumber(data[2])

if tokens == nil then
    tokens = burst
    last_us = now_us
else
    local elapsed_sec = math.max(0, now_us - last_us) / 1000000.0
    tokens = math.min(burst, tokens + elapsed_sec * rate)
    last_us = now_us
end

if tokens >= 1.0 then
    tokens = tokens - 1.0
    redis.call("HMSET", key, "tokens", tokens, "last_us", last_us)
    redis.call("EXPIRE", key, math.max(10, math.ceil(burst / math.max(1, rate)) * 2))
    return 1
else
    redis.call("HMSET", key, "tokens", tokens, "last_us", last_us)
    redis.call("EXPIRE", key, math.max(10, math.ceil(burst / math.max(1, rate)) * 2))
    return 0
end
`

// ratelimiterconfig defines parameters for distributed and local token-bucket rate limiting.
type RateLimiterConfig struct {
	// rate is the token refill rate per second per client key.
	Rate float64
	// burst is the maximum token bucket capacity per client key.
	Burst float64
	// redisaddr is an optional host:port for distributed redis lua token bucket limiting.
	// when empty or unreachable, the limiter automatically falls back to local in-memory buckets.
	RedisAddr string
	// redistimeout bounds the maximum latency spent waiting on redis before falling back locally.
	RedisTimeout time.Duration
}

// defaultratelimiterconfig returns production-tuned defaults for per-client rate limiting.
func DefaultRateLimiterConfig() RateLimiterConfig {
	return RateLimiterConfig{
		Rate:         5000,
		Burst:        1000,
		RedisAddr:    "",
		RedisTimeout: 15 * time.Millisecond,
	}
}

// extractclientkey extracts a client identifier from x-api-key, x-forwarded-for, or remoteaddr.
func ExtractClientKey(r *http.Request) string {
	if r == nil {
		return "default"
	}
	if apiKey := strings.TrimSpace(r.Header.Get("X-API-Key")); apiKey != "" {
		return "key:" + apiKey
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		if idx := strings.IndexByte(xff, ','); idx > 0 {
			return "ip:" + strings.TrimSpace(xff[:idx])
		}
		return "ip:" + xff
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && host != "" {
		return "ip:" + host
	}
	if r.RemoteAddr != "" {
		return "ip:" + r.RemoteAddr
	}
	return "default"
}

type localBucket struct {
	mu     sync.Mutex
	tokens float64
	lastNs int64
}

func (b *localBucket) allow(rate, burst float64, nowNs int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lastNs == 0 {
		b.tokens = burst - 1.0
		b.lastNs = nowNs
		return true
	}

	elapsedSec := float64(nowNs-b.lastNs) / 1e9
	if elapsedSec > 0 {
		b.tokens += elapsedSec * rate
		if b.tokens > burst {
			b.tokens = burst
		}
		b.lastNs = nowNs
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}
	return false
}

// ratelimiter enforces token-bucket rate limits via redis lua with automatic in-memory fallback.
type RateLimiter struct {
	cfg            RateLimiterConfig
	localBuckets   sync.Map // string -> *localBucket
	redisFallbacks atomic.Int64
	rejectedTotal  atomic.Int64
	pool           chan net.Conn
}

// newratelimiter constructs a ratelimiter with optional redis backing and local in-memory fallback.
func NewRateLimiter(cfg RateLimiterConfig) *RateLimiter {
	if cfg.Rate <= 0 {
		cfg.Rate = 5000
	}
	if cfg.Burst < 1 {
		cfg.Burst = 1000
	}
	if cfg.RedisTimeout <= 0 {
		cfg.RedisTimeout = 15 * time.Millisecond
	}

	rl := &RateLimiter{
		cfg: cfg,
	}
	if cfg.RedisAddr != "" {
		rl.pool = make(chan net.Conn, 32)
	}
	return rl
}

// allow checks whether a request for the given client key is permitted within its rate quota.
func (rl *RateLimiter) Allow(clientKey string) bool {
	if clientKey == "" {
		clientKey = "default"
	}

	if rl.cfg.RedisAddr != "" {
		allowed, err := rl.allowRedis(clientKey)
		if err == nil {
			if !allowed {
				rl.rejectedTotal.Add(1)
			}
			return allowed
		}
		// redis unavailable or timed out: fail open into local in-memory token bucket.
		rl.redisFallbacks.Add(1)
	}

	allowed := rl.allowLocal(clientKey)
	if !allowed {
		rl.rejectedTotal.Add(1)
	}
	return allowed
}

func (rl *RateLimiter) allowLocal(clientKey string) bool {
	nowNs := time.Now().UnixNano()
	val, ok := rl.localBuckets.Load(clientKey)
	if !ok {
		fresh := &localBucket{}
		val, _ = rl.localBuckets.LoadOrStore(clientKey, fresh)
	}
	bucket := val.(*localBucket)
	return bucket.allow(rl.cfg.Rate, rl.cfg.Burst, nowNs)
}

func (rl *RateLimiter) getConn() (net.Conn, error) {
	select {
	case conn := <-rl.pool:
		return conn, nil
	default:
		return net.DialTimeout("tcp", rl.cfg.RedisAddr, rl.cfg.RedisTimeout)
	}
}

func (rl *RateLimiter) putConn(conn net.Conn) {
	select {
	case rl.pool <- conn:
	default:
		_ = conn.Close()
	}
}

func (rl *RateLimiter) allowRedis(clientKey string) (bool, error) {
	conn, err := rl.getConn()
	if err != nil {
		return false, err
	}

	_ = conn.SetDeadline(time.Now().Add(rl.cfg.RedisTimeout))

	redisKey := "avry:rl:" + clientKey
	rateStr := strconv.FormatFloat(rl.cfg.Rate, 'f', 2, 64)
	burstStr := strconv.FormatFloat(rl.cfg.Burst, 'f', 2, 64)
	nowUsStr := strconv.FormatInt(time.Now().UnixMicro(), 10)

	cmd := encodeRESPArray(
		"EVAL",
		tokenBucketScript,
		"1",
		redisKey,
		rateStr,
		burstStr,
		nowUsStr,
	)

	if _, err := io.WriteString(conn, cmd); err != nil {
		_ = conn.Close()
		return false, err
	}

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return false, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) < 2 || line[0] != ':' {
		_ = conn.Close()
		return false, fmt.Errorf("unexpected redis response: %s", line)
	}

	val, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil {
		_ = conn.Close()
		return false, err
	}

	rl.putConn(conn)
	return val == 1, nil
}

func encodeRESPArray(args ...string) string {
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, arg := range args {
		b.WriteString("$")
		b.WriteString(strconv.Itoa(len(arg)))
		b.WriteString("\r\n")
		b.WriteString(arg)
		b.WriteString("\r\n")
	}
	return b.String()
}

// redisfallbacks returns the number of times redis evaluation failed and fell back to local memory.
func (rl *RateLimiter) RedisFallbacks() int64 {
	return rl.redisFallbacks.Load()
}

// rejectedtotal returns the total number of rate-limited requests.
func (rl *RateLimiter) RejectedTotal() int64 {
	return rl.rejectedTotal.Load()
}
