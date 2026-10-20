# avry-proxy

Adaptive L7 Resilience Proxy & Chaos Testbed written in Go.

## Overview

`avry-proxy` is a high-concurrency L7 HTTP reverse proxy engineered to mitigate grey failures, tail-latency amplification, and cascading overload across upstream clusters.

### Core Capabilities
- **Adaptive Load Balancing:** Power of Two Choices (P2C) weighted by lock-free Exponentially Weighted Moving Average (EWMA) latency and active in-flight concurrency, plus a baseline Round-Robin mode for A/B benchmarking.
- **Per-Node Circuit Breaking:** Rolling state machine (`Closed` -> `Open` -> `Half-Open`) isolating degraded upstreams before thread/connection pools saturate.
- **Distributed Token-Bucket Rate Limiting:** Redis-backed atomic Lua token bucket with automatic fail-open fallback to a local in-memory limiter if Redis becomes unavailable.
- **Priority-Aware Load Shedding:** Protects critical traffic SLOs (`X-Traffic-Priority: critical | high | low`) by shedding low-priority load when global concurrency or tail latency crosses thresholds.
- **Chaos Worker & Observability:** Upstream worker service with live `/chaos` fault injection (latency spikes, jitter, HTTP error bursts) and Prometheus/Grafana telemetry.

## System Architecture

```mermaid
flowchart LR
    Client["Clients / Load Generator"]
    Chaos["Chaos Controller"]

    subgraph Proxy["avry-proxy (Go)"]
        direction LR
        RL["Rate Limiter"] --> LS["Priority Shedder"] --> LB["P2C + EWMA Balancer"] --> CB["Circuit Breakers"]
    end

    Redis[("Redis (Lua Token Bucket)")]

    subgraph Upstreams["Upstream Workers"]
        W1["worker-1 (Healthy: 5ms)"]
        W2["worker-2 (Healthy: 5ms)"]
        W3["worker-3 (Grey Failure: 450ms)"]
    end

    subgraph Telemetry["Observability"]
        Prom["Prometheus"] --> Graf["Grafana"]
    end

    Client -->|"HTTP + X-Traffic-Priority"| RL
    RL <-->|"Atomic Quota (Local Fallback)"| Redis
    CB -->|"Fast Route"| W1
    CB -->|"Fast Route"| W2
    CB -.->|"Deprioritized / Tripped"| W3
    Chaos -.->|"POST /chaos"| W3
    Proxy -.->|"/metrics"| Prom
```

## Quick Start

```bash
make test
make build
```
