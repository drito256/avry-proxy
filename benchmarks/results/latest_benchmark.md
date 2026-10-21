### Grey Failure Benchmark Results (Asymmetric Node Degradation)

**Workload Configuration:**
- **Cluster Topology:** 4 upstream workers (`5ms` baseline latency) behind `avry-proxy`
- **Fault Injection:** At $t = 1s$, `worker-3` experiences a **grey failure** (`+450ms` latency spike + `20%` HTTP 500 error rate)
- **Target Load:** ~2000 req/s (64 concurrent clients, mixed `critical` / `high` / `low` priorities)

| Metric | Baseline (`round-robin`) | `avry-proxy` (`p2c-ewma` + CB + Shedder) | Improvement |
| :--- | :---: | :---: | :---: |
| **p50 Latency** | 6.48 ms | **7.15 ms** | Steady ~5–7ms baseline |
| **p95 Latency** | 456.44 ms | **10.74 ms** | **97.6% reduction** |
| **p99 Tail Latency** | 457.24 ms | **11.58 ms** | **97.5% reduction** |
| **Traffic Routed to Degraded `worker-3`** | 25.2% (1322 reqs) | **3.2% (499 reqs)** | Isolated automatically |
| **Upstream 5xx Error Rate** | 2.96% | **0.03%** | Fast-failed & rerouted |
| **Critical Priority SLO Survival** | 95.85% | **99.69%** | Protected |
| **Sustained Throughput** | 654 req/s | **1979 req/s** | **3.0x higher** (no head-of-line blocking) |
