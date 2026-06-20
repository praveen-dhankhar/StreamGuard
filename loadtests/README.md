# StreamGuard k6 p95/p99 Load Test

This k6 test measures end-to-end `/v1/stream` SSE request duration, including upstream failover and full streamed response delivery. It reports and thresholds p95/p99 latency through both built-in `http_req_duration` and the custom `stream_duration` trend.

## Local Demo Run

Terminal 1:

```sh
go run ./cmd/demo-upstreams
```

Terminal 2:

```sh
OPERATOR_TOKEN=dev-operator-token go run ./cmd/streamguard
```

Terminal 3:

```sh
k6 run loadtests/k6_stream_p95_p99.js
```

## Configuration

Environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `TARGET_URL` | `http://localhost:8080/v1/stream` | StreamGuard stream endpoint |
| `API_KEY` | `sg_live_demo` | Client API key from `keys.yaml` |
| `MODEL` | `k6-p95-p99` | Request model value |
| `VUS` | `10` | Concurrent virtual users |
| `DURATION` | `30s` | Test duration |
| `P95_THRESHOLD_MS` | `2500` | Failing threshold for p95 stream duration |
| `P99_THRESHOLD_MS` | `3500` | Failing threshold for p99 stream duration |

Example:

```sh
VUS=25 DURATION=1m P95_THRESHOLD_MS=3000 P99_THRESHOLD_MS=4500 k6 run loadtests/k6_stream_p95_p99.js
```

Key output fields:

- `http_req_duration{endpoint:stream}`: k6's built-in full HTTP request duration.
- `stream_duration{endpoint:stream}`: explicit custom trend using `res.timings.duration`.
- `stream_ok{endpoint:stream}`: status/body checks for successful stream responses.

Recorded local runs:

- [2026-06-20 p95/p99 run](./results/k6_stream_p95_p99_2026-06-20.md)
