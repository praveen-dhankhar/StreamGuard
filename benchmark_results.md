# StreamGuard Benchmark Results

**Platform:** Apple M2, darwin/arm64, Go 1.26.4
**Date:** 2026-06-20
**Benchmark command:** `go test -run '^$' -bench . -benchmem ./internal/...`
**Packages benchmarked:** 10 internal packages, 63 benchmarks

---

## Summary

| Package | Benchmarks | Result |
|---------|------------|--------|
| `auth` | 7 | Passed |
| `breaker` | 7 | Passed |
| `budget` | 5 | Passed |
| `calibration` | 8 | Passed |
| `cascade` | 4 | Passed |
| `ledger` | 7 | Passed |
| `parser` | 5 | Passed |
| `protocol` | 6 | Passed |
| `ratelimit` | 5 | Passed |
| `tokenizer` | 9 | Passed |

`config`, `reconcile`, and `server` have tests but no benchmark files. `mockupstream` has no test files.

---

## Detailed Results

### Auth - Key Hashing, Redaction, and Lookups

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `HashKey` | 159.8 | 128 | 2 |
| `Redact` | 19.23 | 0 | 0 |
| `RedactShort` | 18.35 | 0 | 0 |
| `LookupRaw` | 183.9 | 128 | 2 |
| `LookupHash` | 22.40 | 0 | 0 |
| `LookupRawContended` | 135.9 | 128 | 2 |
| `LookupManyKeys` | 205.2 | 128 | 2 |

`LookupHash` bypasses SHA-256 and is about 8x faster than `LookupRaw` in this run. Raw lookups allocate because they hash the provided key on each call.

### Circuit Breaker

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `AllowAttemptClosed` | 41.04 | 0 | 0 |
| `AllowAttemptContended` | 171.6 | 0 | 0 |
| `RecordSuccess` | 39.62 | 0 | 0 |
| `RecordFailure` | 88.51 | 0 | 0 |
| `StateTransitionCycle` | 244.3 | 0 | 0 |
| `Snapshot` | 263.9 | 392 | 5 |
| `SnapshotContended` | 397.6 | 392 | 5 |

The circuit-breaker hot path is allocation-free. `Snapshot` allocates due to `map[string]any` construction and should stay off per-token paths.

### Budget - Atomic Token Reservations

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `TryReserve` | 13.41 | 0 | 0 |
| `TryReserveContended` | 190.6 | 0 | 0 |
| `Exhausted` | 0.5140 | 0 | 0 |
| `Allows` | 3.604 | 0 | 0 |
| `AllowsMiss` | 4.244 | 0 | 0 |

Budget checks are allocation-free. The CAS-based reservation remains cheap even under contention.

### Calibration - Percentile Computation

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `Sample` | 39.73 | 44 | 0 |
| `SampleContended` | 194.8 | 40 | 0 |
| `Percentile100` | 322.2 | 896 | 1 |
| `Percentile1000` | 2,627 | 8,192 | 1 |
| `Percentile10000` | 22,245 | 81,920 | 1 |
| `SilentHangDeadline` | 2,556 | 8,192 | 1 |
| `DriftThreshold` | 344.5 | 896 | 1 |
| `Clamp` | 0.5124 | 0 | 0 |

`Percentile` copies and sorts the sample slice on each call. At 10,000 samples it costs about 22 microseconds and 80 KiB per call, so it should stay on calibration/control-plane paths.

### Cascade - Session Management

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `NewSession` | 4.901 | 0 | 0 |
| `StartAttempt` | 50.49 | 176 | 1 |
| `FinishAttempt` | 4.739 | 0 | 0 |
| `FullCascadeFlow` | 89.72 | 176 | 1 |

The full two-provider failover session flow completes in about 90 ns with one allocation from attempt slice growth.

### Ledger - Billing Record Storage

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `RecordTerminal` | 207.4 | 88 | 3 |
| `RecordTerminalContended` | 588.8 | 88 | 3 |
| `BillingPeriod` | 159.3 | 56 | 2 |
| `SummarySingleEntry` | 70.59 | 0 | 0 |
| `SummaryManyEntries` | 1,278 | 0 | 0 |
| `UpsertReconciliation` | 90.69 | 56 | 2 |
| `Entries` | 10,594 | 16,896 | 101 |

`Entries()` deep-clones stored records, including provider token maps. The copy is safe for callers, but expensive enough to avoid in tight loops.

### Parser - SSE Frame Parsing

| Benchmark | ns/op | MB/s | B/op | allocs/op |
|-----------|-------|------|------|-----------|
| `ParseFrameContent` | 1,347 | 42.32 | 656 | 15 |
| `ParseFrameStatus` | 1,014 | 68.06 | 504 | 11 |
| `ParseFrameDone` | 179.8 | 77.85 | 72 | 3 |
| `ParseFrameLargeContent` | 5,643 | 96.75 | 2,688 | 15 |
| `ReaderNext` | 9,971 | - | 1,272 | 24 |

Content parsing is dominated by JSON unmarshalling in `extractContent`. The `[DONE]` path is much cheaper but still allocates three times per frame.

### Protocol - SSE Writing

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `WriteSSE` | 299.6 | 88 | 3 |
| `WriteSSEFailover` | 441.2 | 168 | 3 |
| `WriteContent` | 398.9 | 144 | 4 |
| `WriteContentLong` | 850.2 | 288 | 4 |
| `ValidateFailoverReason` | 0.6138 | 0 | 0 |
| `ValidateTruncatedReason` | 0.6031 | 0 | 0 |

`WriteContent` uses a typed payload and preserves the OpenAI-compatible wire shape while reducing the prior generic-map path from 19 allocations to 4 allocations per call.

### Rate Limiter - Sliding Window

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `AdmitSingleKey` | 43.54 | 0 | 0 |
| `AdmitManyKeys` | 58.25 | 0 | 0 |
| `AddSingleKey` | 143,233 | 182 | 0 |
| `AdmitContended` | 288.9 | 0 | 0 |
| `AdmitAfterPrune` | 43.48 | 0 | 0 |

`AddSingleKey` is an outlier because the benchmark continuously appends to the same key without advancing beyond the pruning window. Production behavior should be bounded by the configured sliding window.

### Tokenizer - Token Counting

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `ChunkCounterShort` | 46.97 | 32 | 1 |
| `ChunkCounterLong` | 2,338 | 3,456 | 1 |
| `ChunkCounterEmpty` | 0.5280 | 0 | 0 |
| `ProviderAwareCounterOpenAI` | 11,142 | 3,498 | 56 |
| `ProviderAwareCounterAnthropic` | 147.9 | 96 | 1 |
| `ProviderAwareCounterFallback` | 148.3 | 96 | 1 |
| `ProviderAwareCounterMockHint` | 137.4 | 96 | 1 |
| `RegistryObserve` | 23.87 | 0 | 0 |
| `RegistrySnapshot` | 27.06 | 0 | 0 |

OpenAI local counting remains the most expensive benchmarked per-call operation because it uses `tiktoken-go`. Anthropic production billing now relies on streamed provider usage and this fallback path stays under 200 ns for local/mock-style counting.

---

## Hot Path Snapshot

| Operation | Approximate Cost | Notes |
|-----------|------------------|-------|
| Budget exhaustion check | 0.51 ns | Atomic load |
| Budget reservation | 13.41 ns | Lock-free CAS |
| Circuit breaker allow check | 41.04 ns | Mutex-protected |
| Rate-limit admission | 43.54 ns | Mutex plus prune check |
| API key raw lookup | 183.9 ns | Includes SHA-256 hash |
| SSE content write | 0.40 microseconds | Struct payload plus JSON marshal |
| SSE content parse | 1.35 microseconds | JSON unmarshal |
| OpenAI tiktoken count | 11.14 microseconds | 56 allocations |

## Optimization Candidates

1. Keep calibration percentile calculations off hot paths, or switch to a streaming/sampled percentile strategy if they become request-path work.
2. Investigate OpenAI tokenizer allocation reuse if token counts become a dominant production cost.
3. Add benchmark coverage for provider-native adapter request construction if adapter work becomes performance-sensitive.

## Benchmark Files

| Package | Benchmark File |
|---------|----------------|
| `auth` | [internal/auth/auth_bench_test.go](./internal/auth/auth_bench_test.go) |
| `breaker` | [internal/breaker/breaker_bench_test.go](./internal/breaker/breaker_bench_test.go) |
| `budget` | [internal/budget/budget_bench_test.go](./internal/budget/budget_bench_test.go) |
| `calibration` | [internal/calibration/calibration_bench_test.go](./internal/calibration/calibration_bench_test.go) |
| `cascade` | [internal/cascade/cascade_bench_test.go](./internal/cascade/cascade_bench_test.go) |
| `ledger` | [internal/ledger/ledger_bench_test.go](./internal/ledger/ledger_bench_test.go) |
| `parser` | [internal/parser/parser_bench_test.go](./internal/parser/parser_bench_test.go) |
| `protocol` | [internal/protocol/protocol_bench_test.go](./internal/protocol/protocol_bench_test.go) |
| `ratelimit` | [internal/ratelimit/ratelimit_bench_test.go](./internal/ratelimit/ratelimit_bench_test.go) |
| `tokenizer` | [internal/tokenizer/tokenizer_bench_test.go](./internal/tokenizer/tokenizer_bench_test.go) |

## Verification

```text
go test ./internal/...
go test -run '^$' -bench . -benchmem ./internal/...
```
