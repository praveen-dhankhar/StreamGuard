# StreamGuard

**Product Requirements Document**

**v2.0 · Supersedes v1.0 · Authoritative specification**

| | |
|---|---|
| **Project type** | Portfolio / systems engineering |
| **Status** | v2.0 — supersedes v1.0 |
| **Author** | Parveen Kumar |
| **Last updated** | 19 June |
| **Companion docs** | TRD · Backend Schema · App Flow · UI/UX Brief · Implementation Workflow |

---

## 1 Overview

StreamGuard is a Go-based reverse proxy for streaming LLM API requests. It sits between a client application and one or more upstream LLM providers (OpenAI, Anthropic, etc.) and is responsible for three things: detecting upstream failures mid-stream, recovering from them through a single well-defined cascade algorithm, and communicating every failure and recovery action to the client through an explicit wire protocol — rather than leaving failure handling as an undocumented implementation detail.

The project demonstrates depth in three systems engineering areas: streaming protocol design under failure, concurrent failure detection without false positives, and the gap between 'what the proxy counted' and 'what the provider billed' — with a defined, empirically-grounded reconciliation mechanism.

## 2 Problem Statement

When a client streams a response from an LLM provider over SSE and the upstream fails mid-stream — connection drop, hang, or malformed response — most proxy implementations either:

- Let the client connection die silently, with no explanation and no diagnostic information.
- Silently retry against a different provider with no defined contract for what the client should do with the partial response it has already rendered.

Case (1) produces a poor user experience. Case (2) produces undefined client behavior: UI frameworks cannot know whether to discard, append, or flag partial text they have already displayed. StreamGuard defines and implements a documented contract for both cases.

## 3 Goals

| ID | Goal |
|---|---|
| G1 | Detect three distinct upstream failure modes — dead socket, silent hang, malformed frame — reliably and without false positives on normal SSE traffic, including legitimately split TCP frames. |
| G2 | Recover from failures via a single, server-configured cascade algorithm across configured providers. |
| G3 | Communicate every failure and recovery action through a documented wire protocol with precise, unambiguous semantics. |
| G4 | Track token usage live and reconcile against provider-reported usage on a defined schedule, with an empirically-measured drift threshold. |
| G5 | Prove correctness under concurrency via race-detector tests and a chaos-injected load test. |
| G6 | Provide a real consumer of billing events — the usage ledger — so billing events are never emitted into the void. |

## 4 Non-Goals

These items were each considered and deliberately deferred. All are candidates for a follow-on project.

| ID | Not being built | Reason |
|---|---|---|
| NG1 | Shared circuit-breaker state across proxy replicas | Single-instance only in v1. Extension path: Redis-backed breaker state. Documented in §10. |
| NG2 | Output-quality benchmarking across recovery strategies | Requires human eval or LLM-judge pipeline — a separate project. |
| NG3 | Dynamic cost- or latency-based provider routing | Orthogonal to the failure-handling problem. Priority order is static config. |
| NG4 | Enterprise authentication (SSO, RBAC, multi-tenant) | Auth in v1 is API-key budget + provider allowlist only. |

## 5 Stakeholders

Requirements are written for the primary stakeholder class only. The secondary class is listed for transparency but drives no additional functional requirements.

| Stakeholder | Job to be done | Notes |
|---|---|---|
| Backend developers | Integrate resilient LLM streaming without hand-rolling failover logic. | Primary. All requirements are written for this class. |
| Technical interviewers | Evaluate engineering judgment behind the implementation. | Secondary. Zero additional functional requirements beyond what the primary class already requires. |

## 6 Functional Requirements

### FR-1 Provider Cascade Algorithm

On detecting a failure, the proxy discards the partial upstream response and replays the full original request — the complete messages array and all original parameters, unmodified — against the next provider in the configured priority list. This repeats until a provider succeeds or the list is exhausted.

**Edge cases that must be explicitly handled**

- **Single-provider config:** if only one provider is configured and it fails, the list is immediately exhausted. Emit `gateway_truncated` with reason `all_providers_exhausted`.
- **Open circuit at request start:** if a provider's circuit is open when a new request arrives, the cascade skips it silently and starts with the next available provider. No `gateway_failover` event is emitted for a provider that was never attempted on this request.
- **Replay semantics:** the entire original request body is replayed. No partial text from the failed attempt is included. The new provider starts from the beginning of the response.

### FR-2 Wire Protocol — SSE Events

The proxy emits the following event types interleaved with normal content chunks. Each event type is defined below with its JSON shape, field constraints, and the conditions that trigger it.

#### gateway_status

Emitted once, at stream start, before any content chunks. **Not re-emitted after a failover** — the provider after failover is communicated via `gateway_failover.provider_to`. The reference client must update its provider display from that field, not from a new `gateway_status`.

```
event: gateway_status
data: {"state":"healthy","provider":"openai"}
```

#### gateway_failover

Emitted each time the cascade controller moves to the next provider. The `reason` field uses exactly one of the three failure modes defined in FR-4. The `attempt` field is 1-indexed: the first failover carries `attempt: 1`.

```
event: gateway_failover
data: {
  "reason": "dead_socket",
  "tokens_delivered_before_failure": 142,
  "provider_from": "openai",
  "provider_to": "anthropic",
  "attempt": 1
}
```

| Field | Constraint |
|---|---|
| `reason` | `"dead_socket"` \| `"silent_hang"` \| `"malformed"` only. The value `"upstream_timeout"` does not exist in this protocol — it was an error in the v1 PRD. |
| `tokens_delivered_before_failure` | Integer ≥ 0. Counts tokens delivered to the client before the failure. Does not claim knowledge of how many tokens would have been generated — the proxy never has that information. |
| `attempt` | Integer ≥ 1. The 1-indexed ordinal of this failover in the cascade. First failover = 1, second = 2. |

#### gateway_regenerating

Emitted immediately after `gateway_failover`. Instructs the client to retain the partial response buffer (dim it, do not discard it) while the cascade replays the prompt against the new provider.

```
event: gateway_regenerating
data: {"keep_partial_visible": true}
```

#### gateway_truncated

Terminal event emitted when the provider list is exhausted **or when the API key's token budget is exhausted mid-stream**. After this event the connection closes. No further retries are attempted by the proxy; retry policy is the calling application's decision.

```
event: gateway_truncated
data: {
  "reason": "all_providers_exhausted",
  "tokens_delivered": 142,
  "final": true
}
```

| reason value | When it fires |
|---|---|
| `"all_providers_exhausted"` | Every configured provider has been attempted and failed on this request. |
| `"budget_exceeded"` | The API key's token budget was reached during an active stream. The stream is terminated at the current token count. |

**Field naming discipline**

All field names describe only what the proxy actually knows. `tokens_delivered_before_failure` rather than `tokens_lost` — the proxy cannot know how many tokens would have been generated, only how many were received before the failure.

### FR-3 Reference Client

A minimal reference client must be implemented that consumes the wire protocol and demonstrates the `gateway_regenerating` UX: on receiving `gateway_regenerating`, the partial response dims (not is cleared); new content appears below the dimmed block once the next provider responds; both blocks return to full opacity together on stream completion. The reference client updates its provider display from `gateway_failover.provider_to` — not from a new `gateway_status` event, because none is emitted after a failover.

### FR-4 Failure Detection

| Mode | Detection | Requirements |
|---|---|---|
| Dead socket | Read returns EOF or transport error. | Trigger is immediate. No buffering or timeout required. |
| Silent hang | Read timeout on inter-token gap. | Deadline must be calibrated from measured P99 inter-token latency on real provider traffic — not an assumed constant. Set at a documented multiple of the measured P99 (e.g. 5×). The README must include the measured P99 value, the multiplier chosen, and the resulting deadline. |
| Malformed frame | Schema validation on a fully reassembled SSE frame. | Partial TCP reads must be buffered and reassembled before validation. A classification of 'malformed' fires only when a complete, fully-buffered frame fails the provider's expected SSE event schema. |

**A unit test must demonstrate that a frame split across two or more reads does not trigger a false-positive malformed classification.**

### FR-5 Token Accounting — Live

The proxy counts tokens per request using each provider's actual tokenizer — not heuristic estimation. These counts enforce a per-API-key sliding-window rate limit in real time. The sliding window duration is a required configuration parameter (default: 60 seconds) specified in `config.yaml`.

**Token counting behavior during failover**

- Tokens from a failed provider attempt count toward rate limiting for the originating API key. This prevents a client from gaming the limit by triggering repeated failovers.
- Tokens from failed provider attempts do NOT count toward `tokens_billed` in the usage ledger. Only tokens actually delivered to the client are billed.

### FR-6 Token Accounting — Reconciliation

A scheduled, explicitly offline/batch job runs on a configurable interval (default: 1 hour) and compares locally-counted tokens against each provider's usage-reporting endpoint. The job is offline because providers report usage with a lag; it is not a real-time billing guarantee.

**Drift threshold**

If drift between local count and provider-reported count exceeds the configured threshold for a billing window, the window is flagged for manual review and logged. The threshold must be derived from a measured baseline drift distribution (e.g. P95 of observed drift over the calibration period) — not an assumed constant. The README must include the measured distribution and the threshold value derived from it.

**drift_flag lifecycle**

The `drift_flag` on a LedgerEntry is set when drift exceeds the threshold for a billing window. It is cleared by a subsequent reconciliation pass that finds drift within threshold for the same window. A flag that is set once and never cleared provides no ongoing signal and is useless — clearing is required behavior, not optional.

**Idempotency**

The reconciliation job is keyed by `(api_key_hash, billing_period)`. Running the job twice for the same window must produce the same result: if the window was already flagged and drift is still above threshold, the flag remains set with no additional side effects. If drift has since resolved, the flag clears. Duplicate runs must not double-count tokens or double-flag windows.

### FR-7 Usage Ledger

An in-memory usage ledger maintains a running per-API-key token total, exposed via `GET /usage/{key}`. The ledger records token counts on three distinct events: successful stream completion, `gateway_truncated` (partial delivery from either provider exhaustion or budget exhaustion), and reconciliation drift flag updates.

**Authentication on GET /usage/{key} — required, not optional**

This endpoint requires a valid API key in the `Authorization: Bearer` header. A key may only read its own usage data: the key in the header must match the `{key}` in the path.

An unauthorized request — missing Authorization header, invalid key, or a key attempting to read another key's data — returns `403 Forbidden`.

This is not an optional hardening step. An unauthenticated `/usage` endpoint that returns per-key token counts is a data leak by design.

`GET /usage/{key}` response shape:

```json
{
  "api_key": "sg_live_***",
  "tokens_billed": 184213,
  "truncated_requests": 3,
  "last_reconciled_at": "2026-06-19T03:00:00Z",
  "drift_flag": false
}
```

### FR-8 Authentication and Budget Enforcement

API keys are scoped to a token budget and a provider allowlist. Enforcement happens at two distinct points in the request lifecycle.

**Pre-stream rejection (HTTP status codes)**

These checks happen before the SSE stream opens and return plain HTTP responses:

- Invalid or missing API key → `401 Unauthorized`
- Requested provider not in the key's allowlist → `403 Forbidden`
- Key's token budget already at zero → `429 Too Many Requests`

**Mid-stream budget exhaustion**

If a key's budget is exhausted during an active stream, the proxy emits `gateway_truncated` with `"reason":"budget_exceeded"` and closes the connection. This is **not** a 429 HTTP response — the stream is already open and must be terminated via the wire protocol, not an HTTP status code. The token count at the point of termination is recorded in the ledger.

**Logging and redaction**

Request and response logs redact prompt and completion content by default. Wire protocol metadata — event types, token counts, provider names, detection times, failure reasons — is loggable and is not redacted.

### FR-9 Graceful Shutdown

On SIGTERM, the proxy stops accepting new connections and allows in-flight streams to complete up to the configurable drain timeout (`drain_timeout_s` in `config.yaml`).

**Drain behavior for streams in failover state**

- A stream that is mid-failover — waiting for a new provider to respond after a failure was detected — is still in-flight and is included in the drain window.
- The drain timeout runs from the moment SIGTERM is received, not from when the stream started.
- If the drain window expires while a failover stream is waiting for an upstream provider, the proxy force-closes the connection, logs the forced termination, and records the partial token count in the usage ledger.

## 7 Circuit Breaker Specification

Each configured provider has its own circuit breaker with three states. The parameters below are defaults; all must be configurable via `config.yaml` under a `circuit_breaker` key.

| State | Definition | Transitions |
|---|---|---|
| closed | Normal operation. Requests pass through. | → open when `consecutive_failures` reaches `failure_threshold` (default: 3). |
| open | Provider is known-bad. Requests are skipped without contacting the upstream. | → half_open after `open_timeout_s` elapses since last state change (default: 30s). |
| half_open | One probe request is allowed through to test recovery. | → closed if the probe succeeds (default: 1 success required). → open if the probe fails. |

**Cascade controller interaction with circuit state**

When the cascade picks a provider, it checks circuit state first:

- **open:** skip the provider immediately. Do not contact the upstream. Do not emit `gateway_failover` — this provider was never attempted on this request.
- **half_open:** allow one probe attempt. If the probe succeeds, the stream continues normally and the circuit closes. If the probe fails, the cascade moves to the next provider and the circuit returns to open.
- **closed:** proceed normally.

**Configurable parameters**

| Parameter | Default | Description |
|---|---|---|
| `failure_threshold` | 3 | Consecutive failures on a provider before opening its circuit. |
| `open_timeout_s` | 30 | Seconds a circuit stays open before transitioning to half_open. |
| `half_open_success_threshold` | 1 | Successful probe requests required to close the circuit from half_open. |

## 8 Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-1 | All shared-state components — rate limiter, circuit breaker, usage ledger — must pass `go test -race` with no data race reports. |
| NFR-2 | A load test must demonstrate 50+ concurrent streams with failures injected into a random subset via the chaos harness. The test must complete with no corrupted ledger state, no panics, and the ledger's final token total must match the sum of individually-tracked expected outcomes. |
| NFR-3 | Detection time for each of the three failure modes must be measured from actual test runs and reported in the README — not assumed or approximated. |
| NFR-4 | The chaos harness must be gated by a build tag or environment variable (e.g. `STREAMGUARD_CHAOS_ENABLED=true`). It must not be compiled into or active in non-test builds. It must be documented as a deliberate fault-injection tool. |

## 9 Acceptance Criteria

All items below must be satisfied. Items that reference measured data cannot be satisfied with placeholder or assumed values.

- All three failure modes detected correctly in automated tests, including a negative test that proves a legitimately split TCP frame does not trigger a false-positive malformed classification.
- Wire protocol (FR-2) implemented end-to-end with correct event types, correct reason enums, and correct attempt numbering. Consumed by the reference client with the `gateway_regenerating` UX visibly demonstrated.
- Reference client updates provider display from `gateway_failover.provider_to` — verified in a demo showing a mid-stream provider switch.
- Silent-hang timeout in `config.yaml` documented alongside the measured P99 inter-token latency baseline and the multiplier used to derive it.
- Reconciliation drift threshold documented alongside the measured baseline drift distribution it was derived from.
- `go test -race` passes on all shared-state packages. The load test (NFR-2) passes and results — final ledger total, zero panics, zero races — are recorded in the README.
- `GET /usage/{key}` returns a correct token total in an integration test that covers both a `gateway_truncated` event and a successful stream completion.
- `GET /usage/{key}` returns 403 when called with a key that does not match the path parameter.
- `gateway_truncated` with reason `budget_exceeded` is emitted and verified in an integration test that exhausts a key's token budget mid-stream.
- Circuit breaker transitions (closed → open → half_open → closed and closed → open → half_open → open) verified by the chaos harness across all paths.
- `drift_flag` is set when drift exceeds threshold and cleared when a subsequent reconciliation pass finds drift within threshold — both transitions verified in tests.
- Graceful shutdown verified: SIGTERM during an active stream either allows it to complete within the drain timeout or force-closes it cleanly with the partial token count recorded in the ledger.
- Chaos harness is gated behind `STREAMGUARD_CHAOS_ENABLED=true`. A build without the flag must not compile or activate the harness.
- README includes an Out of Scope section matching §4 of this document.

## 10 Known Limitations

These limitations apply to v1 and are documented here and in the README.

- **Single-instance only.** Circuit-breaker state and the usage ledger are in-process. A restart loses the accumulated ledger. No state is shared across proxy replicas. Extension path: Redis-backed breaker state and a persistent ledger store.
- **Reconciliation is batch and offline.** Provider usage data is available with a reporting lag. The reconciliation job is not a real-time billing guarantee.
- **Output quality across cascade attempts is not benchmarked.** The cascade optimizes for availability, not response quality. A failover to a different provider may return a stylistically different response.
- **Mid-stream provider swap is visible to the user.** The `gateway_regenerating` UX mitigates but does not eliminate the disruption of a provider switch.
- **Provider tokenizer drift.** If a provider updates its tokenizer after deployment, local counts may diverge from provider-reported counts until the tokenizer library is updated. The reconciliation job will detect this drift and flag it.

## 11 Milestones

**Calibration scheduling constraint — read before treating this as a simple checklist**

The silent-hang timeout and reconciliation drift threshold both require measured data: P99 inter-token latency and observed drift distribution respectively. This data accumulates over time. Background instrumentation must start on Day 1 so that by Week 4 there is real data to compute against. Week 2 delivers a placeholder timeout (clearly marked in code). Week 4 replaces it with the calibrated value. Do not attempt calibration in Week 2 — there is no data yet.

| Week | Deliverables |
|---|---|
| 1 | Base proxy, SSE pass-through, cascade algorithm (FR-1, including single-provider and open-circuit edge cases), live token counting + rate limiter with configurable window (FR-5). Background latency and drift logging starts this week — this is the data Week 4 calibration depends on. |
| 2 | Wire protocol end-to-end (FR-2, all events, correct reason enum — not `upstream_timeout`), reference client with `gateway_regenerating` UX (FR-3), frame-reassembly parser with false-positive unit test (FR-4). Hang-detection uses a placeholder timeout, clearly marked `// TODO: replace with calibrated value in Phase 4`. |
| 3 | Chaos harness gated by `STREAMGUARD_CHAOS_ENABLED` (NFR-4), all three failure modes exercised. Circuit breaker full state machine with all three transition paths (§7). Usage ledger with authenticated `GET /usage/{key}` (FR-7). Budget mid-stream exhaustion with `gateway_truncated` `reason:budget_exceeded` (FR-8). `go test -race` and concurrent load test (NFR-1, NFR-2). |
| 4 | Calibration: pull three weeks of latency and drift logs, compute real P99 and drift P95, replace placeholder values, document measurements in README. Reconciliation job with idempotent, lifecycle-correct `drift_flag` behavior (FR-6). Graceful shutdown with failover-state drain behavior (FR-9). All acceptance criteria (§9) verified top to bottom. |

## 12 Risks

| Risk | Mitigation |
|---|---|
| Calibration data unavailable by Week 4 because background logging was not started on Day 1. | The implementation workflow is explicit on this: logging starts in Phase 1, Day 5. If logging is broken or skipped, fix it and extend the timeline rather than fabricating threshold values. |
| False-positive hang detections on slow but valid provider responses. | The measured P99 baseline combined with the configured multiplier provides margin above normal variation. The multiplier and distribution are documented and visible; they can be tuned if false positives appear. |
| Concurrency bugs in shared state appear only under load. | NFR-1 and NFR-2 are hard acceptance gates, not optional polish. Flaky race-detector tests indicate an unresolved concurrency bug and block proceeding to Phase 4. |
| Chaos harness accidentally active in a production-like build. | NFR-4 requires a build-time gate. Acceptance criteria include verifying that a build without `STREAMGUARD_CHAOS_ENABLED` does not compile or activate the harness. |
| Provider tokenizer update causes persistent reconciliation drift after deployment. | Documented as a known limitation (§10). The reconciliation job will detect and flag the drift. Resolution is a library update, not a protocol change. |
| Scope creep toward features listed in §4. | Section 4 is the scope boundary. Any addition requires removing something else of equivalent scope. The implementation workflow phase gates enforce ordering and prevent premature feature work. |
