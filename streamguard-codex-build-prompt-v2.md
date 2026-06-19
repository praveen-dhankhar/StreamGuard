# StreamGuard — Codex Build Prompt (v2, hardened)

**How to use this file:** Copy `streamguard-prd.md`, `streamguard-trd.md`, `streamguard-backend-schema.md`,
`streamguard-app-flow.md`, `streamguard-ui-ux-brief.md`, and `streamguard-implementation-workflow.md` into the
repo (e.g. a `/specs` folder) before you start, then give Codex this entire file as its task. The six specs
are the canonical source of truth for *intent*. Section 1 below is the canonical source of truth for every
place that intent was ambiguous — where this file and a spec file disagree on a **mechanism** (not a goal),
this file wins, because it exists specifically to remove the ambiguity the spec files left open.

---

## 0. Mission

Build **StreamGuard**: a Go reverse proxy that sits between a client and multiple streaming LLM providers
(OpenAI, Anthropic). It detects three distinct mid-stream upstream failure modes, recovers via a single
well-defined cascade/failover algorithm, and communicates every failure and recovery action to the client
through an explicit, documented SSE wire protocol.

This is a systems-engineering project. Correct concurrency, an honest wire protocol, and
empirically-calibrated thresholds (not guessed constants) are the point. Do not shortcut these to move faster.
Work through Section 13's phases in order. Do not begin a phase before the previous phase's exit criteria are
met.

---

## 1. Explicit Design Decisions (read this section first — it resolves every ambiguity below)

The source specs are precise about *what must happen* and *what fields must exist*, but leave several
*mechanisms* genuinely underspecified. Guessing at these mechanisms is the most common way an agent silently
builds something that passes its own tests and is still wrong. Treat every decision below as a hard
requirement, not a suggestion — and carry each one into the README's "Known Limitations" / "Design Decisions"
section so it's visible, not buried.

| # | Ambiguity in the source specs | Decision (implement exactly this) |
|---|---|---|
| D1 | Tests can't hit real OpenAI/Anthropic APIs reliably, cheaply, or deterministically — and most build environments have no network/API keys at all. | Build a **mock upstream harness** (Section 9) first. The entire test suite — happy path, failure injection, chaos, load test, reconciliation — runs against it. Real-provider wiring exists and is *optionally* exercised behind an env-var + build-tag gate, never required for `go test ./...` to pass. |
| D2 | Does `tokens_delivered` in `gateway_truncated` mean cumulative-across-all-attempts, or just-the-final-attempt? Does a failed-over attempt's tokens ever get billed? | See Section 5's worked example. Short version: **everything the client ever received is cumulative** (`StreamSession.TokensDelivered`, also what populates `gateway_truncated.tokens_delivered`); **only the final, non-superseded attempt's tokens are billed** (`LedgerEntry.TokensBilled`). These are two different numbers and they are *supposed* to diverge whenever a failover happened. |
| D3 | FR-5 says rate limiting happens "in real time," but the wire protocol's `TruncatedReason` enum is closed at exactly two values and the schema forbids adding a third. There is no protocol-legal way to terminate an open stream for a rate-limit violation. | **Rate limiting only gates admission of new requests** (pre-stream `429`, same as budget-zero). It does not and cannot terminate an already-open stream — only budget exhaustion can do that, via `gateway_truncated`/`budget_exceeded`. The sliding window is still fed continuously by every token delivered (including failed-attempt tokens, per FR-5's anti-gaming rule) — it just never causes mid-stream truncation. |
| D4 | `half_open` is described as admitting "one probe," but the stated concurrency model (`RWMutex`, read-heavy) doesn't make probe admission exclusive. Two concurrent requests can both observe `half_open` and both treat themselves as the probe. | Probe admission must be an **atomic claim**, not a read. Use a CAS on an internal `probing bool` (or an explicit `half_open_probing` sub-state) so exactly one concurrent request becomes the probe; every other concurrent request that observes `half_open` while a probe is in flight is treated as if the circuit were `open` (skip silently, try the next provider). See Section 8. |
| D5 | A naive mock upstream streams chunks at loopback speed (sub-millisecond gaps), which produces a P99 near zero and therefore a useless or actively harmful `silent_hang_deadline_ms`. Symmetrically, a mock whose "provider-reported usage" always exactly matches the local count produces a 0% drift baseline that flags every real reconciliation pass. | The mock upstream **must** inject randomized, realistic per-token delay (Section 9) and the mock usage-reporting endpoint **must** inject a configurable non-zero offset from the true local count (Section 9). Calibrated outputs are also sanity-bounded (Section 10) so a degenerate run can't silently produce a nonsense deadline or threshold. |
| D6 | The example `GET /usage/{key}` response (`tokens_billed: 184213`) is far larger than one reconciliation window's worth of traffic, but `LedgerEntry` is keyed per `(api_key_hash, billing_period)` where a period defaults to 1 hour. | `billing_period` partitioning exists **only** to make reconciliation idempotent per window. `GET /usage/{key}` aggregates **across every `LedgerEntry` for that key**: sum `TokensBilled` and `TruncatedRequests`, OR all `DriftFlag` values together, take the max non-null `LastReconciledAt`. See Section 5.4 for the exact algorithm and the `billing_period` string-computation formula. |
| D7 | The reference client's required behaviors (dim text, color badges, "monospace is appropriate") could describe either a terminal program or a web UI, and the package layout (`client-ref/main.go`) implies the former but never says so. | The reference client is a **single Go CLI binary** using ANSI escape codes: faint/dim SGR for the retained partial block (never strikethrough), ANSI color for state badges with a `--no-color` fallback, and terminal output is inherently monospace. Do **not** build a browser UI, Electron app, or web frontend — that would be unrequested scope per the UI/UX brief's own "out of scope" list. |
| D8 | "Replay the full original request unmodified" against the next provider breaks the moment failover crosses from an OpenAI-style `model` string to an Anthropic one — the spec never addresses model-name translation. | **No cross-provider model-name translation in v1.** The original request body, including its `model` field, is forwarded byte-for-byte to whichever provider is attempted. It is the operator's responsibility to only configure providers that can serve a model identifier compatible with what clients send. Document this explicitly as a Known Limitation (Section 14) — it's consistent with NG3 (no dynamic routing), not a new gap you're introducing. |
| D9 | "Invalid config is rejected at startup" doesn't say what counts as invalid. | See Section 15's concrete validation rule list. |
| D10 | No error-response body shape was ever specified for 401/403/429 — only status codes. | See Section 6.4 for the exact JSON envelope and error codes. |

---

## 2. Tech Stack

| Layer | Choice |
|---|---|
| Language | Go 1.22+. Run `go mod init streamguard` (or your chosen module path) and use it consistently for every internal import. |
| HTTP | `net/http` + `http.Flusher` for SSE. No framework needed. |
| Tokenization | Provider-specific libraries (`tiktoken-go` for OpenAI; Anthropic's published tokenizer/count-tokens endpoint as fallback). Must match what the provider actually bills on. |
| Concurrency | `sync.Mutex` / `sync/atomic` with CAS loops where check-and-update must be atomic; `context.Context` for cancellation. Prefer mutex/CAS over channels where simpler. |
| Testing | standard `testing`, `-race`, `httptest`. The full suite runs against the mock upstream harness (Section 9) — never against the real OpenAI/Anthropic network by default. |
| Config | `config.yaml` + environment variable overrides for secrets. |
| Storage | In-memory only for ledger, breaker state, API key store. No persistence in v1 — documented limitation, not a bug. |

---

## 3. Repository Layout

```
streamguard/
├── cmd/
│   └── streamguard/        # main.go, wiring, config load
├── internal/
│   ├── cascade/             # cascade controller, provider priority, breaker pre-check on every attempt
│   ├── breaker/              # per-provider circuit breaker, config-driven, atomic half-open probe claim
│   ├── parser/                 # frame reassembly + failure classification; feeds calibration.Sample("inter_token_gap", ...)
│   ├── ratelimit/                # sliding-window limiter — admission-only, see D3
│   ├── budget/                     # APIKeyRecord, API Key Store, TryReserve, Budget Resetter, pre/mid-stream enforcement
│   ├── ledger/                       # usage ledger + /usage handler + ownership check + cross-period aggregation (D6)
│   ├── reconcile/                      # batch reconciliation, idempotent on (api_key_hash, billing_period); feeds calibration.Sample("drift", ...)
│   ├── tokenizer/                        # Tokenizer Registry — pinned versions, consecutive-drift tracking, tokenizer_drift_suspected
│   ├── calibration/                        # exposes Sample(kind string, value float64); accumulates samples; sanity-bound checks (Section 10)
│   ├── auth/                                  # API key validation, operator token check, log redaction
│   ├── protocol/                                # GatewayEvent types, SSE encoding
│   └── mockupstream/                              # mock OpenAI/Anthropic streaming + usage-reporting server (Section 9)
├── chaos/
│   └── harness.go            # //go:build chaos_enabled — excluded from default build
├── client-ref/
│   └── main.go               # minimal reference client — CLI + ANSI, see D7. Not a web app.
├── config.yaml
├── keys.yaml                  # example API key store seed file
└── README.md
```

---

## 4. Wire Protocol — Non-Negotiable Contract

Four SSE event types, interleaved with normal upstream content chunks. Implement every field exactly as
named below, as real Go structs — never a bare `map[string]interface{}`.

### `gateway_status` — emitted once, at stream start, before any content chunk. Never re-emitted after failover.
```json
event: gateway_status
data: {"state":"healthy","provider":"openai"}
```

### `gateway_failover` — emitted each time the cascade moves to the next *attempted* provider.
```json
event: gateway_failover
data: {
  "reason": "dead_socket",
  "tokens_delivered_before_failure": 142,
  "provider_from": "openai",
  "provider_to": "anthropic",
  "attempt": 1
}
```
- `reason` is a **closed enum**: `"dead_socket" | "silent_hang" | "malformed"`. `"upstream_timeout"` does not
  exist anywhere in this system. Never let it appear in code, tests, logs, or docs.
- `attempt` is 1-indexed.
- `tokens_delivered_before_failure` is scoped to **this one attempt only** — see Section 5.
- Never emitted for a provider skipped silently because its circuit was already `open` at request start.

### `gateway_regenerating` — emitted immediately after `gateway_failover`.
```json
event: gateway_regenerating
data: {"keep_partial_visible": true}
```

### `gateway_truncated` — terminal. Connection closes after this. No further retries from the proxy.
```json
event: gateway_truncated
data: {"reason": "all_providers_exhausted", "tokens_delivered": 222, "final": true}
```
- `reason` is a closed enum with **exactly two** values: `"all_providers_exhausted"` | `"budget_exceeded"`.
- `final` is always `true`.
- `tokens_delivered` is the **cumulative** total across every attempt in this `StreamSession` — see Section 5.

**Field-naming discipline:** every field describes only what the proxy actually knows.
`tokens_delivered_before_failure`, not `tokens_lost` — the proxy never knows how many tokens *would* have
been generated.

---

## 5. Token & Billing Semantics (worked example — implement exactly this)

Three different counters exist and they are **not** the same number. Getting this wrong is the single most
likely silent bug in the whole project.

| Counter | Scope | Resets when? |
|---|---|---|
| `ProviderAttempt.TokensDeliveredBeforeFailure` | One attempt only | New attempt starts |
| `StreamSession.TokensDelivered` (→ `gateway_truncated.tokens_delivered`) | Cumulative across **every** attempt in the request, since the client genuinely received every byte over the wire (dimmed or not) | Never, within one request |
| `LedgerEntry.TokensBilled` | Cumulative across **billing periods**, incremented **only by the final attempt of each request** | Never resets except via the Budget Resetter (which is a separate field, `APIKeyRecord.TokensUsed`, not this one) |

**The rule:** any attempt that was *superseded by a successful failover to the next provider* contributes to
`StreamSession.TokensDelivered` (the client saw it) but **not** to `LedgerEntry.TokensBilled` (it isn't part
of the final answer). Only the request's **last** attempt — whichever one actually produced the terminal
outcome, whether that's success, budget truncation, or the final failed attempt that triggered
`all_providers_exhausted` — contributes to `TokensBilled`.

**Worked example 1 — failover then success:**
1. Stream starts on P1 (`openai`). P1 delivers 142 tokens, then `dead_socket`.
   - `gateway_failover` fires: `tokens_delivered_before_failure: 142`, `attempt: 1`.
   - `ProviderAttempt[0] = {Provider: "openai", Outcome: "dead_socket", TokensDeliveredBeforeFailure: 142}`
2. Cascade replays the full original request against P2 (`anthropic`). P2 delivers 80 tokens, then completes.
   - `ProviderAttempt[1] = {Provider: "anthropic", Outcome: "success", TokensDeliveredBeforeFailure: 0}` (this field is only meaningful for failed attempts; a successful attempt's delivered count lives in the session total, not here)
3. `StreamSession.TokensDelivered = 142 + 80 = 222` — this is everything the client's SSE connection ever received.
4. `LedgerEntry.TokensBilled += 80` — **only** P2's contribution. The 142 from P1 is permanently excluded from billing, even though the client visually retained and saw it as the dimmed block.

**Worked example 2 — failover then total exhaustion:**
1. Same as above through step 1: P1 delivers 142, fails, `gateway_failover` fires with `attempt: 1`.
2. Cascade replays against P2. P2 delivers 80 tokens, then also fails (`malformed`), and no providers remain.
   - `gateway_truncated` fires: `reason: "all_providers_exhausted"`, `tokens_delivered: 222` (cumulative — both attempts' bytes genuinely reached the client), `final: true`.
3. `LedgerEntry.TokensBilled += 80` — P2's contribution still counts, because P2 was the **final** attempt and was never itself superseded by a further failover (the cascade simply ran out of providers, it didn't choose to discard P2's output in favor of something better). P1's 142 is still excluded.
4. `LedgerEntry.TruncatedRequests += 1`.

**Worked example 3 — mid-stream budget exhaustion, no prior failover:**
1. Stream is on P1 only. After 300 delivered tokens, the next chunk's `TryReserve` fails.
2. That crossing chunk is withheld. `gateway_truncated` fires: `reason: "budget_exceeded"`, `tokens_delivered: 300`, `final: true`.
3. `LedgerEntry.TokensBilled += 300` (P1 was the only and therefore final attempt).

**Scope note:** only completion/output tokens streamed back to the client are metered against budget, rate
limit, and billing in v1. Prompt/input tokens are not separately reserved before the stream opens — the
pre-stream `429` check is literally `TokensUsed >= TokenBudget` (already at zero), not a check of whether the
upcoming request would fit. This matches PRD FR-8's wording exactly and avoids needing to estimate completion
length before generation starts.

### 5.4 `billing_period` computation and `GET /usage/{key}` aggregation (D6)

```go
func billingPeriod(t time.Time, interval time.Duration) string {
    periodStart := t.UTC().Truncate(interval) // aligns cleanly for any interval that evenly divides 24h
    return periodStart.Format(time.RFC3339) + "/" + interval.String()
}
```
Use the timestamp of the **terminal event** (success or truncation), not the request's start time, to decide
which period bucket a `LedgerEntry` write belongs to.

`GET /usage/{key}` is **not** scoped to the current period. It aggregates across every `LedgerEntry` row for
that `api_key_hash`:
- `tokens_billed` = sum of `TokensBilled` across all periods.
- `truncated_requests` = sum of `TruncatedRequests` across all periods.
- `drift_flag` = `true` if **any** period for this key currently has `DriftFlag == true` (logical OR).
- `last_reconciled_at` = the max non-null `LastReconciledAt` across all periods (`null` if none yet).

---

## 6. HTTP API Surface

### 6.1 `POST /v1/stream` — client-facing streaming endpoint
- Header: `Authorization: Bearer <api_key>`. Body: `{"model", "messages", "stream": true}`.
- Pre-stream rejection (plain HTTP, stream never opens): missing/invalid key → `401`; provider not in
  allowlist → `403`; budget already at zero **or** sliding-window rate limit already saturated → `429`
  (see D3 — these are the only two `429` causes; rate limiting never causes a mid-stream event).
- Mid-stream budget exhaustion is **not** a `429`. It's `gateway_truncated`/`budget_exceeded`.
- Malformed JSON body, or a body missing `model`/`messages` → `400 Bad Request` before auth is even checked.
- Response: `text/event-stream`, content chunks pass through, StreamGuard events interleave.
- No cross-provider model translation (D8): the `model` field is forwarded unmodified to whatever provider
  is currently being attempted.

### 6.2 `GET /usage/{key}` — ledger read
- Requires `Authorization: Bearer <api_key>`; header key must equal path key. Missing header, invalid key, or
  mismatch → `403` in all three cases.
- Add an explicit test: a **valid** client API key used as the Bearer token against `/healthz` must be
  rejected — client keys and the operator token are separate credential spaces, never interchangeable.
- Success response (aggregated per Section 5.4):
```json
{"api_key":"sg_live_***","tokens_billed":184213,"truncated_requests":3,"last_reconciled_at":"2026-06-19T03:00:00Z","drift_flag":false}
```

### 6.3 `GET /healthz` — operator endpoint
- Requires `Authorization: Bearer <operator_token>` from `OPERATOR_TOKEN`. Client API keys are never valid here.
- Returns proxy liveness + per-provider circuit breaker state.
- An optional unauthenticated liveness-only endpoint (for a load balancer) must never include breaker detail.

### 6.4 Error response body contract (D10)

Every pre-stream rejection and every `/usage`/`/healthz` auth failure returns this envelope:
```json
{"error": "<short_code>", "message": "<human-readable detail>"}
```

| Endpoint | Status | `short_code` |
|---|---|---|
| `POST /v1/stream` | 400 | `invalid_request_body` |
| `POST /v1/stream` | 401 | `invalid_api_key` |
| `POST /v1/stream` | 403 | `provider_not_allowed` |
| `POST /v1/stream` | 429 | `budget_exhausted` |
| `POST /v1/stream` | 429 | `rate_limited` |
| `GET /usage/{key}` | 403 | `unauthorized` |
| `GET /healthz` | 403 | `unauthorized` |
| `GET /healthz` | 404 (wrong method) | `not_found` |

Never log a raw API key or the raw operator token, even inside an error message — log only `KeyHash` or a
redacted form (`sg_live_***`).

---

## 7. Cascade / Failover Algorithm (FR-1) + Failure Detection (FR-4)

**Cascade rules:**
- Check the relevant circuit breaker before **every** attempt, including the first.
- `open` → skip silently, do not contact upstream, do not emit `gateway_failover`.
- `half_open` → exactly one concurrent request becomes the probe (see Section 8/D4); all others treat the
  circuit as `open` for this attempt.
- `closed` → proceed normally.
- On failure: discard the partial upstream response, replay the **entire original request body**,
  unmodified, against the next provider.
- Single-provider exhaustion, or every provider already `open` at request start → `gateway_truncated`,
  `reason: "all_providers_exhausted"`. If exhausted before any provider was attempted, `tokens_delivered` is
  `0` and **no** `gateway_failover` is emitted.

**Three failure detectors:**

| Mode | Detection | Notes |
|---|---|---|
| `dead_socket` | Read returns EOF / transport error | Immediate. No buffering or timeout needed. |
| `silent_hang` | Read timeout on inter-token gap | Deadline = calibrated multiple (default 5×) of measured P99, sanity-bounded — see Section 10. |
| `malformed` | Schema validation on a **fully reassembled** SSE frame | Buffer partial TCP reads first. Classify `malformed` only after a complete frame fails schema validation. |

**Required test:** a unit test proving a frame legitimately split across two or more TCP reads does not
trigger a false-positive `malformed` classification. Hard gate — Section 12.

---

## 8. Circuit Breaker (one per provider)

| State | Definition | Transition |
|---|---|---|
| `closed` | Normal. Requests pass through. | → `open` when `consecutive_failures` hits `failure_threshold` (default 3) |
| `open` | Known-bad. Requests skipped. | → `half_open` after `open_timeout_s` since last state change (default 30s) |
| `half_open` | Exactly one probe allowed. | → `closed` on probe success (default 1 required) → `open` on probe failure |

All three parameters live under top-level `circuit_breaker` in `config.yaml`, overridable per provider.

**Atomic probe claim (D4) — implement exactly this:**
```go
type ProviderCircuitState struct {
    mu                  sync.RWMutex
    state               CircuitBreakerState
    consecutiveFailures int
    lastStateChange     time.Time
    probing             bool // exclusive claim flag, only meaningful while state == HalfOpen
}

// TryClaimProbe returns true if the caller is the exclusive probe for this
// half-open window. Must be a single atomic critical section — never a
// separate read-then-write.
func (s *ProviderCircuitState) TryClaimProbe() bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.state != HalfOpen || s.probing {
        return false
    }
    s.probing = true
    return true
}
```
Every cascade attempt that observes `half_open` calls `TryClaimProbe()` under the **write** lock (not a read
lock — admission decisions during `half_open` are a state mutation, not a pure read, precisely because only
one caller may win). A caller that fails to claim treats the provider exactly as if it were `open` for this
attempt. Write a concurrency test: fire N goroutines at a breaker sitting in `half_open` simultaneously and
assert exactly one claims the probe.

---

## 9. Mock Upstream Harness (`internal/mockupstream`) — build this before anything depends on it

This is new infrastructure not explicitly named in the source specs, required by D1/D5. The entire test
suite — Phases 1 through 4 — runs against this, never against real provider APIs by default.

**Streaming endpoint** (one mock instance per simulated provider, so multi-provider failover scenarios are
testable):
- Serves SSE responses shaped like real OpenAI/Anthropic chunk formats, closely enough that the real parser
  code exercises real frame-reassembly logic — not a single pre-built blob written in one `Write` call.
- **Configurable, randomized per-token delay**, default drawn from a distribution centered around 50–150ms
  per chunk (not a fixed constant — use real jitter). This is mandatory, not cosmetic: a loopback-speed mock
  produces a near-zero P99 and therefore a degenerate `silent_hang_deadline_ms` (see D5, Section 10).
- **Deterministic failure injection**, settable per test case: force `dead_socket` (abrupt connection close
  mid-stream), `silent_hang` (stop writing without closing — just stall past the configured deadline), or
  `malformed` (write a syntactically broken SSE frame) at a configurable point in the stream.
- **Randomized failure injection mode** for the chaos harness and load test: inject one of the three failure
  modes into a configurable percentage of streams.
- Must also support deliberately writing a frame split across multiple `Write`/`Flush` calls, so the
  split-frame negative test (Section 7) exercises real reassembly rather than a single complete read.

**Usage-reporting endpoint** (mirrors what the reconciliation job queries):
- Returns a token count for a given period.
- **Configurable, non-zero offset** from the true locally-counted value (e.g. a random ±0–8% deviation),
  settable per test. A mock that always agrees with the local count produces a 0% drift baseline, which
  silently breaks `drift_threshold_pct` calibration (D5).
- Also supports a "no drift" mode and a "always-flagged" mode for the reconciliation lifecycle tests
  (set/clear `drift_flag`).

**Real-provider wiring:** the real `tiktoken-go`/Anthropic client code still exists behind `Provider.BaseURL`,
but is only exercised under an explicit opt-in (e.g. `go test -tags integration_live`) that requires
`OPENAI_API_KEY`/`ANTHROPIC_API_KEY` to be present. `go test ./...` and `go test -race ./...` must pass with
zero network access and zero API keys configured.

---

## 10. Calibration — concrete gates, not vibes

Two values must be derived from real measured data, never invented: `timeouts.silent_hang_deadline_ms` and
`reconciliation.drift_threshold_pct`. "Real measured" here means real samples from the mock harness running
at volume (Section 9's realism requirements make this meaningful rather than degenerate), not invented
numbers.

**Concrete sample-count gates (replaces "sufficient volume"):**
- At least **1,000** `inter_token_gap` samples accumulated before computing P99.
- At least **100** `drift` samples accumulated before computing the drift baseline.
- If a single load-test/chaos-harness pass doesn't produce this volume, **loop it** (run it repeatedly) until
  the threshold is met. This is a hard gate at the start of Phase 4 (Section 12).

**Sanity bounds on the computed outputs** — if the raw calculation falls outside these bands, do not use it
silently; clamp it and document why in the README:
- `silent_hang_deadline_ms`: clamp to **[1000, 15000]**. Default multiplier is 5× measured P99; if that
  produces a value outside the band, either pick a different defensible multiplier or clamp, and log the raw
  P99, the multiplier, and the final clamped value together so the choice is auditable.
- `drift_threshold_pct`: clamp to **[1.0, 25.0]**. Default is the measured P95 of observed drift samples.

Both the unclamped raw statistic and the final configured value go in the README, side by side, so a reviewer
can see whether clamping happened and why.

---

## 11. Reconciliation (FR-6) & Usage Ledger (FR-7) & Graceful Shutdown (FR-9)

**Reconciliation:** offline, batch, scheduled (default interval `1h`, configurable). Idempotent per
`(api_key_hash, billing_period)` — re-running a window must not double-count tokens or duplicate flags.
`drift_pct = abs(local_tokens - provider_reported_tokens) / provider_reported_tokens * 100`, guarding against
division by zero (if `provider_reported_tokens == 0`, treat drift as `0` for that window and log a warning —
don't crash the job). `drift_flag` is set when `drift_pct` exceeds the threshold, and **must be clearable**: a
later pass over the same window that finds drift back within threshold clears the flag. Every run
unconditionally pushes its drift value to the Calibration Logger (raw data collection) and independently
compares against the configured threshold for the flag decision.

**Usage ledger:** in-memory, single `sync.Mutex`. Consumes events from successful completion,
`gateway_truncated` (either reason), and reconciliation drift updates, per the Section 5 billing rule.
`GET /usage/{key}` aggregates per Section 5.4.

**Graceful shutdown:** on `SIGTERM`, stop accepting new connections immediately. In-flight streams —
including streams currently sitting in failover state, waiting on the next provider — count as in-flight and
get the full `shutdown.drain_timeout_s` window, timed from the moment `SIGTERM` was received. If the window
expires first, force-close, log the forced termination, and record the partial token count in the ledger
(per Section 5's billing rule — the in-progress attempt at the moment of force-close is the "final" attempt).

---

## 12. Concurrency Requirements (NFR-1, NFR-2)

- One goroutine per client stream, `context.Context` derived from the incoming request, canceled on disconnect.
- Breaker state: `sync.RWMutex` per provider; half-open probe claim is a **write**-lock-guarded CAS, not a read (Section 8).
- Rate limiter: `sync.Map` keyed by API key hash, atomic increment; admission-only per D3.
- Budget: all spending goes through `TryReserve`'s CAS loop — never separate read/compare/increment steps.
- Budget resetter: independent background goroutine, ticker no coarser than every 1 minute; only resets the
  counter `TryReserve` reads; never touches hot-path reservation logic directly.
- Usage ledger: single mutex is sufficient at this write frequency.
- Reconciliation: idempotent upsert under the ledger's mutex.

**Required correctness proof:**
- `go test -race` across `breaker`, `ratelimit`, `ledger`, and `budget` — zero data races.
- Concurrent `TryReserve` stress test near a key's budget boundary — successful reservations ×
  tokens-per-reservation never exceeds `TokenBudget`.
- Concurrent `TryClaimProbe` stress test (Section 8) — exactly one winner under N concurrent callers.
- A load test firing 50+ concurrent simulated streams (against the mock harness, randomized failure
  injection) — ledger's final token total matches the sum of individually tracked expected outcomes, zero
  panics, zero races.

---

## 13. Build Order (execute in this sequence)

Treat these as sequential phases within one continuous build, not literal calendar weeks.

### Phase 0 — Bootstrap
- Full package layout (Section 3), including `internal/mockupstream`.
- `go mod init`; config loading (`config.yaml` + env overrides for `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `OPERATOR_TOKEN`).
- Config validation per Section 15.
- Startup loading of `auth.keys_file`.
- CI commands: `go build ./...`, `go test ./...`, `go test -race ./...`.
- `chaos/harness.go` carries `//go:build chaos_enabled`. Verify exclusion: `go build -o /tmp/sg ./cmd/streamguard && ! go tool nm /tmp/sg | grep -qi 'streamguard/chaos'`.
- **Exit:** `go build ./...` succeeds; invalid config (per Section 15's list) is rejected at startup with a clear error; default build excludes chaos.

### Phase 1 — Mock harness + core proxy path + live accounting
- Build `internal/mockupstream` (Section 9) — this phase's other work depends on it.
- `POST /v1/stream` base path with SSE pass-through, tested against the mock harness.
- Cascade controller skeleton: single-provider exhaustion, full-request replay, silent skip on open circuit at request start.
- Auth: key validation, allowlist enforcement, pre-stream rejection codes with the Section 6.4 error envelope.
- Provider-specific token counting + `TryReserve` wired into the streaming path.
- Sliding-window rate limiter (admission-only, D3), `rate_limit.window_s` default 60s.
- Start calibration logging now — inter-token-gap samples from every chunk. (Drift samples naturally begin later, once reconciliation exists in Phase 4 — that's expected, not a gap.)
- Redacted structured logging; protocol metadata and credentials stay redacted per Section 6.4.
- **Exit:** happy-path streaming works end-to-end against the mock harness; open-circuit-at-start tested; live counting + budget reservation active; calibration logging running; 400/401/403/429 paths return the exact Section 6.4 envelope.

### Phase 2 — Wire protocol, failure detection, reference client
- All four SSE events exactly per Section 4, closed enums, 1-indexed `attempt`.
- Frame-reassembly parser (buffer, validate only after full-frame reassembly). **Add the split-frame negative test**, using the mock harness's split-write mode.
- Three failure detectors against the mock harness's deterministic injection mode. Silent-hang uses a placeholder timeout marked `// TODO: replace with calibrated value in Phase 4`.
- Extend cascade to full multi-provider failover with replay; verify the Section 5 billing worked examples with integration tests asserting the exact numbers.
- Build the reference client (Section 16) — CLI, ANSI, against the mock harness.
- **Exit:** single-failover success and full exhaustion both work end-to-end through the reference client; split-frame negative test passes; the Section 5 worked-example assertions pass; no invalid reason values anywhere; only remaining placeholder is the silent-hang timeout.

### Phase 3 — Breaker, ledger, budget truncation, chaos, concurrency
- Finish the breaker state machine including the atomic half-open probe claim (Section 8) and its concurrency test.
- Usage ledger keyed by `(api_key_hash, billing_period)` (Section 5.4 algorithm); authenticated `GET /usage/{key}` with ownership + the client-key-rejected-by-/healthz negative test.
- Mid-stream budget exhaustion per Section 5's worked example 3.
- `GET /healthz` with operator-token auth.
- Chaos harness: build-tag gate **and** `STREAMGUARD_CHAOS_ENABLED=true` runtime gate, both independent. Exercise all three failure modes via the mock harness's randomized injection mode. **Run it at enough volume/repetition to also start meeting Section 10's sample-count gates** — this run's output feeds Phase 4 calibration.
- `go test -race`; `TryReserve` boundary stress test; 50+ concurrent stream load test against the mock harness.
- **Exit:** NFR-1/NFR-2 pass; usage endpoint auth/ownership tests pass (including the cross-credential negative test); budget-exceeded truncation has an integration test matching worked example 3 exactly; breaker transitions exercised via chaos; default build still excludes chaos.

### Phase 4 — Calibration, reconciliation, shutdown, README, final verification
- Confirm Section 10's sample-count gates are met (loop the load test/chaos harness further if not).
- Compute real P99 from accumulated samples, apply the multiplier, sanity-bound per Section 10, replace the Phase 2 placeholder. Document raw P99, multiplier, clamping decision (if any), final value.
- Implement the reconciliation job and `drift_pct` formula per Section 11. Run it against the mock usage-reporting endpoint enough times to clear the 100-sample drift gate. Compute the P95 baseline, sanity-bound, derive `drift_threshold_pct`. Document the same way.
- `drift_flag` set/clear/idempotency tests; `tokenizer_drift_suspected` escalation logging.
- Graceful shutdown per Section 11, tested for both clean drain and forced close.
- **Write a documented demo procedure** (e.g. a `make demo` target or an explicit command sequence in the README) that runs the reference client against the mock harness with `STREAMGUARD_CHAOS_ENABLED=true`, to reliably and reproducibly demonstrate a mid-stream provider switch for the acceptance criteria — real provider behavior can't be relied on to fail on cue.
- README: measured detection times per failure mode; measured P99 + multiplier + clamping + deadline; measured drift distribution + threshold; load/race-test results; Out of Scope (Section 17); Known Limitations (Section 18, including every D-numbered decision from Section 1).
- Run the full acceptance checklist (Section 19) top to bottom.
- **Exit:** no placeholder calibration values remain; sample-count gates were met with real (mock-harness-generated) data, not invented numbers; reconciliation set/clear/idempotency verified; graceful shutdown tested both ways; demo procedure documented and runnable; README reflects measured reality.

---

## 14. Hard Gates

| Gate | Checked at | Required result |
|---|---|---|
| Split-frame negative test | End of Phase 2 | Reassembled split frames never produce false `malformed` |
| Section 5 billing worked examples | End of Phase 2 | All three worked-example integration tests pass with the exact numbers shown |
| Half-open probe exclusivity | End of Phase 3 | N concurrent callers against a `half_open` breaker → exactly one claims the probe |
| Chaos harness build isolation | End of Phase 0, again end of Phase 3 | Default build excludes chaos code entirely |
| Race detector + concurrent load test | End of Phase 3 | Zero races, zero panics, ledger totals match expected outcomes |
| Calibration sample-count gates | Start of Phase 4 | ≥1,000 `inter_token_gap` samples, ≥100 `drift` samples, all from real (not invented) mock-harness runs |
| Calibration sanity bounds | End of Phase 4 | `silent_hang_deadline_ms` in [1000, 15000]; `drift_threshold_pct` in [1.0, 25.0]; clamping documented if it occurred |
| Acceptance checklist | End of Phase 4 | Every item in Section 19 explicitly verified |

If a gate fails, fix the prerequisite. Never paper over it with manual testing or placeholder numbers.

---

## 15. Config Validation Rules (resolves D9 — "invalid config rejected at startup" means exactly this)

Reject startup with a clear error message if any of the following hold:
- Two providers share the same `name`.
- Two providers share the same `priority`.
- Any `priority` is negative.
- Any `base_url` fails to parse as a valid URL.
- `circuit_breaker.failure_threshold` < 1, `open_timeout_s` < 1, or `half_open_success_threshold` < 1 (top-level or any per-provider override).
- `rate_limit.window_s` < 1.
- `reconciliation.interval` parses to ≤ 0.
- `shutdown.drain_timeout_s` < 0.
- `auth.keys_file` does not exist or fails to parse.
- Any key entry in `keys.yaml` has an empty `provider_allowlist` or a negative `token_budget`.

---

## 16. Reference Client (FR-3)

A single Go CLI binary (`client-ref/main.go`, D7) — not a web app. Its job is to prove the wire protocol.

**Required elements:** prompt input + send; a provider display; a streaming output region able to show the
retained partial block and the regenerated block simultaneously; a terminal notice area for truncated
outcomes.

**State-by-state behavior:**

| Trigger | Required behavior |
|---|---|
| `gateway_status` | Print provider; mark status active/healthy |
| Content chunk | Append text at full brightness |
| `gateway_failover` | Update provider display from `provider_to` immediately — never wait for another `gateway_status` |
| `gateway_regenerating` | Keep the rendered partial text visible, apply ANSI dim/faint, indicate regeneration is underway |
| New content after failover | Print into a **separate block below** the dimmed partial — never appended into it |
| Successful completion after failover | Both blocks return to full brightness together |
| `gateway_truncated` / `all_providers_exhausted` | Stop; terminal incomplete-response notice; include delivered-token count |
| `gateway_truncated` / `budget_exceeded` | Stop; **distinct** terminal budget-exhausted notice; include delivered-token count |

**Non-negotiable:** dim, never strikethrough (strikethrough means deletion, the wrong meaning here).
`gateway_truncated` is terminal — no silent auto-retry. ANSI color conveys state but every cue also has
explicit text (never color-only), and `--no-color` must remain legible. The truncated notice must name
`budget_exceeded` vs `all_providers_exhausted` explicitly, not just "truncated."

---

## 17. Out of Scope (do not build these)

| Not building | Reason |
|---|---|
| Shared circuit-breaker state across proxy replicas | Single-instance only in v1. |
| Output-quality benchmarking across recovery strategies | Separate project. |
| Dynamic cost-/latency-based provider routing | Priority order is static config. |
| Enterprise auth (SSO, RBAC, multi-tenant) | API-key budget + allowlist only. |
| A second dashboard surface | Operator visibility is `/usage/{key}`, `/healthz`, logs, chaos/test output. |
| Persistent storage for ledger / API key store | Restart loses state — documented limitation. |
| API key hot-reload | Keys load once at startup. |
| Automatic tokenizer-drift remediation | Detected and logged only. |
| Cross-provider model-name translation (D8) | Request body forwarded unmodified; operator's responsibility to configure compatible providers. |
| Web/Electron reference client (D7) | CLI only. |
| Responsive/mobile design, markdown rendering, attachments, multi-conversation history, client-side retry after truncation | Out of scope per the UI/UX brief. |

---

## 18. Known Limitations (document in the README)

- Single-instance only — breaker state and ledger are in-process; a restart loses accumulated state.
- Reconciliation is batch and offline; not a real-time billing guarantee.
- Output quality across cascade attempts is not benchmarked.
- Mid-stream provider swap is visible to the user.
- Provider tokenizer drift may cause local/provider counts to diverge until the tokenizer library is updated.
- **Calibration was bootstrapped from mock-harness/chaos-harness traffic volume** (Section 10), not weeks of
  production traffic. Recalibrate against real production traffic after deployment.
- **Rate limiting is admission-only (D3)**: it prevents new requests from starting once a key's sliding
  window is saturated, but cannot terminate a stream already in progress — only budget exhaustion can.
- **No cross-provider model-name translation (D8)**: a failover that crosses providers forwards the original
  `model` field unchanged; misconfigured providers can fail in a way the proxy doesn't specifically diagnose.

---

## 19. Acceptance Checklist

- [ ] All three failure modes detected correctly, including the split-frame negative test.
- [ ] Wire protocol implemented end-to-end with correct event types, reason enums, 1-indexed `attempt`; `gateway_regenerating` UX demonstrated via the reference client's documented demo procedure.
- [ ] Reference client updates its provider display from `gateway_failover.provider_to`, demonstrated in a mid-stream provider-switch scenario.
- [ ] Section 5's three worked-example billing scenarios pass as integration tests with the exact numbers shown.
- [ ] `timeouts.silent_hang_deadline_ms` documented with measured P99, multiplier, and any sanity-bound clamping.
- [ ] `reconciliation.drift_threshold_pct` documented with measured baseline distribution and any clamping.
- [ ] `go test -race` passes on all shared-state packages; the 50+ concurrent load test passes; results recorded in the README.
- [ ] Half-open probe exclusivity verified under concurrent load (exactly one winner).
- [ ] `GET /usage/{key}` returns a correct, cross-period-aggregated total, tested via both a `gateway_truncated` event and a successful completion.
- [ ] `GET /usage/{key}` returns `403` for a key/path mismatch; `/healthz` rejects a valid client API key.
- [ ] `gateway_truncated`/`budget_exceeded` verified via an integration test exhausting a key's budget mid-stream.
- [ ] All four circuit-breaker transition paths verified via the chaos harness.
- [ ] `drift_flag` set-and-clear lifecycle verified.
- [ ] Graceful shutdown verified for both clean drain and forced close.
- [ ] Chaos harness confirmed absent from a default build via the symbol-table check.
- [ ] Every 401/403/429/400 path returns the exact Section 6.4 error envelope.
- [ ] README includes Out of Scope (Section 17) and Known Limitations (Section 18, with all D-decisions called out).

---

## 20. Configuration Reference

```yaml
# config.yaml
circuit_breaker:
  failure_threshold: 3
  open_timeout_s: 30
  half_open_success_threshold: 1

providers:
  - name: openai
    priority: 0
    base_url: https://api.openai.com
  - name: anthropic
    priority: 1
    base_url: https://api.anthropic.com
    circuit_breaker:
      failure_threshold: 5

timeouts:
  silent_hang_deadline_ms: 4500   # placeholder until Phase 4 — replace with measured, sanity-bounded value

reconciliation:
  interval: 1h
  drift_threshold_pct: 4.2        # placeholder until Phase 4 — replace with measured, sanity-bounded value

rate_limit:
  window_s: 60

budget:
  default_period: 24h

auth:
  keys_file: ./keys.yaml

shutdown:
  drain_timeout_s: 30
```

Secrets (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `OPERATOR_TOKEN`) come from environment variables only —
never committed to `config.yaml`.

---

Build it in phase order. Keep the wire protocol byte-exact. Never fabricate a calibration number — generate
real samples from the mock harness instead. Treat Section 1's ten decisions as law. Verify every item in
Section 19 before calling it done.
