# StreamGuard — App Flow

**Companion to:** `streamguard-prd.md`, `streamguard-trd.md`
**Status:** Aligned to PRD v2.0 and TRD
**Last updated:** 19 June 2026

This document enumerates the concrete runtime paths StreamGuard is required to support. It follows the PRD and TRD exactly; it does not add new product behavior.

---

## 1. Request Lifecycle State Model

```mermaid
stateDiagram-v2
    [*] --> Authenticating
    Authenticating --> Rejected: 401 / 403 / 429
    Authenticating --> SelectingProvider: auth + allowlist + budget pass
    SelectingProvider --> Streaming: first attempted provider selected
    SelectingProvider --> Truncated: all providers skipped or exhausted
    Streaming --> Streaming: content chunk forwarded
    Streaming --> FailoverPending: dead_socket / silent_hang / malformed
    FailoverPending --> Streaming: next attempted provider succeeds
    FailoverPending --> Truncated: provider list exhausted
    Streaming --> Truncated: budget_exceeded
    Streaming --> Complete: upstream finishes normally
    Complete --> [*]
    Truncated --> [*]
    Rejected --> [*]
```

Client disconnect is handled as immediate request-context cancellation. It is a transport teardown, not a StreamGuard wire-protocol state: once the client is gone, there is no additional SSE event to emit.

---

## 2. Happy Path — Successful Stream on the First Attempted Provider

This is the baseline path: auth succeeds, the first attempted provider's circuit is not open, and the stream completes without failover or truncation.

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant AB as Auth + Budget
    participant B as Breaker (P1)
    participant P as Provider 1
    participant Cal as Calibration Logger
    participant L as Usage Ledger

    C->>G: POST /v1/stream
    G->>AB: validate key, allowlist, budget
    AB-->>G: ok
    G->>B: check circuit state
    B-->>G: closed
    G->>C: event: gateway_status (provider=P1)
    G->>P: forward request
    loop per chunk
        P-->>G: content chunk (n tokens)
        G->>Cal: record inter-token gap sample
        G->>AB: TryReserve(n)
        AB-->>G: reserved
        G-->>C: forward chunk
    end
    P-->>G: stream complete
    G->>L: record delivered tokens for successful completion
    G-xC: normal close
```

Required behavior on this path:

- `gateway_status` is emitted once, before any content chunks.
- No `gateway_failover`, `gateway_regenerating`, or `gateway_truncated` events are emitted.
- Live token counting and budget reservation happen before each chunk is forwarded.
- The calibration logger records inter-token-gap samples from process start; successful traffic is part of the baseline used later for silent-hang calibration.

---

## 3. Open Circuit at Request Start

The cascade controller checks circuit state before every attempt, including the first. If the highest-priority provider is already `open`, StreamGuard skips it silently.

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant AB as Auth + Budget
    participant B1 as Breaker (P1)
    participant B2 as Breaker (P2)
    participant P2 as Provider 2

    C->>G: POST /v1/stream
    G->>AB: validate key, allowlist, budget
    AB-->>G: ok
    G->>B1: check circuit state
    B1-->>G: open
    Note over G,C: P1 is skipped silently; no gateway_failover
    G->>B2: check circuit state
    B2-->>G: closed
    G->>C: event: gateway_status (provider=P2)
    G->>P2: forward request
    P2-->>G: stream chunks...
    P2-->>G: stream complete
    G-xC: normal close
```

Required behavior on this path:

- A provider skipped because its circuit was already `open` is never surfaced as a `gateway_failover`.
- The client sees `gateway_status.provider` for the first provider actually attempted on this request.
- If every configured provider is already `open`, the list is exhausted without any upstream call and the terminal path is `gateway_truncated` with `reason: "all_providers_exhausted"`.

---

## 4. Mid-Stream Failover That Eventually Succeeds

The current provider begins streaming, then fails with one of the three defined failure modes. StreamGuard emits the failover events, replays the full original request against the next provider, and completes successfully.

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant B1 as Breaker (P1)
    participant P1 as Provider 1
    participant B2 as Breaker (P2)
    participant P2 as Provider 2
    participant L as Usage Ledger

    C->>G: POST /v1/stream
    G->>B1: check circuit state
    B1-->>G: closed
    G->>C: event: gateway_status (provider=P1)
    G->>P1: forward request
    P1-->>G: chunks...
    P1--xG: dead_socket / silent_hang / malformed
    G->>B1: record failure
    G->>C: event: gateway_failover (provider_from=P1, provider_to=P2, attempt=1)
    G->>C: event: gateway_regenerating (keep_partial_visible=true)
    Note over C: Partial text stays visible but dimmed
    G->>B2: check circuit state
    B2-->>G: closed
    G->>P2: replay full original request
    P2-->>G: chunks...
    P2-->>G: stream complete
    G->>L: record only the tokens actually delivered in the final outcome
    G-xC: normal close
```

Required behavior on this path:

- `gateway_status` is not re-emitted after failover. The client updates its provider display from `gateway_failover.provider_to`.
- `gateway_failover.reason` is one of `dead_socket`, `silent_hang`, or `malformed` only. `upstream_timeout` is not a valid protocol value.
- `gateway_regenerating` is emitted immediately after `gateway_failover`.
- The client retains the partial block, dims it, streams new content below it, and returns both blocks to full opacity together on successful completion.
- The replayed request is the full original request body, unmodified. No partial text from the failed attempt is appended into the retry prompt.
- Tokens from the failed attempt still count toward live budget/rate enforcement, but they do not count toward `tokens_billed` in the usage ledger.

---

## 5. Provider List Exhausted

This is the terminal failure path for the cascade controller. It is reached either because attempted providers fail one after another, or because every provider is skipped at request start due to an open circuit.

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant P1 as Provider 1
    participant P2 as Provider 2
    participant L as Usage Ledger

    C->>G: POST /v1/stream
    G->>P1: forward request
    P1--xG: failure detected
    G->>C: event: gateway_failover (attempt=1)
    G->>C: event: gateway_regenerating
    G->>P2: replay full original request
    P2--xG: failure detected
    Note over G: no attempted providers remain
    G->>C: event: gateway_truncated (reason=all_providers_exhausted, final=true)
    G->>L: record partial delivery and truncated request
    G-xC: close connection
```

Required behavior on this path:

- `gateway_truncated.reason` is `all_providers_exhausted`.
- `gateway_truncated.final` is always `true`.
- After `gateway_truncated`, the proxy closes the stream and does not attempt further retries.
- The usage ledger records the partial token count that actually reached the client and increments `truncated_requests`.
- If exhaustion happened before any provider was attempted, `tokens_delivered` is `0` and no `gateway_failover` event is emitted.

---

## 6. Mid-Stream Budget Exhaustion

Budget exhaustion after the SSE stream is already open is handled through the wire protocol, not through an HTTP status code.

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant AB as Auth + Budget
    participant P as Provider
    participant L as Usage Ledger

    C->>G: POST /v1/stream
    G->>P: forward request
    loop until budget boundary
        P-->>G: content chunk (n tokens)
        G->>AB: TryReserve(n)
        alt reservation succeeds
            AB-->>G: reserved
            G-->>C: forward chunk
        else reservation would exceed budget
            AB-->>G: rejected
            Note over G,C: crossing chunk is withheld
            G->>C: event: gateway_truncated (reason=budget_exceeded, tokens_delivered=<count before chunk>, final=true)
            G->>L: record partial delivery and truncated request
            G-xC: close connection
        end
    end
```

Required behavior on this path:

- The chunk that would push the key over budget is not forwarded to the client.
- The terminal event is `gateway_truncated` with `reason: "budget_exceeded"`.
- There is no HTTP `429` here because the stream is already open.
- The usage ledger records the token count at the moment the stream is cut off.

---

## 7. Pre-Stream Rejection

These checks happen before the SSE stream opens. Because no stream exists yet, StreamGuard returns plain HTTP responses rather than gateway events.

```mermaid
flowchart TD
    A[Client POST /v1/stream] --> B{Valid API key?}
    B -- No --> R1[401 Unauthorized]
    B -- Yes --> C{Provider in allowlist?}
    C -- No --> R2[403 Forbidden]
    C -- Yes --> D{Budget already exhausted?}
    D -- Yes --> R3[429 Too Many Requests]
    D -- No --> E[Proceed to provider selection]
```

Required behavior on this path:

- Missing or invalid API key returns `401 Unauthorized`.
- Requested provider outside the key's allowlist returns `403 Forbidden`.
- A key whose budget is already exhausted before streaming begins returns `429 Too Many Requests`.
- No SSE stream is opened and no StreamGuard event is emitted.

---

## 8. Usage Ledger Read Flow — `GET /usage/{key}`

The usage endpoint is part of the product contract, not a debugging-only endpoint. Its auth rule is stricter than a typical "read any usage" admin surface: a key may read only its own data.

```mermaid
flowchart TD
    A[Client GET /usage/{key}] --> B{Authorization Bearer key present?}
    B -- No --> R1[403 Forbidden]
    B -- Yes --> C{Key valid?}
    C -- No --> R1
    C -- Yes --> D{Header key matches path key?}
    D -- No --> R1
    D -- Yes --> E[Return usage summary JSON]
```

On success, the response shape is:

```json
{
  "api_key": "sg_live_***",
  "tokens_billed": 184213,
  "truncated_requests": 3,
  "last_reconciled_at": "2026-06-19T03:00:00Z",
  "drift_flag": false
}
```

Required behavior on this path:

- Missing auth, invalid key, and key/path mismatch all return `403 Forbidden`.
- The endpoint returns usage data for the caller's key only.
- `tokens_billed` is based on delivered tokens only; failed-attempt tokens are excluded even though they counted toward live budget/rate enforcement.

---

## 9. Health Flow — `GET /healthz`

`GET /healthz` is an operator endpoint, not a client-key endpoint.

```mermaid
flowchart TD
    A[Operator GET /healthz] --> B{Authorization Bearer operator token present?}
    B -- No or invalid --> R1[403 or equivalent auth rejection]
    B -- Valid --> C[Return proxy liveness and per-provider circuit state]
```

Required behavior on this path:

- Authentication uses `Authorization: Bearer <operator_token>`, sourced from `OPERATOR_TOKEN`.
- Client API keys are not valid credentials for this endpoint.
- The response includes proxy liveness plus per-provider circuit breaker state.
- An unauthenticated liveness surface, if exposed separately for infrastructure, must not include circuit breaker detail.

---

## 10. Reconciliation Batch Flow

Reconciliation is explicitly offline and idempotent per `(api_key_hash, billing_period)`. It never blocks the hot request path.

```mermaid
flowchart TD
    A[Scheduled trigger - default every 1h] --> B[Load local ledger entries for one billing period]
    B --> C[Fetch provider-reported usage for the same period]
    C --> D[Compute drift value]
    D --> E[Record drift sample in Calibration Logger]
    E --> F{Drift exceeds calibrated threshold?}
    F -- Yes --> G[Set drift_flag for that api_key_hash and billing_period]
    F -- No --> H[Clear drift_flag for that same period if it was previously set]
    G --> I[Persist idempotent reconciliation result]
    H --> I
    I --> J[Update last_reconciled_at]
```

Required behavior on this path:

- The default schedule is hourly, but the interval is configurable.
- The threshold is derived from measured baseline drift, not guessed.
- A repeated reconciliation run for the same `(api_key_hash, billing_period)` does not double-count tokens or create duplicate flags.
- `drift_flag` can be set and later cleared for the same billing window if a later pass finds drift back within threshold.

---

## 11. Graceful Shutdown During Active Streams

On SIGTERM, StreamGuard stops accepting new work but lets in-flight streams drain up to `drain_timeout_s`.

```mermaid
flowchart TD
    A[SIGTERM received] --> B[Stop accepting new connections]
    B --> C{Any in-flight streams?}
    C -- No --> Z[Exit]
    C -- Yes --> D[Allow streams to continue until drain timeout]
    D --> E{Stream finishes before timeout?}
    E -- Yes --> Z
    E -- No --> F[Force-close remaining connections]
    F --> G[Log forced termination]
    G --> H[Record partial token count in usage ledger]
    H --> Z
```

Required behavior on this path:

- A stream sitting in failover state still counts as in-flight and remains inside the drain window.
- The drain timeout starts when SIGTERM is received, not when the request started.
- If the drain window expires while a failover stream is waiting on a new upstream provider, the proxy force-closes it, logs the forced termination, and records the partial token count in the ledger.

---

## 12. Summary Matrix

| Situation | Client-visible result | StreamGuard-specific events |
|---|---|---|
| First attempted provider succeeds | Normal streaming response | `gateway_status` once |
| Higher-priority provider open at request start | Stream starts on next attempted provider | `gateway_status` once; no `gateway_failover` for skipped provider |
| Mid-stream failover succeeds | Partial block dims, regenerated block streams below it, request completes | `gateway_status`, `gateway_failover`, `gateway_regenerating` |
| Attempted providers exhausted | Terminal incomplete response | `gateway_truncated` with `reason: "all_providers_exhausted"` |
| Budget exhausted mid-stream | Terminal incomplete response | `gateway_truncated` with `reason: "budget_exceeded"` |
| Auth or allowlist failure before stream | Plain HTTP rejection | None |
| `GET /usage/{key}` with missing/invalid/mismatched key | `403 Forbidden` | None |
| `GET /healthz` with invalid operator token | Auth rejection | None |
| Reconciliation run | No client-visible change unless usage is later read | None to the stream; ledger and drift state updated internally |
