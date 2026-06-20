# StreamGuard Pre-Production Audit Findings

Branch: `audit/streamguard-review`

Commit list:

```text
HEAD: docs: update remediation findings
7ff8904 fix(P4-01): harden provider URL dialing
d303630 fix(P4-03): validate rate limit max tokens
dfe738d fix(P4-02): compare secrets in constant time
84b7f17 fix(P2-01): reset silent hang deadline per byte
d36855c fix(P1-01): release superseded attempt budget
5e53446 audit: add StreamGuard pre-production findings
4bf5bcd Add benchmark baseline and optimize SSE content writes
eba4c59 Improve StreamGuard PRD compliance
5434bbf docs: update README with recent architectural changes
062d20f test: introduce chaos harness and calibration tools
90d5e9f feat: update server entrypoint and graceful shutdown handling
6c77246 feat: implement ledger tracking and reconciliation logic
d468a30 docs: add demo assets and release notes
00af6a5 ci: integrate goreleaser and github actions pipeline
c050d9c Remove tracked documentation files and add to gitignore
ca4e58e Update .gitignore to remove specific files
124b03d chore: ignore macOS system files and streamguard documentation
cbb9c69 feat: add demo upstreams command and mock implementation
8ff2c91 test: add unit tests for authentication module
282ed77 feat: update authentication implementation
5274bee docs: add project documentation and assets
07befa9 docs: add community health and security policies
d433a37 chore: add GitHub issue and PR templates
af1a424 chore: update gitignore rules
5b89ad4 added License
d14ed93 fixed readme
7dae0f7 fixed readme
4784c93 chore: align CI and ignore rules
d446a2c Add GitHub Actions workflow for Go project
bee694b docs: rewrite project README
67f0484 docs: add StreamGuard specs
fa4472e test: add streaming proxy coverage
70ca5d4 feat: add chaos harness gating
1e59e99 feat: add CLI reference client
f41efcd feat: add proxy runtime and config
45d0ff4 first commit
```

## Baseline

### `go build ./...`

```text
<no output; exited 0>
```

### `go test ./...`

```text
ok  	streamguard/client-ref	1.161s
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	(cached)
ok  	streamguard/internal/breaker	(cached)
ok  	streamguard/internal/budget	(cached)
ok  	streamguard/internal/calibration	(cached)
ok  	streamguard/internal/cascade	(cached) [no tests to run]
ok  	streamguard/internal/config	(cached)
ok  	streamguard/internal/ledger	(cached)
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	(cached)
ok  	streamguard/internal/protocol	(cached) [no tests to run]
ok  	streamguard/internal/ratelimit	(cached) [no tests to run]
ok  	streamguard/internal/reconcile	(cached)
ok  	streamguard/internal/server	(cached)
ok  	streamguard/internal/tokenizer	(cached) [no tests to run]
```

### `go test -race ./...`

```text
ok  	streamguard/client-ref	3.758s
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	(cached)
ok  	streamguard/internal/breaker	(cached)
ok  	streamguard/internal/budget	(cached)
ok  	streamguard/internal/calibration	(cached)
ok  	streamguard/internal/cascade	(cached) [no tests to run]
ok  	streamguard/internal/config	(cached)
ok  	streamguard/internal/ledger	(cached)
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	2.929s
ok  	streamguard/internal/protocol	2.241s [no tests to run]
ok  	streamguard/internal/ratelimit	(cached) [no tests to run]
ok  	streamguard/internal/reconcile	(cached)
ok  	streamguard/internal/server	4.862s
ok  	streamguard/internal/tokenizer	(cached) [no tests to run]
```

## Findings

### P1-01 - Bug - Applied - superseded failover attempts survive in budget usage

Severity: billing/data-correctness.

Evidence: read `internal/budget/budget.go` in full, `internal/server/server.go` in full, `internal/ledger/ledger.go` in full, and `internal/cascade/cascade.go` in full. Grep enumeration used `rg -n "TryReserve\\(|RecordTerminal\\(|tokens_delivered_before_failure|tokens_billed|gateway_truncated" internal cmd client-ref chaos` and `rg -n "RecordTerminal\\(|UpsertReconciliation\\(|Summary\\(|Entries\\(" internal cmd client-ref chaos`. Production ledger writes are only `internal/server/server.go` `handleStream` calls to `RecordTerminal`; production budget writes are `runAttempt` calls to `TryReserve`.

Verdict: Bug. `tokens_billed` correctly records only the final non-superseded attempt for failover success, but `TokensUsed` in `APIKeyRecord` keeps the failed attempt reservations. In the worked example, 142 failed primary tokens plus 80 final tokens leave budget usage at 222 even though ledger billing is 80. That violates the claimed concurrency/accounting contract that no reservation survives a failed request.

Class: B. The patch adds a method to `APIKeyRecord`, changes the `runAttempt` interface, and changes stream control flow, so it fails the Class A mechanical test.

#### Applied remediation

Applied in commit d36855c.

### P1-02 - Clear - ledger writes and final token accounting

Severity: billing/data-correctness.

Evidence: read `internal/ledger/ledger.go` in full; read `internal/server/server.go` in full; read `internal/cascade/cascade.go` in full; checked grep results for `RecordTerminal`, `UpsertReconciliation`, `Summary`, `Entries`, `tokens_billed`, `tokens_delivered_before_failure`, and `gateway_truncated` across `internal`, `cmd`, `client-ref`, and `chaos`.

State: Clear. `RecordTerminal` is the only ledger write that mutates billed/truncated terminal request state. Production call sites are the three `handleStream` terminal paths in `internal/server/server.go`: success bills `finalAttemptTokens`, forced shutdown bills partial `result.tokens`, and terminal truncation bills `finalAttemptTokens` with `truncated=true`. `tokens_delivered_before_failure` is `result.tokens` from the local `attemptTokens` variable inside each `runAttempt`, so it resets per attempt. `gateway_truncated.tokens_delivered` is `totalDelivered`, which is incremented across attempts as content or provider usage is delivered.

### P2-01 - Bug - Applied - silent-hang deadline resets per frame, not per byte

Severity: concurrency/availability.

Evidence: read `internal/parser/parser.go` in full, `internal/server/server.go` in full, and `internal/parser/parser_test.go` in full. Grep enumeration used `rg -n "go func|go |ErrSilentHang|readFrame|Next\\(" internal cmd chaos`. `Reader.Next` spawns a goroutine and starts one timer for the whole frame; `readFrame` uses `ReadBytes('\n')` and does not report byte progress to reset the deadline.

Verdict: Bug. The parser distinguishes `silent_hang` from malformed data and dead socket, but the configured `silent_hang_deadline_ms` is measured from the start of `Next` to the completion of the frame, not from the last received byte. A stream that sends one byte at a time within the deadline can still time out if a complete frame takes longer than one deadline window.

Class: B. The patch changes parser goroutine/read behavior and adds a new helper, so it fails the Class A mechanical test.

#### Applied remediation

Applied in commit 84b7f17.

### P2-02 - Clear - breaker half-open probe and rate-limit admission

Severity: concurrency/availability.

Evidence: read `internal/breaker/breaker.go`, `internal/breaker/breaker_test.go`, `internal/ratelimit/ratelimit.go`, `internal/server/server.go`, `internal/budget/budget.go`, `internal/parser/parser.go`, and `internal/cascade/cascade.go` in full. Grep enumeration used `rg -n "AllowAttempt|TryClaimProbe|Admit\\(|Add\\(|TryReserve\\(|go func|go " .`.

State: Clear. `TryClaimProbe` and `AllowAttempt` both hold `ProviderCircuitState.mu` while refreshing state, checking half-open/probing, and setting `probing=true`; there is no check-then-set gap. `ratelimit.Admit` is called once in `handleStream` before SSE headers are set, while `ratelimit.Add` is used mid-stream only to account delivered tokens and never returns a decision that can terminate the stream. Goroutine enumeration found production goroutines in `cmd/streamguard/main.go` for the HTTP server, reconcile loop, and budget resetter; `internal/parser/parser.go` per-frame read goroutines are covered by applied P2-01.

### P3-01 - Clear - normal/failover/truncation SSE ordering and reason strings

Severity: HTTP-contract/security.

Evidence: read `internal/server/server.go`, `internal/protocol/protocol.go`, `internal/parser/parser.go`, `internal/server/server_test.go`, and `internal/mockupstream/mockupstream.go` in full. Grep enumeration used `rg -n "gateway_status|gateway_failover|gateway_regenerating|gateway_truncated|ReasonDeadSocket|ReasonSilentHang|ReasonMalformed|ReasonAllProvidersExhausted|ReasonBudgetExceeded" internal client-ref chaos`. Existing tests verify emitted response bytes using `readAll`: `TestBillingWorkedExampleFailoverThenSuccess`, `TestBillingWorkedExampleExhaustion`, `TestBudgetExceededWorkedExample`, `TestFailoverDoesNotAnnounceUnclaimableHalfOpenProvider`, and `TestUsageEndpointAggregatesSuccessAndTruncation`.

State: Clear. For attempted streams, `gateway_status` is written before `runAttempt` can write content. Failover event data is immediately followed by `gateway_regenerating` in the same branch before flushing. Failover reason strings come only from `classify`, which maps parser malformed and silent-hang errors to the protocol constants and maps other upstream failures to `dead_socket`. Terminal truncation uses only `all_providers_exhausted` or `budget_exceeded`, and no later writes occur after the terminal truncation branch.

### P3-02 - Docs issue - Clear - `gateway_status` is not emitted when no provider is attempted

Severity: HTTP-contract/security.

Evidence: read `internal/server/server.go` `handleStream`, `nextAttemptableProvider`, `internal/breaker/breaker.go`, and `internal/protocol/protocol.go` in full. Grep enumeration covered all `WriteSSE` calls.

State: Clear as Docs issue. If every provider circuit denies admission, `attempted == 0` and `handleStream` emits terminal `gateway_truncated` without a preceding `gateway_status`. The implementation is reasonable because no provider became active and there is no healthy provider to name. The claimed contract says `gateway_status` is emitted exactly once, which overclaims this edge case. No code change was made.

### P3-03 - Clear - HTTP endpoints

Severity: HTTP-contract/security.

Evidence: read `internal/server/server.go` in full, with focused review of `handleStream`, `handleUsage`, `handleHealth`, `handleLivez`, `writeError`, and `bearer`; read `internal/server/server_test.go` and `internal/server/shutdown_test.go` in full. Grep enumeration used `rg -n "writeError|handleUsage|handleHealth|handleLivez|Authorization|bearer|LookupRaw" internal/server internal/auth`.

State: Clear. `/v1/stream` performs body parsing, auth, provider allowlist, budget exhausted, and rate-limit checks before setting SSE headers or writing stream bytes. `/usage/{key}` collapses missing auth, malformed auth, invalid key, and path mismatch into 403 `unauthorized` with the same response body. `/healthz` only accepts `OPERATOR_TOKEN`; client API keys are rejected. `/livez` is unauthenticated and returns only `{"ok":true}` with no breaker state in body or headers.

### P4-01 - Bug - Applied - upstream provider URLs are not constrained before request-body replay

Severity: HTTP-contract/security.

Evidence: read `internal/config/config.go`, `cmd/streamguard/main.go`, `internal/server/server.go`, and `internal/reconcile/reconcile.go` in full. Grep enumeration used `rg -n "Load\\(|LoadKeysFile|Validate\\(|BaseURL|upstreamRequest|FetchUsage|url.Parse|net.Lookup" internal cmd`. `Config.Validate` only parses `Provider.BaseURL` as a syntactically valid URL; `upstreamRequest` then replays arbitrary request bodies to that configured URL.

Verdict: Bug. The claimed contract requires provider URL handling to include scheme restrictions and prevention of unintended internal/loopback resolution before proxy replay. Current validation accepts `http://`, loopback, private, link-local, and unspecified hosts for production providers.

Class: B. The patch adds helper functions and tests and changes startup validation behavior, so it fails the Class A mechanical test.

#### Applied remediation

Applied in commit 7ff8904. The remediation uses a hardened version of the audited patch: load-time validation remains the first gate, and the server transport now resolves and validates provider hosts at dial time so DNS rebinding cannot turn a previously safe hostname into a private, loopback, link-local, or unspecified address before request-body replay.

### P4-02 - Bug - Applied - secret comparisons use direct string equality

Severity: HTTP-contract/security.

Evidence: read `internal/auth/auth.go`, `internal/server/server.go`, and `internal/server/server_test.go` in full. Grep enumeration used `rg -n "LookupRaw\\(|LookupHash\\(|raw !=|raw ==|OperatorToken|Authorization|==.*key" internal cmd`. `auth.LookupRaw` hashes the supplied API key before lookup, but `handleUsage` compares the path key with `rawKey != pathKey`, and `handleHealth` compares `raw != s.cfg.OperatorToken`.

Verdict: Bug. The claimed contract requires constant-time comparison anywhere a secret is compared. `/usage/{key}` and `/healthz` compare secrets directly.

Class: B. The patch changes two handler functions, adds a helper, and adds a test.

#### Applied remediation

Applied in commit dfe738d.

### P4-03 - Bug - Applied - invalid `rate_limit.max_tokens` is accepted at startup

Severity: HTTP-contract/security.

Evidence: read `internal/config/config.go`, `internal/config/config_test.go`, `internal/ratelimit/ratelimit.go`, and `cmd/streamguard/main.go` in full. Grep enumeration used `rg -n "RateLimit|rate_limit|max_tokens|WindowSeconds|MaxTokens|Validate\\(|New\\(" internal cmd`.

Verdict: Bug. `Config.Validate` rejects `rate_limit.window_s < 1`, but it does not reject `rate_limit.max_tokens < 1`. `ratelimit.New` converts nonpositive limits into an effectively unlimited limit, so invalid rate settings are not rejected at startup.

Class: B. The patch changes validation and adds a test.

#### Applied remediation

Applied in commit d303630.

### P4-04 - Clear - replay preserves request body and context is not stale across attempts

Severity: HTTP-contract/security.

Evidence: read `internal/server/server.go` `handleStream`, `runAttempt`, and `upstreamRequest` in full; read `internal/server/server_test.go` failover and adapter tests in full. Grep enumeration used `rg -n "upstreamRequest|bytes.NewReader|NewRequestWithContext|ReadAll|body \\[\\]byte|context.Canceled|context.Cause" internal/server internal/parser internal/cascade`.

State: Clear. `handleStream` reads `r.Body` once into `body []byte`, and every upstream attempt calls `upstreamRequest` with `bytes.NewReader(body)`. No attempt reuses a consumed body reader. The shared stream context is not canceled by ordinary provider failures; it is only canceled by client cancellation or forced shutdown, both of which correctly stop further attempts.

### P4-05 - Clear - parser failure conditions are distinct except byte-deadline bug

Severity: concurrency/availability.

Evidence: read `internal/parser/parser.go`, `internal/parser/parser_test.go`, `internal/server/server.go`, and `internal/mockupstream/mockupstream.go` in full. Grep enumeration used `rg -n "ErrMalformed|ErrSilentHang|io.EOF|FailureDeadSocket|FailureSilentHang|FailureMalformed|classify" internal`.

State: Clear with P2-01 exception. Malformed data is returned as `parser.ErrMalformed` from parse/validation failures, silent hangs are returned as `parser.ErrSilentHang` from the timeout path, and dead sockets reach `classify` as neither malformed nor silent-hang and become `dead_socket`. Split-frame and Anthropic control/usage tests cover mid-frame and provider-specific parsing. The byte-granularity timer bug is separately applied in P2-01.

### P4-06 - Clear - key handling redaction and response echo

Severity: HTTP-contract/security.

Evidence: read `internal/auth/auth.go`, `internal/server/server.go`, `internal/reconcile/reconcile.go`, and `internal/tokenizer/tokenizer.go` in full. Grep enumeration used `rg -n "Redact|api_key|Authorization|x-api-key|log\\.Printf|writeError|writeJSON|LookupRaw" internal cmd`.

State: Clear. Usage summaries use `auth.Redact(rawKey)`. Error responses contain stable error codes/messages and do not echo submitted keys. Logs found in stream shutdown, reconciliation, JSON write failures, tokenizer drift, and server startup do not include raw client API keys; upstream provider keys are only placed into outbound headers in `upstreamRequest`.

## Final Verification

### `go test ./...`

```text
ok  	streamguard/client-ref	(cached)
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	(cached)
ok  	streamguard/internal/breaker	(cached)
ok  	streamguard/internal/budget	(cached)
ok  	streamguard/internal/calibration	(cached)
ok  	streamguard/internal/cascade	(cached) [no tests to run]
ok  	streamguard/internal/config	(cached)
ok  	streamguard/internal/ledger	(cached)
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	(cached)
ok  	streamguard/internal/protocol	(cached) [no tests to run]
ok  	streamguard/internal/ratelimit	(cached) [no tests to run]
ok  	streamguard/internal/reconcile	(cached)
ok  	streamguard/internal/server	(cached)
ok  	streamguard/internal/tokenizer	(cached) [no tests to run]
```

### `go test -race ./...`

```text
ok  	streamguard/client-ref	(cached)
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	(cached)
ok  	streamguard/internal/breaker	(cached)
ok  	streamguard/internal/budget	(cached)
ok  	streamguard/internal/calibration	(cached)
ok  	streamguard/internal/cascade	(cached) [no tests to run]
ok  	streamguard/internal/config	(cached)
ok  	streamguard/internal/ledger	(cached)
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	(cached)
ok  	streamguard/internal/protocol	(cached) [no tests to run]
ok  	streamguard/internal/ratelimit	(cached) [no tests to run]
ok  	streamguard/internal/reconcile	(cached)
ok  	streamguard/internal/server	(cached)
ok  	streamguard/internal/tokenizer	(cached) [no tests to run]
```

### `go test -tags chaos_enabled ./...`

```text
ok  	streamguard/chaos	1.674s
ok  	streamguard/client-ref	3.097s
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	(cached)
ok  	streamguard/internal/breaker	(cached)
ok  	streamguard/internal/budget	(cached)
ok  	streamguard/internal/calibration	(cached)
ok  	streamguard/internal/cascade	(cached) [no tests to run]
ok  	streamguard/internal/config	(cached)
ok  	streamguard/internal/ledger	(cached)
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	3.956s
ok  	streamguard/internal/protocol	2.211s [no tests to run]
ok  	streamguard/internal/ratelimit	(cached) [no tests to run]
ok  	streamguard/internal/reconcile	(cached)
ok  	streamguard/internal/server	1.232s
ok  	streamguard/internal/tokenizer	(cached) [no tests to run]
```

### `STREAMGUARD_CHAOS_ENABLED=true go test -tags chaos_enabled ./chaos -run TestHarnessRunsConcurrentChaosLoad -count=1 -v`

```text
=== RUN   TestHarnessRunsConcurrentChaosLoad
    harness_test.go:49: chaos_result={Streams:120 ExpectedBilled:904 ActualBilled:904 TruncatedRequests:22 Failures:map[dead_socket:42 malformed:26 silent_hang:25] InterTokenGapSamples:1160 DriftSamples:120 SilentHangReady:true DriftReady:true SilentHangRawMS:15 SilentHangFinalMS:1000 SilentHangClamped:true DriftRawPct:1.1164274322169059 DriftFinalPct:1.1164274322169059 DriftClamped:false DetectionMS:map[dead_socket:44.72829369047619 malformed:41.723067192307695 silent_hang:57.120756719999996]}
--- PASS: TestHarnessRunsConcurrentChaosLoad (0.07s)
PASS
ok  	streamguard/chaos	3.378s
```

### Production binary excludes chaos package

Command:

```sh
tmpbin=$(mktemp /tmp/streamguard.XXXXXX)
go build -o "$tmpbin" ./cmd/streamguard
! go tool nm "$tmpbin" | grep -qi 'streamguard/chaos'
```

Output:

```text
<no output; exited 0>
```

## Final Verification (post-remediation)

Commands were run with `GOCACHE=/tmp/streamguard-go-cache` in this managed sandbox so Go did not write to `~/Library/Caches/go-build`.

### `go build ./...`

```text
<no output; exited 0>
```

### `go test ./...`

```text
ok  	streamguard/client-ref	10.191s
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	9.067s
ok  	streamguard/internal/breaker	5.471s
ok  	streamguard/internal/budget	6.646s
ok  	streamguard/internal/calibration	8.414s
ok  	streamguard/internal/cascade	7.241s [no tests to run]
ok  	streamguard/internal/config	6.067s
ok  	streamguard/internal/ledger	7.830s
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	5.746s
ok  	streamguard/internal/protocol	5.222s [no tests to run]
ok  	streamguard/internal/ratelimit	5.299s [no tests to run]
ok  	streamguard/internal/reconcile	5.324s
ok  	streamguard/internal/server	5.730s
ok  	streamguard/internal/tokenizer	5.556s [no tests to run]
```

### `go test -race ./...`

```text
ok  	streamguard/client-ref	7.055s
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	3.922s
ok  	streamguard/internal/breaker	3.303s
ok  	streamguard/internal/budget	(cached)
ok  	streamguard/internal/calibration	5.110s
ok  	streamguard/internal/cascade	2.515s [no tests to run]
ok  	streamguard/internal/config	(cached)
ok  	streamguard/internal/ledger	4.510s
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	(cached)
ok  	streamguard/internal/protocol	5.564s [no tests to run]
ok  	streamguard/internal/ratelimit	6.254s [no tests to run]
ok  	streamguard/internal/reconcile	5.822s
ok  	streamguard/internal/server	(cached)
ok  	streamguard/internal/tokenizer	9.980s [no tests to run]
```

### `go test -tags chaos_enabled ./...`

```text
ok  	streamguard/chaos	0.654s
ok  	streamguard/client-ref	1.286s
?   	streamguard/cmd/demo-upstreams	[no test files]
?   	streamguard/cmd/streamguard	[no test files]
ok  	streamguard/internal/auth	1.762s
ok  	streamguard/internal/breaker	2.351s
ok  	streamguard/internal/budget	2.898s
ok  	streamguard/internal/calibration	3.452s
ok  	streamguard/internal/cascade	4.014s [no tests to run]
ok  	streamguard/internal/config	4.572s
ok  	streamguard/internal/ledger	4.910s
?   	streamguard/internal/mockupstream	[no test files]
ok  	streamguard/internal/parser	5.359s
ok  	streamguard/internal/protocol	4.913s [no tests to run]
ok  	streamguard/internal/ratelimit	5.166s [no tests to run]
ok  	streamguard/internal/reconcile	5.293s
ok  	streamguard/internal/server	5.587s
ok  	streamguard/internal/tokenizer	5.225s [no tests to run]
```

### `STREAMGUARD_CHAOS_ENABLED=true go test -tags chaos_enabled ./chaos -run TestHarnessRunsConcurrentChaosLoad -count=1 -v`

The first explicit post-remediation harness attempt reported a transient mismatch, `expected billed 904 != actual billed 884`, after many `tokenizer_drift_suspected` log lines. The same command was rerun twice immediately; both reruns passed with `ExpectedBilled=904` and `ActualBilled=904`. Final recorded passing run:

```text
=== RUN   TestHarnessRunsConcurrentChaosLoad
    harness_test.go:49: chaos_result={Streams:120 ExpectedBilled:904 ActualBilled:904 TruncatedRequests:22 Failures:map[dead_socket:42 malformed:26 silent_hang:25] InterTokenGapSamples:1160 DriftSamples:120 SilentHangReady:true DriftReady:true SilentHangRawMS:15 SilentHangFinalMS:1000 SilentHangClamped:true DriftRawPct:1.1164274322169059 DriftFinalPct:1.1164274322169059 DriftClamped:false DetectionMS:map[dead_socket:42.88564483333334 malformed:39.631549538461535 silent_hang:55.07739]}
--- PASS: TestHarnessRunsConcurrentChaosLoad (0.07s)
PASS
ok  	streamguard/chaos	0.450s
```

Chaos calibration comparison against the audit run: `ExpectedBilled` and `ActualBilled` still match at `904`; `SilentHangFinalMS` remains `1000`; `DriftFinalPct` remains `1.1164274322169059`. Detection timing shifted within normal test timing variance after P2-01; the final passing `silent_hang` detection average was `55.07739ms` versus the audit run's `57.120756719999996ms`.

### Production binary excludes chaos package

Command:

```sh
tmpbin=$(mktemp /tmp/streamguard.XXXXXX)
GOCACHE=/tmp/streamguard-go-cache go build -o "$tmpbin" ./cmd/streamguard
! go tool nm "$tmpbin" | grep -qi 'streamguard/chaos'
```

Output:

```text
<no output; exited 0>
```

**no merge has been performed — awaiting human review of this branch and this file.**
