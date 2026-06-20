# StreamGuard Pre-Production Audit Findings

Branch: `audit/streamguard-review`

Commit list:

```text
HEAD: audit: add StreamGuard pre-production findings
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

### P1-01 - Bug - Drafted - superseded failover attempts survive in budget usage

Severity: billing/data-correctness.

Evidence: read `internal/budget/budget.go` in full, `internal/server/server.go` in full, `internal/ledger/ledger.go` in full, and `internal/cascade/cascade.go` in full. Grep enumeration used `rg -n "TryReserve\\(|RecordTerminal\\(|tokens_delivered_before_failure|tokens_billed|gateway_truncated" internal cmd client-ref chaos` and `rg -n "RecordTerminal\\(|UpsertReconciliation\\(|Summary\\(|Entries\\(" internal cmd client-ref chaos`. Production ledger writes are only `internal/server/server.go` `handleStream` calls to `RecordTerminal`; production budget writes are `runAttempt` calls to `TryReserve`.

Verdict: Bug. `tokens_billed` correctly records only the final non-superseded attempt for failover success, but `TokensUsed` in `APIKeyRecord` keeps the failed attempt reservations. In the worked example, 142 failed primary tokens plus 80 final tokens leave budget usage at 222 even though ledger billing is 80. That violates the claimed concurrency/accounting contract that no reservation survives a failed request.

Class: B. The patch adds a method to `APIKeyRecord`, changes the `runAttempt` interface, and changes stream control flow, so it fails the Class A mechanical test.

#### Proposed patch (not applied)

```diff
diff --git a/internal/budget/budget.go b/internal/budget/budget.go
index 3c1483d..a069834 100644
--- a/internal/budget/budget.go
+++ b/internal/budget/budget.go
@@ -29,6 +29,22 @@ func (k *APIKeyRecord) TryReserve(n int64) bool {
 	}
 }
 
+func (k *APIKeyRecord) Release(n int64) {
+	if n <= 0 {
+		return
+	}
+	for {
+		used := atomic.LoadInt64(&k.TokensUsed)
+		next := used - n
+		if next < 0 {
+			next = 0
+		}
+		if atomic.CompareAndSwapInt64(&k.TokensUsed, used, next) {
+			return
+		}
+	}
+}
+
 func (k *APIKeyRecord) Exhausted() bool {
 	return atomic.LoadInt64(&k.TokensUsed) >= k.TokenBudget
 }
diff --git a/internal/server/server.go b/internal/server/server.go
index 3c7e48c..c866030 100644
--- a/internal/server/server.go
+++ b/internal/server/server.go
@@ -225,6 +225,7 @@ func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
 		active.session.FinishAttempt(attemptIndex, string(reason), result.tokens, time.Now())
 		if next, ok := nextAttemptableProvider(providers, i+1, s.breakers, claimedProbes); ok {
 			failovers++
+			key.Release(int64(result.tokens))
 			_ = protocol.WriteSSE(w, protocol.EventFailover, protocol.FailoverData{
 				Reason:                       reason,
 				TokensDeliveredBeforeFailure: result.tokens,
@@ -261,7 +262,10 @@ type attemptResult struct {
 var errBudgetExceeded = errors.New("budget_exceeded")
 var errForcedShutdown = errors.New("forced_shutdown")
 
-func (s *Server) runAttempt(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, body []byte, model string, p config.Provider, keyHash string, key interface{ TryReserve(int64) bool }, total *int) attemptResult {
+func (s *Server) runAttempt(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, body []byte, model string, p config.Provider, keyHash string, key interface {
+	TryReserve(int64) bool
+	Release(int64)
+}, total *int) attemptResult {
 	req, err := s.upstreamRequest(ctx, p, body, keyHash)
 	if err != nil {
 		return attemptResult{err: err}
diff --git a/internal/server/server_test.go b/internal/server/server_test.go
index 50aafa7..c95fb5e 100644
--- a/internal/server/server_test.go
+++ b/internal/server/server_test.go
@@ -7,6 +7,7 @@ import (
 	"net/http"
 	"net/http/httptest"
 	"strings"
+	"sync/atomic"
 	"testing"
 	"time"
 
@@ -36,6 +37,13 @@ func TestBillingWorkedExampleFailoverThenSuccess(t *testing.T) {
 	if sum.TokensBilled != 80 {
 		t.Fatalf("tokens_billed = %d, want 80", sum.TokensBilled)
 	}
+	rec, ok := srv.auth.LookupRaw(raw)
+	if !ok {
+		t.Fatal("missing key record")
+	}
+	if got := atomic.LoadInt64(&rec.TokensUsed); got != 80 {
+		t.Fatalf("tokens_used = %d, want 80 after superseded attempt release", got)
+	}
 }
 
 func TestBillingWorkedExampleExhaustion(t *testing.T) {
```

Test command and output with patch applied:

```text
$ gofmt -w internal/budget/budget.go internal/server/server.go internal/server/server_test.go && go test -race ./internal/budget ./internal/server
ok  	streamguard/internal/budget	1.821s
ok  	streamguard/internal/server	2.956s
```

Revert performed: `git diff -- internal/budget/budget.go internal/server/server.go internal/server/server_test.go > /tmp/P1-01.patch && git checkout -- internal/budget/budget.go internal/server/server.go internal/server/server_test.go`.

### P1-02 - Clear - ledger writes and final token accounting

Severity: billing/data-correctness.

Evidence: read `internal/ledger/ledger.go` in full; read `internal/server/server.go` in full; read `internal/cascade/cascade.go` in full; checked grep results for `RecordTerminal`, `UpsertReconciliation`, `Summary`, `Entries`, `tokens_billed`, `tokens_delivered_before_failure`, and `gateway_truncated` across `internal`, `cmd`, `client-ref`, and `chaos`.

State: Clear. `RecordTerminal` is the only ledger write that mutates billed/truncated terminal request state. Production call sites are the three `handleStream` terminal paths in `internal/server/server.go`: success bills `finalAttemptTokens`, forced shutdown bills partial `result.tokens`, and terminal truncation bills `finalAttemptTokens` with `truncated=true`. `tokens_delivered_before_failure` is `result.tokens` from the local `attemptTokens` variable inside each `runAttempt`, so it resets per attempt. `gateway_truncated.tokens_delivered` is `totalDelivered`, which is incremented across attempts as content or provider usage is delivered.

### P2-01 - Bug - Drafted - silent-hang deadline resets per frame, not per byte

Severity: concurrency/availability.

Evidence: read `internal/parser/parser.go` in full, `internal/server/server.go` in full, and `internal/parser/parser_test.go` in full. Grep enumeration used `rg -n "go func|go |ErrSilentHang|readFrame|Next\\(" internal cmd chaos`. `Reader.Next` spawns a goroutine and starts one timer for the whole frame; `readFrame` uses `ReadBytes('\n')` and does not report byte progress to reset the deadline.

Verdict: Bug. The parser distinguishes `silent_hang` from malformed data and dead socket, but the configured `silent_hang_deadline_ms` is measured from the start of `Next` to the completion of the frame, not from the last received byte. A stream that sends one byte at a time within the deadline can still time out if a complete frame takes longer than one deadline window.

Class: B. The patch changes parser goroutine/read behavior and adds a new helper, so it fails the Class A mechanical test.

#### Proposed patch (not applied)

```diff
diff --git a/internal/parser/parser.go b/internal/parser/parser.go
index cb66858..1191e36 100644
--- a/internal/parser/parser.go
+++ b/internal/parser/parser.go
@@ -50,36 +50,57 @@ func (r *Reader) Next(ctx context.Context, deadline time.Duration) (Frame, error
 		err   error
 	}
 	ch := make(chan result, 1)
+	activity := make(chan struct{}, 1)
 	go func() {
-		frame, err := r.readFrame()
+		frame, err := r.readFrameWithActivity(activity)
 		ch <- result{frame: frame, err: err}
 	}()
 
 	timer := time.NewTimer(deadline)
 	defer timer.Stop()
-	select {
-	case <-ctx.Done():
-		return Frame{}, ctx.Err()
-	case <-timer.C:
-		return Frame{}, ErrSilentHang
-	case res := <-ch:
-		if res.err == nil {
-			now := time.Now()
-			if !r.lastFrameAt.IsZero() && r.cal != nil {
-				r.cal.Sample("inter_token_gap", float64(now.Sub(r.lastFrameAt).Milliseconds()))
+	for {
+		select {
+		case <-ctx.Done():
+			return Frame{}, ctx.Err()
+		case <-timer.C:
+			return Frame{}, ErrSilentHang
+		case <-activity:
+			if !timer.Stop() {
+				select {
+				case <-timer.C:
+				default:
+				}
+			}
+			timer.Reset(deadline)
+		case res := <-ch:
+			if res.err == nil {
+				now := time.Now()
+				if !r.lastFrameAt.IsZero() && r.cal != nil {
+					r.cal.Sample("inter_token_gap", float64(now.Sub(r.lastFrameAt).Milliseconds()))
+				}
+				r.lastFrameAt = now
 			}
-			r.lastFrameAt = now
+			return res.frame, res.err
 		}
-		return res.frame, res.err
 	}
 }
 
 func (r *Reader) readFrame() (Frame, error) {
+	return r.readFrameWithActivity(nil)
+}
+
+func (r *Reader) readFrameWithActivity(activity chan<- struct{}) (Frame, error) {
 	var buf bytes.Buffer
 	for {
-		line, err := r.br.ReadBytes('\n')
-		if len(line) > 0 {
-			buf.Write(line)
+		b, err := r.br.ReadByte()
+		if err == nil {
+			buf.WriteByte(b)
+			if activity != nil {
+				select {
+				case activity <- struct{}{}:
+				default:
+				}
+			}
 			if bytes.HasSuffix(buf.Bytes(), []byte("\n\n")) || bytes.HasSuffix(buf.Bytes(), []byte("\r\n\r\n")) {
 				return ParseFrameForProvider(buf.Bytes(), r.format)
 			}
diff --git a/internal/parser/parser_test.go b/internal/parser/parser_test.go
index d1cc5d6..c827366 100644
--- a/internal/parser/parser_test.go
+++ b/internal/parser/parser_test.go
@@ -9,12 +9,16 @@ import (
 
 type chunkedReader struct {
 	chunks [][]byte
+	delay  time.Duration
 }
 
 func (r *chunkedReader) Read(p []byte) (int, error) {
 	if len(r.chunks) == 0 {
 		return 0, io.EOF
 	}
+	if r.delay > 0 {
+		time.Sleep(r.delay)
+	}
 	chunk := r.chunks[0]
 	r.chunks = r.chunks[1:]
 	copy(p, chunk)
@@ -70,3 +74,19 @@ func TestAnthropicControlAndUsageFrames(t *testing.T) {
 		t.Fatalf("event = %q, want done", done.Event)
 	}
 }
+
+func TestSilentHangDeadlineResetsOnEveryReceivedByte(t *testing.T) {
+	raw := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"héllo\"}}]}\n\n")
+	chunks := make([][]byte, 0, len(raw))
+	for _, b := range raw {
+		chunks = append(chunks, []byte{b})
+	}
+	r := &chunkedReader{chunks: chunks, delay: 10 * time.Millisecond}
+	frame, err := NewReader(r, nil).Next(context.Background(), 30*time.Millisecond)
+	if err != nil {
+		t.Fatalf("one-byte-at-a-time frame parsed with error: %v", err)
+	}
+	if frame.Text != "héllo" {
+		t.Fatalf("text = %q, want héllo", frame.Text)
+	}
+}
```

Test command and output with patch applied:

```text
$ gofmt -w internal/parser/parser.go internal/parser/parser_test.go && go test -race ./internal/parser
ok  	streamguard/internal/parser	2.478s
```

Revert performed: `git diff -- internal/parser/parser.go internal/parser/parser_test.go > /tmp/P2-01.patch && git checkout -- internal/parser/parser.go internal/parser/parser_test.go`.

### P2-02 - Clear - breaker half-open probe and rate-limit admission

Severity: concurrency/availability.

Evidence: read `internal/breaker/breaker.go`, `internal/breaker/breaker_test.go`, `internal/ratelimit/ratelimit.go`, `internal/server/server.go`, `internal/budget/budget.go`, `internal/parser/parser.go`, and `internal/cascade/cascade.go` in full. Grep enumeration used `rg -n "AllowAttempt|TryClaimProbe|Admit\\(|Add\\(|TryReserve\\(|go func|go " .`.

State: Clear. `TryClaimProbe` and `AllowAttempt` both hold `ProviderCircuitState.mu` while refreshing state, checking half-open/probing, and setting `probing=true`; there is no check-then-set gap. `ratelimit.Admit` is called once in `handleStream` before SSE headers are set, while `ratelimit.Add` is used mid-stream only to account delivered tokens and never returns a decision that can terminate the stream. Goroutine enumeration found production goroutines in `cmd/streamguard/main.go` for the HTTP server, reconcile loop, and budget resetter; `internal/parser/parser.go` per-frame read goroutines are covered by P2-01.

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

### P4-01 - Bug - Drafted - upstream provider URLs are not constrained before request-body replay

Severity: HTTP-contract/security.

Evidence: read `internal/config/config.go`, `cmd/streamguard/main.go`, `internal/server/server.go`, and `internal/reconcile/reconcile.go` in full. Grep enumeration used `rg -n "Load\\(|LoadKeysFile|Validate\\(|BaseURL|upstreamRequest|FetchUsage|url.Parse|net.Lookup" internal cmd`. `Config.Validate` only parses `Provider.BaseURL` as a syntactically valid URL; `upstreamRequest` then replays arbitrary request bodies to that configured URL.

Verdict: Bug. The claimed contract requires provider URL handling to include scheme restrictions and prevention of unintended internal/loopback resolution before proxy replay. Current validation accepts `http://`, loopback, private, link-local, and unspecified hosts for production providers.

Class: B. The patch adds helper functions and tests and changes startup validation behavior, so it fails the Class A mechanical test.

#### Proposed patch (not applied)

```diff
diff --git a/internal/config/config.go b/internal/config/config.go
index fb6a555..2f98c05 100644
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -4,6 +4,8 @@ import (
 	"bufio"
 	"errors"
 	"fmt"
+	"net"
+	"net/netip"
 	"net/url"
 	"os"
 	"sort"
@@ -152,9 +154,8 @@ func (c Config) Validate() error {
 			return fmt.Errorf("duplicate provider priority %d", p.Priority)
 		}
 		priorities[p.Priority] = true
-		u, err := url.Parse(p.BaseURL)
-		if err != nil || u.Scheme == "" || u.Host == "" {
-			return fmt.Errorf("provider %q base_url must be a valid URL", p.Name)
+		if err := validateProviderURL(p); err != nil {
+			return fmt.Errorf("provider %q base_url invalid: %w", p.Name, err)
 		}
 		if err := validateBreaker(c.BreakerConfigFor(p)); err != nil {
 			return fmt.Errorf("provider %q circuit breaker invalid: %w", p.Name, err)
@@ -194,6 +195,49 @@ func validateBreaker(cfg breaker.Config) error {
 	return nil
 }
 
+func validateProviderURL(p Provider) error {
+	u, err := url.Parse(p.BaseURL)
+	if err != nil || u.Scheme == "" || u.Host == "" {
+		return errors.New("must be a valid URL")
+	}
+	switch p.ProviderType() {
+	case "mock":
+		if u.Scheme != "http" && u.Scheme != "https" {
+			return errors.New("scheme must be http or https")
+		}
+		return nil
+	default:
+		if u.Scheme != "https" {
+			return errors.New("scheme must be https")
+		}
+	}
+	host := u.Hostname()
+	if host == "" {
+		return errors.New("host is required")
+	}
+	if ip, err := netip.ParseAddr(host); err == nil {
+		if isPrivateUpstreamIP(ip) {
+			return errors.New("host resolves to a private, loopback, link-local, or unspecified address")
+		}
+		return nil
+	}
+	addrs, err := net.LookupIP(host)
+	if err != nil {
+		return fmt.Errorf("host lookup failed: %w", err)
+	}
+	for _, addr := range addrs {
+		ip, ok := netip.AddrFromSlice(addr)
+		if !ok || isPrivateUpstreamIP(ip) {
+			return errors.New("host resolves to a private, loopback, link-local, or unspecified address")
+		}
+	}
+	return nil
+}
+
+func isPrivateUpstreamIP(ip netip.Addr) bool {
+	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
+}
+
 func parseFile(path string, cfg *Config) error {
 	f, err := os.Open(path)
 	if err != nil {
diff --git a/internal/config/config_test.go b/internal/config/config_test.go
index 79fe488..d15415a 100644
--- a/internal/config/config_test.go
+++ b/internal/config/config_test.go
@@ -98,3 +98,54 @@ auth:
 		t.Fatalf("expected invalid provider type error, got %v", err)
 	}
 }
+
+func TestLoadRejectsProductionProviderHTTPAndInternalHosts(t *testing.T) {
+	dir := t.TempDir()
+	keys := filepath.Join(dir, "keys.yaml")
+	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
+		t.Fatal(err)
+	}
+	cases := []struct {
+		name      string
+		provider  string
+		wantError string
+	}{
+		{
+			name: "http scheme",
+			provider: `providers:
+  - name: openai
+    type: openai
+    priority: 0
+    base_url: http://api.openai.com
+auth:
+  keys_file: KEYS
+`,
+			wantError: "scheme must be https",
+		},
+		{
+			name: "loopback ip",
+			provider: `providers:
+  - name: openai
+    type: openai
+    priority: 0
+    base_url: https://127.0.0.1:8080
+auth:
+  keys_file: KEYS
+`,
+			wantError: "private, loopback",
+		},
+	}
+	for _, tc := range cases {
+		t.Run(tc.name, func(t *testing.T) {
+			cfg := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+".yaml")
+			body := strings.ReplaceAll(tc.provider, "KEYS", keys)
+			if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
+				t.Fatal(err)
+			}
+			_, err := Load(cfg)
+			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
+				t.Fatalf("expected %q error, got %v", tc.wantError, err)
+			}
+		})
+	}
+}
```

Test command and output with patch applied:

```text
$ gofmt -w internal/config/config.go internal/config/config_test.go && go test ./internal/config
ok  	streamguard/internal/config	0.887s
```

Revert performed: `git diff -- internal/config/config.go internal/config/config_test.go > /tmp/P4-01.patch && git checkout -- internal/config/config.go internal/config/config_test.go`.

### P4-02 - Bug - Drafted - secret comparisons use direct string equality

Severity: HTTP-contract/security.

Evidence: read `internal/auth/auth.go`, `internal/server/server.go`, and `internal/server/server_test.go` in full. Grep enumeration used `rg -n "LookupRaw\\(|LookupHash\\(|raw !=|raw ==|OperatorToken|Authorization|==.*key" internal cmd`. `auth.LookupRaw` hashes the supplied API key before lookup, but `handleUsage` compares the path key with `rawKey != pathKey`, and `handleHealth` compares `raw != s.cfg.OperatorToken`.

Verdict: Bug. The claimed contract requires constant-time comparison anywhere a secret is compared. `/usage/{key}` and `/healthz` compare secrets directly.

Class: B. The patch changes two handler functions, adds a helper, and adds a test.

#### Proposed patch (not applied)

```diff
diff --git a/internal/server/server.go b/internal/server/server.go
index 3c7e48c..c84f7ef 100644
--- a/internal/server/server.go
+++ b/internal/server/server.go
@@ -3,6 +3,7 @@ package server
 import (
 	"bytes"
 	"context"
+	"crypto/subtle"
 	"encoding/json"
 	"errors"
 	"fmt"
@@ -403,7 +404,7 @@ func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
 	pathKey := strings.TrimPrefix(r.URL.Path, "/usage/")
 	rawKey, ok := bearer(r.Header.Get("Authorization"))
 	key, valid := s.auth.LookupRaw(rawKey)
-	if !ok || !valid || rawKey != pathKey {
+	if !ok || !valid || !constantTimeEqualString(rawKey, pathKey) {
 		writeError(w, http.StatusForbidden, "unauthorized", "usage requires matching API key")
 		return
 	}
@@ -416,7 +417,7 @@ func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
 		return
 	}
 	raw, ok := bearer(r.Header.Get("Authorization"))
-	if !ok || raw != s.cfg.OperatorToken {
+	if !ok || !constantTimeEqualString(raw, s.cfg.OperatorToken) {
 		writeError(w, http.StatusForbidden, "unauthorized", "operator token required")
 		return
 	}
@@ -451,6 +452,10 @@ func bearer(v string) (string, bool) {
 	return token, token != ""
 }
 
+func constantTimeEqualString(a, b string) bool {
+	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
+}
+
 func flush(f http.Flusher) {
 	if f != nil {
 		f.Flush()
diff --git a/internal/server/server_test.go b/internal/server/server_test.go
index 50aafa7..b61308f 100644
--- a/internal/server/server_test.go
+++ b/internal/server/server_test.go
@@ -287,6 +287,15 @@ func TestUsageEndpointAggregatesSuccessAndTruncation(t *testing.T) {
 	}
 }
 
+func TestConstantTimeEqualString(t *testing.T) {
+	if !constantTimeEqualString("sg_live_test", "sg_live_test") {
+		t.Fatal("matching values were rejected")
+	}
+	if constantTimeEqualString("sg_live_test", "sg_live_other") {
+		t.Fatal("different values were accepted")
+	}
+}
+
 func testServer(t *testing.T, p1URL, p2URL string, budget int64) (*Server, string) {
 	t.Helper()
 	raw := "sg_live_test"
```

Test command and output with patch applied:

```text
$ gofmt -w internal/server/server.go internal/server/server_test.go && go test ./internal/server
ok  	streamguard/internal/server	1.140s
```

Revert performed: `git diff -- internal/server/server.go internal/server/server_test.go > /tmp/P4-02.patch && git checkout -- internal/server/server.go internal/server/server_test.go`.

### P4-03 - Bug - Drafted - invalid `rate_limit.max_tokens` is accepted at startup

Severity: HTTP-contract/security.

Evidence: read `internal/config/config.go`, `internal/config/config_test.go`, `internal/ratelimit/ratelimit.go`, and `cmd/streamguard/main.go` in full. Grep enumeration used `rg -n "RateLimit|rate_limit|max_tokens|WindowSeconds|MaxTokens|Validate\\(|New\\(" internal cmd`.

Verdict: Bug. `Config.Validate` rejects `rate_limit.window_s < 1`, but it does not reject `rate_limit.max_tokens < 1`. `ratelimit.New` converts nonpositive limits into an effectively unlimited limit, so invalid rate settings are not rejected at startup.

Class: B. The patch changes validation and adds a test.

#### Proposed patch (not applied)

```diff
diff --git a/internal/config/config.go b/internal/config/config.go
index fb6a555..9227d51 100644
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -166,6 +166,9 @@ func (c Config) Validate() error {
 	if c.RateLimit.WindowSeconds < 1 {
 		return errors.New("rate_limit.window_s must be >= 1")
 	}
+	if c.RateLimit.MaxTokens < 1 {
+		return errors.New("rate_limit.max_tokens must be >= 1")
+	}
 	if c.Reconciliation.Interval <= 0 {
 		return errors.New("reconciliation.interval must be > 0")
 	}
diff --git a/internal/config/config_test.go b/internal/config/config_test.go
index 79fe488..d72238d 100644
--- a/internal/config/config_test.go
+++ b/internal/config/config_test.go
@@ -98,3 +98,29 @@ auth:
 		t.Fatalf("expected invalid provider type error, got %v", err)
 	}
 }
+
+func TestLoadRejectsInvalidRateLimitMaxTokens(t *testing.T) {
+	dir := t.TempDir()
+	keys := filepath.Join(dir, "keys.yaml")
+	if err := os.WriteFile(keys, []byte("keys:\n  - key: sg_live_test\n    provider_allowlist: [openai]\n    token_budget: 10\n"), 0600); err != nil {
+		t.Fatal(err)
+	}
+	cfg := filepath.Join(dir, "config.yaml")
+	body := strings.ReplaceAll(`providers:
+  - name: openai
+    priority: 0
+    base_url: http://127.0.0.1:9001
+rate_limit:
+  window_s: 60
+  max_tokens: 0
+auth:
+  keys_file: KEYS
+`, "KEYS", keys)
+	if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
+		t.Fatal(err)
+	}
+	_, err := Load(cfg)
+	if err == nil || !strings.Contains(err.Error(), "rate_limit.max_tokens") {
+		t.Fatalf("expected invalid max_tokens error, got %v", err)
+	}
+}
```

Test command and output with patch applied:

```text
$ gofmt -w internal/config/config.go internal/config/config_test.go && go test ./internal/config
ok  	streamguard/internal/config	1.193s
```

Revert performed: `git diff -- internal/config/config.go internal/config/config_test.go > /tmp/P4-03.patch && git checkout -- internal/config/config.go internal/config/config_test.go`.

### P4-04 - Clear - replay preserves request body and context is not stale across attempts

Severity: HTTP-contract/security.

Evidence: read `internal/server/server.go` `handleStream`, `runAttempt`, and `upstreamRequest` in full; read `internal/server/server_test.go` failover and adapter tests in full. Grep enumeration used `rg -n "upstreamRequest|bytes.NewReader|NewRequestWithContext|ReadAll|body \\[\\]byte|context.Canceled|context.Cause" internal/server internal/parser internal/cascade`.

State: Clear. `handleStream` reads `r.Body` once into `body []byte`, and every upstream attempt calls `upstreamRequest` with `bytes.NewReader(body)`. No attempt reuses a consumed body reader. The shared stream context is not canceled by ordinary provider failures; it is only canceled by client cancellation or forced shutdown, both of which correctly stop further attempts.

### P4-05 - Clear - parser failure conditions are distinct except byte-deadline bug

Severity: concurrency/availability.

Evidence: read `internal/parser/parser.go`, `internal/parser/parser_test.go`, `internal/server/server.go`, and `internal/mockupstream/mockupstream.go` in full. Grep enumeration used `rg -n "ErrMalformed|ErrSilentHang|io.EOF|FailureDeadSocket|FailureSilentHang|FailureMalformed|classify" internal`.

State: Clear with P2-01 exception. Malformed data is returned as `parser.ErrMalformed` from parse/validation failures, silent hangs are returned as `parser.ErrSilentHang` from the timeout path, and dead sockets reach `classify` as neither malformed nor silent-hang and become `dead_socket`. Split-frame and Anthropic control/usage tests cover mid-frame and provider-specific parsing. The byte-granularity timer bug is separately Drafted in P2-01.

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

**no merge has been performed — awaiting human review of this branch and this file.**
