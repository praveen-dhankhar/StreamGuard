# StreamGuard — UI/UX Brief

**Companion to:** `streamguard-prd.md`, `streamguard-trd.md`, `streamguard-app-flow.md`, `streamguard-backend-schema.md`
**Status:** Aligned to PRD v2.0 and TRD
**Last updated:** 19 June 2026

StreamGuard is primarily a backend systems project. The only required UI surface in the source specs is the minimal reference client used to prove the wire protocol and failover UX end to end.

---

## 1. Scope

| Surface | Required? | Purpose |
|---|---|---|
| Reference client | Yes | Demonstrate `gateway_status`, `gateway_failover`, `gateway_regenerating`, and `gateway_truncated` exactly as the PRD defines them |
| Separate demo dashboard | No | Not required by the PRD or TRD; keep out of v1 scope |
| Product-grade chat application | No | StreamGuard is not shipping a consumer chat product |

Design goal: make failure handling visible, explicit, and unambiguous. Visual polish is secondary to protocol clarity.

---

## 2. Required Surface — Reference Client

The reference client is a single-screen, minimal chat-style interface. Its job is not to look like a full assistant product; its job is to prove the failover contract.

### Layout

```text
┌──────────────────────────────────────────────────────┐
│ StreamGuard Reference Client                         │
├──────────────────────────────────────────────────────┤
│ Prompt                                               │
│ [ user input..................................... ]  │
│ [ Send ]                                             │
├──────────────────────────────────────────────────────┤
│ Provider: openai                                     │
│ Status: streaming / regenerating / truncated / done  │
├──────────────────────────────────────────────────────┤
│ Partial response block                               │
│ (dims on gateway_regenerating, never hard-clears)    │
│                                                      │
│ Regenerated response block                           │
│ (appears below the partial block after failover)     │
├──────────────────────────────────────────────────────┤
│ Terminal notice area                                 │
│ (used only for gateway_truncated)                    │
└──────────────────────────────────────────────────────┘
```

### Required elements

- A prompt input and send action.
- A provider display showing the currently active provider.
- A streaming response area that can show both the retained partial block and the regenerated block at the same time.
- A terminal notice area for truncated outcomes.

No navigation, no multi-thread chat history, no avatars, no markdown renderer, and no product-branding work are required.

---

## 3. State-by-State UI Behavior

| Trigger | Required UI response |
|---|---|
| Before submit | Empty response area; provider and status can be blank or idle |
| `gateway_status` | Show the provider from `gateway_status.provider`; mark status as active/healthy |
| Content chunk | Append text normally at full opacity |
| `gateway_failover` | Immediately update the provider display from `gateway_failover.provider_to`; do not wait for or expect another `gateway_status` |
| `gateway_regenerating` | Keep the already rendered partial text visible, dim it, and show that regeneration is underway |
| New content after failover | Stream new content into a separate block below the dimmed partial block |
| Successful completion after failover | Return both the dimmed partial block and regenerated block to full opacity together |
| `gateway_truncated` with `reason: "all_providers_exhausted"` | Stop streaming, show a terminal incomplete-response notice, include delivered-token count if available |
| `gateway_truncated` with `reason: "budget_exceeded"` | Stop streaming, show a distinct budget-exhausted terminal notice, include delivered-token count if available |

The client must visibly distinguish three outcomes:

- Normal completion
- Regeneration after failover
- Terminal truncation

If those states look the same, the reference client has failed its purpose.

---

## 4. Core UX Rules

These are direct consequences of the PRD and TRD and are not optional stylistic choices:

- Partial text is retained on failover. It is dimmed, not removed.
- Dimmed text must not use strikethrough. Strikethrough signals deletion; the correct meaning here is "previous partial output retained for context."
- New content after failover appears below the retained partial block, not appended into it.
- The provider label updates from `gateway_failover.provider_to`, because `gateway_status` is emitted only once at stream start.
- `gateway_truncated` is terminal. The client may explain that the response is incomplete, but it must not silently auto-retry.
- Plain streamed text is sufficient. Rich formatting is not required to prove the protocol.

---

## 5. Visual Language

Use a utilitarian developer-tool aesthetic. The UI should read like an instrument panel, not a marketing surface.

| Element | Treatment |
|---|---|
| Provider label | Plain text plus a small state badge |
| Healthy/active state | Clear positive state cue plus text label |
| Regenerating state | Amber or equivalent transition color plus explicit status text |
| Truncated state | Red or equivalent error color plus explicit terminal wording |
| Response text | High legibility, left-aligned, plain text |
| Data values | Monospace is appropriate for provider names, token counts, and protocol-related labels |

Color can support meaning, but meaning must still be readable without color.

---

## 6. Accessibility and Clarity Requirements

- State is never conveyed by color alone; every state cue has accompanying text.
- The truncated banner must name the condition explicitly, especially for `budget_exceeded` versus `all_providers_exhausted`.
- The retained partial block and regenerated block must be visually distinguishable even for users who cannot perceive color differences.
- Motion, if any, should be minimal and functional. A subtle loading indicator during regeneration is acceptable; decorative animation is not needed.

---

## 7. Out of Scope

The following are intentionally excluded to stay aligned to the source specs:

- A second dashboard surface for operators or interview demos
- Charts, circuit-breaker panels, or chaos-harness controls in the UI
- Mobile-first or responsive-product design work
- Multi-conversation history, persistence, or user accounts
- Markdown rendering, rich text, attachments, or code blocks
- Retry orchestration in the client after `gateway_truncated`

Operator visibility in v1 comes from the authenticated backend surfaces and logs already defined in the TRD: `GET /usage/{key}`, `GET /healthz`, structured logs, and the chaos/test harness outputs.
