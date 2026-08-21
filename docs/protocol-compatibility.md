# Protocol compatibility

Agent-connector is a bidirectional converter between **ACP** (editor/CLI ↔ coding
agent, stdio JSON-RPC) and **A2A** (HTTP JSON-RPC + SSE) with a unified internal
task lifecycle (UAIC).

## Flows

| Client | Direction | Transport |
|---|---|---|
| ACP | ACP agent ↔ runtime | stdio JSON-RPC |
| A2A | runtime ↔ remote agent | HTTP JSON-RPC + SSE |
| UAIC | runtime ↔ agent (any) | NDJSON (stdio) / HTTP+SSE |
| ANP | runtime ↔ ANP peer | HTTPS + HTTP Message Signatures |

## Semantic mapping

- `protocol-a2a-mapper` translates A2A Task/Message/Artifact → `CoreCommand`,
  and `CoreEvent` → A2A Task/Stream events.
- `protocol-acp-mapper` translates ACP session prompt/update → `CoreCommand`,
  and `CoreEvent` → ACP session updates.
- Drivers (`driver-stdio`, `driver-http-sse`) speak UAIC/1 and return
  only normalized `DriverEvent`.

## UAIC/1

Unified contract runtime ↔ agent: one JSON object per line (stdio) or
`POST`/SSE frames (HTTP). SDK pin and details in
`design/universal-agent-adapter-module-specifications.md` (§2 UAIC).

## ANP transport

Real ANP transport is feature-gated behind `anp` in `crates/anp-transport`.

### SDK pin

```toml
anp-sdk = { package = "anp", git = "https://github.com/agent-network-protocol/AgentConnect", rev = "aaca169c3e5b051e48875b023b60364a1dd93022", default-features = false, features = ["jwt-pem", "network"] }
```

- **Revision:** `aaca169c3e5b051e48875b023b60364a1dd93022` (exact 40-char SHA)
- **Features enabled:** `jwt-pem`, `network`
- **Features excluded:** `mls` (group E2EE out of scope per security policy §4)
- **Default build:** `anp` feature is NOT enabled — zero new dependencies pulled in

### Dependency tree (depth 2)

```text
anp-transport v0.7.2
├── anp v0.9.4 (rev aaca169c)
│   ├── base64 v0.22.1
│   ├── bs58 v0.5.1
│   ├── chrono v0.4.45
│   ├── ed25519-dalek v2.1.1
│   ├── jsonwebtoken v9.3.1
│   ├── k256 v0.13.4
│   ├── num-bigint v0.4.8
│   ├── p256 v0.13.2
│   ├── pkcs8 v0.10.2
│   ├── rand v0.8.7
│   ├── regex v1.13.1
│   ├── reqwest v0.12.28
│   ├── ring v0.17.14
│   ├── serde v1.0.229
│   ├── serde_json v1.0.151
│   ├── serde_json_canonicalizer v0.3.2
│   ├── sha2 v0.10.9
│   ├── spki v0.7.3
│   ├── thiserror v1.0.69
│   ├── tiny_http v0.12.0
│   ├── tokio v1.53.1
│   ├── url v2.5.8
│   ├── x25519-dalek v2.0.1
│   └── zeroize v1.9.0
├── reqwest v0.12.28 (optional, feature-gated)
├── url v2.5.8 (optional, feature-gated)
└── [existing deps: async-trait, chrono, serde, serde_json, thiserror, tracing, uuid]
```

## Status

Mappers are implemented (semantic DTO ↔ Core). Wire layers are partially implemented:

- **A2A HTTP JSON-RPC/SSE server** — implemented (`protocol-a2a-server`:
  `build_router`, executor, card, health/auth, task_store) and wired in
  `adapterd` (`main.rs`, `build_router`), including `/healthz`/`/readyz`.
- **ACP stdio JSON-RPC loop** — implemented as library (`protocol-acp-runtime`:
  `AcpRuntime`, `codec`), but launch as separate process/profile is **deferred**
  (see `operations.md`). ACP is a legacy niche without development (strategy §9.3),
  so loop integration into `adapterd` is not prioritized.
- **ANP transport** — `RealAnpTransport` implemented in `crates/anp-transport/src/real.rs`
  (feature-gated `anp`). Identity verification, capability negotiation, and signed
  requests are wired. HTTP Message Signature signing is stubbed (TODO: integrate
  `anp_sdk::authentication::http_signatures`).

Mappers are designed so SDK updates only change the thin boundary layer,
not Core/stores/drivers.

## A2A dialect strategy (2026)

Protocol strategy for both products (gateway + adapter) is documented in
`docs/A2A-protocol-strategy-2026.md` (EN/UK/RU versions; `.summary.md` —
short summaries; unified spec — `docs/TZ-a2a-dialects-gateway-adapter.md`):

1. **Base — A2A SDK (v1.0, ProtoJSON):** `SendMessage`/`GetTask`/`CancelTask`.
2. **Fallback — A2A Spec (pre-1.0):** `message/send`/`tasks/get` — compatibility
   with older clients (Python `a2a-sdk` etc.).
3. **Deep fallback — ACP:** legacy installations only, no development.
4. **ANP (W3C DID)** — separate niche, out of scope.

Impact on adapter wire layers: `driver-a2a-client` gets `wire_format: auto`
(dialect probe + endpoint cache, SDK priority); `protocol-a2a-server` — accepts
Spec on input. Details and DoD in §9.2 of the strategy document.
