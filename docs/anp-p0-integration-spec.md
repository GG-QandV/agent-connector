# P0 ANP Integration — Architecture, Specification and Implementation Roadmap

**Status:** proposed; implementation begins only after ADR-ANP-001 is accepted.

## 1. Decision gate

`docs/protocol-integrations-roadmap.md` marks ANP as P0. However, the approved A2A strategy currently explicitly classifies ANP/W3C DID as a separate niche and **out of scope** for the A2A wire layer. This specification therefore does **not** add ANP as a third A2A dialect and does not change A2A SDK → Spec → ACP priority.

ADR-ANP-001 must supersede that narrow out-of-scope decision only for an **optional open-web identity, discovery and secure peer transport profile**:

```text
A2A = task delegation wire (unchanged)
ANP = optional DID/WNS identity + discovery + protected peer channel
AdapterCore = canonical task/event lifecycle (unchanged)
```

No implementation starts before the ADR identifies one concrete peer/use case and approves the security/trust model.

## 2. Inputs and constraints

### Current agent-connector architecture

```text
Inbound protocol server             Outbound driver
protocol-a2a-server                 driver-a2a-client
protocol-acp-runtime                driver-acp-client
                                  driver-http-sse / driver-mcp / driver-stdio
          │                                   │
          └──────── mapper / AgentDriver ─────┘
                              │
                        AdapterCore
           CoreCommand / DispatchResult / CoreEvent / TaskSubscription
                              │
                   task stores: memory / SQLite / PostgreSQL
```

Existing protocol mapper pattern:

- `protocol-a2a-mapper` is transport-neutral and maps A2A DTOs to `CoreCommand`, then maps `CoreEvent` to A2A updates.
- `protocol-acp-mapper` follows the same pattern for ACP.
- Drivers implement `AgentDriver`: `health`, `invoke`, `cancel`, `provide_input`; `invoke` yields `mpsc::Receiver<DriverEvent>`.
- `AdapterCore` owns task identity, authorization, idempotency, state transitions, durable event history and live subscription.

ANP must preserve these boundaries. It must not add an ANP task store, an ANP event log, or an alternate task state machine.

### Streaming prerequisite

Before ANP streaming is exposed, complete the planned `ReliableTaskStream` abstraction:

```text
durable history + monotonic CoreEvent.seq + after_seq
  + catch-up on broadcast Lagged/gap + terminal close
```

ANP transport must consume this common stream; it must not rely solely on `tokio::broadcast`.

### ANP Rust SDK evidence

Checked upstream: [agent-network-protocol/anp](https://github.com/agent-network-protocol/anp), Rust workspace at [`rust/`](https://github.com/agent-network-protocol/anp/tree/master/rust). The source tree includes modules for canonical JSON, keys, authentication, proofs, WNS and direct/group E2EE.

**Important:** the inspected SDK shape demonstrates an identity/security foundation; it does not by itself prove a stable, complete task-delegation client/server binding compatible with `AgentDriver`. Treat upstream as a pinned crypto/identity capability candidate, while keeping ANP application DTOs and transport boundary owned by this repository until conformance proves otherwise.

## 3. Scope

### P0 release scope

1. Outbound integration: `agent-connector` invokes a configured remote ANP peer as an `AgentDriver`.
2. ANP peer identity verification and mapping to local configured `AgentId`.
3. Basic remote capability discovery and routing eligibility.
4. Invoke, progress, artifact, input-required, terminal result, cancel, timeout and idempotency mapping.
5. Ordered stream/reconnect when upstream binding supports a stable event cursor.
6. Rust feature flag; default build remains unchanged.

### Deferred

- Generic public inbound ANP server.
- Automatic WNS registration or automatic DID publication.
- Multi-hop routing, group E2EE, payments, reputation, marketplace semantics.
- Replacing A2A’s Agent Card, OAuth or A2A wire.
- Accepting arbitrary peer DID documents from the open internet without allowlist/trust policy.

## 4. Target crates and dependencies

```text
crates/
  protocol-anp-mapper/       # local ANP semantic DTO ↔ AdapterCore DTO
  driver-anp-client/         # optional outbound AgentDriver implementation
  protocol-anp-server/       # deferred; reserve name only, no P0 crate required
```

### Dependency direction

```text
protocol-anp-mapper
  ├── adapter-core
  ├── adapter-model
  ├── serde / thiserror / async-trait
  └── no HTTP runtime, no database, no ANP SDK dependency

driver-anp-client
  ├── adapter-core
  ├── protocol-anp-mapper
  ├── anp Rust SDK (pinned exact revision; optional)
  ├── transport client dependencies
  └── no direct task-store dependency
```

`protocol-anp-mapper` deliberately uses local stable DTOs. This prevents an upstream SDK API change from leaking into Core, stores or other protocols.

## 5. Workspace/configuration changes

### Workspace

- Add both P0 crates to root `Cargo.toml` workspace members.
- Add an `anp` feature to `adapterd` and `adapterctl`; the feature enables `driver-anp-client`.
- Pin upstream by immutable commit, never `branch = "master"`:

```toml
anp-sdk = { git = "https://github.com/agent-network-protocol/anp", rev = "<verified-40-char-SHA>", package = "<verified-package-name>", optional = true }
```

The exact package name/version and SHA are an explicit Phase 0 deliverable, because they must be taken from the reviewed upstream manifest and lockfile—not guessed in this spec.

### Example configuration

```yaml
agents:
  - id: research-anp-peer
    driver: anp
    endpoint: https://peer.example/.well-known/anp
    anp:
      peer_did: did:wba:peer.example
      expected_key_ids:
        - did:wba:peer.example#key-1
      allowed_protocol_versions: ["<validated-version>"]
      connect_timeout_ms: 5000
      request_timeout_ms: 30000
      stream_idle_timeout_ms: 60000
      reconnect_attempts: 8
      require_e2ee: true
      trust_policy: pinned_did
```

Secrets (private keys, client credentials, wallet material) are references to an external secret provider; they must never be represented in YAML, task context or telemetry.

## 6. Local ANP semantic model

The P0 mapper must define minimal local types, with upstream SDK conversions isolated in `driver-anp-client`.

```rust
pub struct AnpPeerDescriptor {
    pub peer_id: String,
    pub did: String,
    pub endpoint: url::Url,
    pub protocol_version: String,
    pub capabilities: AnpCapabilities,
}

pub struct AnpCapabilities {
    pub invoke: bool,
    pub streaming: bool,
    pub resume: bool,
    pub cancellation: bool,
    pub provide_input: bool,
    pub artifacts: bool,
}

pub struct AnpInvoke {
    pub task_id: TaskId,
    pub idempotency_key: String,
    pub session_id: Option<Uuid>,
    pub input: Vec<Part>,
    pub context: serde_json::Value,
    pub deadline_ms: Option<u64>,
}

pub struct AnpEventEnvelope {
    pub task_id: TaskId,
    pub seq: u64,
    pub event_id: String,
    pub kind: AnpEventKind,
}
```

`AnpEventEnvelope.seq` is mandatory for a stream advertised as resumable. If the upstream peer cannot provide a monotonic cursor, P0 runs in **non-resumable mode** and must advertise this; it must not invent reliable resume semantics from arrival order.

## 7. Mapping contract

### Inbound / outbound command mapping

| Local ANP operation | AdapterCore / driver mapping | Required property |
|---|---|---|
| Invoke/delegate | `AgentDriver::invoke(task_id, InvokeRequest)` | stable `task_id`, idempotency key |
| Cancel | `AgentDriver::cancel(task_id)` | idempotent; propagate reason if upstream supports it |
| Provide input | `AgentDriver::provide_input(task_id, input)` | only when negotiated |
| Status | ANP status request or event history | must not mutate task |
| Capability discovery | `AnpPeerDescriptor` → configured eligible agent | cache/TTL and identity-bound endpoint |

### Event mapping

| ANP semantic event | `DriverEvent` | Core result |
|---|---|---|
| Accepted / acknowledged | `Accepted` | `CoreEventKind::Accepted` |
| Progress | `Progress { message, percent }` | `CoreEventKind::Progress` |
| Artifact | `Artifact(ArtifactRef)` | `CoreEventKind::Artifact` |
| Input required | `InputRequired(InputRequest)` | `CoreEventKind::InputRequired` |
| Completed | `Completed(Vec<Part>)` | `CoreEventKind::Completed` |
| Failed | `Failed(PublicError)` | `CoreEventKind::Failed` |
| Cancelled | `Cancelled` | `CoreEventKind::Cancelled` |

Rules:

- ANP peer event identifiers are retained as diagnostics/correlation data, but local durable ordering is `CoreEvent.seq`.
- `InputRequest` must carry a stable request ID supplied by peer/event data. Never regenerate it during mapper replay.
- The local `TaskId` remains canonical. A remote task ID is stored as peer correlation metadata, never replaces the Core task ID.
- Remote error codes map to stable `PublicError { code, message, retryable }`; raw remote payload goes only into redacted diagnostics.

## 8. Identity and trust boundary

ANP’s DID/WNS/E2EE capabilities are valuable precisely because this boundary is security-sensitive.

### Required P0 trust policy

```text
configured peer endpoint
  → fetch/resolve peer DID metadata
  → validate DID ↔ endpoint binding
  → validate expected key / proof under pinned trust policy
  → negotiate supported protocol version and capabilities
  → create protected channel
  → invoke only after identity verification
```

Allowed P0 modes:

- `pinned_did`: configured DID and configured endpoint must match; default.
- `pinned_key`: peer DID plus allowed verification key IDs/fingerprints.
- `insecure_dev`: only in tests/local environment; rejected when production profile is enabled.

Prohibited in P0:

- trust-on-first-use in production;
- accepting arbitrary discovered endpoint redirect without identity revalidation;
- caller scopes inferred solely from free-form peer metadata;
- putting private keys or decrypted payloads in logs.

Identity mapping:

```text
verified peer DID       → CallerId("anp:<did>")
validated capabilities  → configured / policy-derived scopes
configured local peer   → AgentId
```

## 9. Streaming and recovery contract

### Required remote capability negotiation

Before starting stream, client records peer flags:

```text
streaming, resume, cancellation, provide_input, artifacts
```

If `resume=true`, the peer must define a cursor compatible with:

```text
subscribe(remote_task_id, after_seq = N) => all events with seq > N, then live stream
```

### Driver loop

```text
invoke -> receive/create remote task correlation
open stream(after_seq=0)
for each remote event:
  reject wrong remote task / invalid identity
  reject duplicate seq <= latest_seq
  if seq > latest_seq + 1: request remote durable catch-up
  map -> DriverEvent -> AdapterCore durable CoreEvent
on disconnect:
  if peer resume capability: reconnect(after_seq=latest_seq)
  else: query remote status; emit terminal state if authoritative,
        otherwise fail with retryable "stream_unavailable"
on local mpsc consumer closed: stop; do not reconnect
on terminal: deliver exactly once, stop
```

This driver-level remote cursor is distinct from the local client-facing `CoreEvent.seq`; the Core persists normalized events, then its own reliable stream handles downstream A2A/ACP/other subscribers.

## 10. Error and retry policy

| Condition | Driver behavior | `PublicError` / outcome |
|---|---|---|
| DID/key/proof validation fails | no retry until config changes | `anp_identity_untrusted`, non-retryable |
| Unsupported version/capability | fail before invoke | `anp_unsupported_capability`, non-retryable |
| Connection timeout before accepted | retry only if idempotency is guaranteed | retryable transport error |
| Disconnect after accepted + resume | reconnect using last remote cursor | no task failure while attempts remain |
| Disconnect without resume | query authoritative status, otherwise fail | `stream_unavailable`, retryable |
| Duplicate event | ignore | none |
| Sequence gap + history available | catch up | none |
| Sequence gap + no recovery | fail task/stream explicitly | `anp_stream_gap`, retryable only if safe |
| Remote cancellation | map once | `Cancelled` |
| Local cancellation | propagate once; persist Core cancellation flow | normal cancellation lifecycle |

Retries never duplicate a remote side-effecting invocation unless the same idempotency key is accepted by the peer and peer identity remains unchanged.

## 11. Observability

Required structured fields:

```text
protocol="anp", local_task_id, remote_task_id_hash, peer_did_hash,
endpoint_host, negotiated_version, stream_seq, reconnect_attempt,
capability_resume, terminal_state, latency_ms
```

Do not log task content, credentials, full DID documents, decrypted E2EE content, public keys beyond configured fingerprints, or raw authorization headers.

Metrics:

- `anp_invocations_total{outcome}`
- `anp_identity_verification_total{outcome}`
- `anp_stream_reconnects_total`
- `anp_stream_gap_total`
- `anp_stream_lag_recovery_total`
- `anp_request_duration_seconds`

## 12. Implementation roadmap

### Phase 0 — ADR and upstream qualification

- [ ] Write/approve ADR-ANP-001; resolve conflict with existing A2A strategy’s ANP out-of-scope statement.
- [ ] Select one target ANP peer and one concrete interoperability scenario.
- [ ] Review upstream `rust/Cargo.toml`, README, source API, license, release cadence and security advisories.
- [ ] Record exact repository commit SHA, package name/version, Rust MSRV and license in `docs/protocol-compatibility.md`.
- [ ] Verify upstream has the required binding for invoke/cancel/input/stream; if absent, constrain P0 to identity/discovery or defer driver implementation.
- [ ] Run `cargo deny`, dependency audit, license review and secret scan on the pinned dependency graph.

**Exit:** approved ADR + reproducible dependency pin + evidence that required binding exists.

### Phase 1 — Core streaming prerequisite

- [ ] Implement/test `ReliableTaskStream` in `adapter-core`.
- [ ] Guarantee durable catch-up after `broadcast::Lagged` and sequence gap.
- [ ] Establish `after_seq = last successfully processed event` contract.
- [ ] Add regression tests for subscribe race, lag recovery, duplicate suppression, resume and terminal close.

**Exit:** downstream protocol adapters can use a reliable local subscription API.

### Phase 2 — Mapper crate

- [ ] Create `crates/protocol-anp-mapper` with only local DTOs and Core mapping.
- [ ] Define/validate `AnpPeerDescriptor`, capability model, invoke/cancel/input/status/event envelopes.
- [ ] Implement command/event/error conversion tables from sections 6–7.
- [ ] Add unit tests without network/SDK dependency.
- [ ] Add stable serialization tests for mapper DTOs where persisted/configured.

**Exit:** 100% mapping tests pass with fake `A2aCoreService`-style Core facade.

### Phase 3 — Outbound driver

- [ ] Create `crates/driver-anp-client` implementing `AgentDriver`.
- [ ] Add feature-gated, pinned ANP SDK adapter behind an internal `AnpTransport` trait.
- [ ] Implement peer resolution, DID/key verification and capability negotiation.
- [ ] Implement invoke with exact idempotency key propagation.
- [ ] Implement cancel/provide-input only after capability checks.
- [ ] Implement stream loop, duplicate/gap/reconnect behavior and terminal stop.
- [ ] Wire driver registration/config parsing in `adapterd`/`adapterctl`.

**Exit:** local mock peer supports end-to-end invoke through `AdapterCore`.

### Phase 4 — Interoperability and resilience

- [ ] Implement independent peer fixture using upstream ANP reference implementation, not the same local mock adapter.
- [ ] Test verified identity success/failure, key rotation and endpoint mismatch.
- [ ] Test idempotent retry before/after remote acceptance.
- [ ] Test progress, artifacts, input-required, completed, failed and cancelled mapping.
- [ ] Test disconnect before first event, mid-stream, duplicate event, gap, resume and terminal disconnect.
- [ ] Test slow local consumer: Core remains live, durable history recovers subscriber.
- [ ] Run SQLite and PostgreSQL event-store variants.

**Exit:** compatibility matrix and E2E suite pass in CI.

### Phase 5 — Documentation and controlled release

- [ ] Add `docs/anp-integration.md` derived from this specification.
- [ ] Update `docs/protocol-compatibility.md` with upstream pin, supported ANP version/capabilities and known gaps.
- [ ] Update `config/adapter.example.yaml`, operations docs and threat model.
- [ ] Publish feature as experimental; require explicit `driver: anp` configuration.
- [ ] Add dashboards/alerts for identity failures, stream gaps and reconnect exhaustion.

**Exit:** opt-in production pilot with one trusted peer; no default activation.

## 13. Acceptance criteria

P0 ANP is complete only when all are true:

- A2A SDK/Spec/ACP behavior is unchanged and ANP is not parsed as an A2A dialect.
- Default build and deployment do not include or enable ANP unless feature/config is selected.
- A configured, pinned, verified DID peer is callable through `AgentDriver`.
- Local task IDs, idempotency and durable Core events remain canonical.
- Remote stream continuation does not lose events when peer supports resumable cursor semantics.
- A missing remote resume guarantee is surfaced as a documented limitation, not silently claimed as reliable streaming.
- Independent upstream interop, security negative tests and reconnect tests run in CI.
