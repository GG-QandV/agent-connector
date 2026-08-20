# ADR-ANP-001: ANP as Transport + Negotiated Task Profile

**Status:** accepted
**Date:** 2026-08-20
**Supersedes (narrowly):** the ANP out-of-scope statement in `docs/A2A-protocol-strategy-2026-en.md` §7, only for the identity/discovery/secure-transport layer described below. A2A remains the sole task-delegation wire (SDK v1.0 base, Spec pre-1.0 fallback, ACP deep fallback); ANP is not added as a third A2A dialect.

## Context

`docs/protocol-integrations-roadmap.md` marks ANP as P0. Upstream research (`anp-phase3-upstream-research.md`, upstream `agent-network-protocol/AgentConnect` at commit `aaca169c3e5b051e48875b023b60364a1dd93022`) confirms:

- ANP provides DID/WBA identity, WNS discovery, HTTP Message Signatures, Direct E2EE, and a generic `direct.send`/`direct.incoming` JSON-RPC messaging binding with delivery-level idempotency keyed on `(sender_did, target.did, method, operation_id)`.
- ANP defines **no** task lifecycle: no `invoke`/`status`/`cancel`/`provide_input`, no remote task ID, no event `seq`/cursor, no resumable stream, no terminal task states.
- ANP does define `anp.get_capabilities` and `anp.negotiate` (execution-mode/profile negotiation with `negotiationId`/`validUntil`).

## Decision

Integrate ANP as a two-layer stack inside `agent-connector`, connected in this fixed order:

```text
1. ANP transport/security  (DID/WNS resolution, endpoint+key binding,
                             HTTP Message Signatures, optional Direct E2EE)
2. Capability negotiation  (anp.get_capabilities, anp.negotiate)
3. Selected profile
     a. agent-connector.anp-task.v1 (our embedded task profile) -> full AgentDriver
     b. any other explicitly supported profile -> dedicated mapper
     c. no common task profile -> generic direct.send messaging only,
        no task/status/cancel/resume claims
```

`agent-connector.anp-task.v1` is an application profile **owned by this repository**, transported as JSON payloads inside ANP `direct.send`/`direct.incoming`. It defines `task.invoke`, `task.accepted`, `task.get_status`, `task.cancel`, `task.input_required`, `task.provide_input`, `task.progress`, `task.artifact`, `task.events` (with `after_seq`), `task.completed`, `task.failed`, `task.cancelled`. `seq` is monotonic per task and durable.

`AdapterCore` remains the sole owner of canonical `TaskId`, task state machine, durable `CoreEvent` history and `ReliableTaskStream` (ADR-adjacent: see `docs/reliable-task-stream-wiring.md`). Remote ANP task/message identifiers are correlation metadata only.

No peer is trusted without DID/key verification (no TOFU in production). A peer that does not negotiate `agent-connector.anp-task.v1` is still usable for generic ANP messaging, but the adapter never fabricates task lifecycle, cancellation or resumable-stream guarantees it cannot back with a real protocol contract.

## Consequences

- `driver-anp-client` only exposes full `AgentDriver` behavior (`invoke`/`cancel`/`provide_input` + reliable event stream) when `agent-connector.anp-task.v1` is the negotiated profile.
- A lower-level `anp-transport` capability (identity, discovery, signed channel, E2EE, capability negotiation) is reusable independently of task semantics and can be shipped before any task driver exists.
- Because upstream ANP has no stream cursor, `agent-connector.anp-task.v1`'s `task.events` stream resume/dedup logic must be implemented entirely by this repository (via `ReliableTaskStream`) — it cannot be inherited from upstream.
- Future non-`agent-connector` ANP peers can be supported by adding new mapper crates without changing `AdapterCore` or `ReliableTaskStream`.

## Open follow-ups (not blocking Phase 0/1)

- Pin exact `anp` crate revision and feature set (`jwt-pem`, `network`; `mls` optional) in workspace `Cargo.toml` once `driver-anp-client` implementation starts.
- Define wire schema/versioning policy for `agent-connector.anp-task.v1` (this ADR fixes the message set and envelope fields; full JSON Schema is a separate deliverable).
- Security review of key custody/storage for ANP private keys before any production peer is configured.
