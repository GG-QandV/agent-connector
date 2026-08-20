# ANP P0 — Agent-Side Execution Report

**Date:** 2026-08-20
**Branch:** `feat/anp-p0-foundation`
**Commits:** `ba83082`, `7d24f51`
**Audience:** Architect (approval for profile/schema/security)
**Scope:** Agent-side tasks P0-B (tests) + P0-D (schema fixtures) from the ANP P0 pool. No upstream SDK, no real transport, no trust-policy changes.

---

## 1. What was delivered

All six agent-side tasks from the pool are complete. Checks green: `cargo fmt --all`, `cargo clippy --workspace --all-targets -D warnings`, `cargo test --workspace` (52 suites, 0 failed). GitNexus `detect_changes`: risk LOW, 0 affected execution flows.

| Task | Deliverable | Where |
|---|---|---|
| 1. Storage E2E tests | Full path `AdapterCore → real TaskStore → subscribe/history → ReliableTaskStream`. SQLite runs in normal suite; PostgreSQL mirrored under `#[ignore]` + `TEST_DATABASE_URL`. Lag recovery proven with a 302-event burst (> broadcast 256) via durable catch-up. | `crates/adapter-core/tests/reliable_stream_real_stores.rs` |
| 2. Canonical JSON Schema | `docs/schemas/anp-task-v1.schema.json` (source of truth: `profile`/`message_type`/`operation_id`/`final`/`after_seq`). 4 fixtures in `examples/anp/`. 10 conformance tests. DTO/mapper aligned to schema (architecture decision: contract > local Rust structs). | `docs/schemas/`, `examples/anp/`, `crates/protocol-anp-profile/src/schema_conformance.rs` |
| 3. Negotiation TTL | `NegotiatedProfile { profile_id, capabilities, negotiation_id, valid_until }` mirroring ANP `negotiationId`/`validUntil`. Deterministic expiry tests via injected clock (no sleeps). Driver blocks task ops after expiry (`NegotiationExpired`) and renegotiates on reconnect. | `crates/anp-transport/src/negotiation.rs`, `crates/driver-anp-client/src/lib.rs` |
| 4. Profile mapper tests | invoke/cancel/provide_input → `CoreCommand`; all event types → `CoreEvent`; duplicate seq rejected; forward gap treated as recoverable (ReliableTaskStream catch-up), backward seq rejected; terminal rules; payload size limit; stable `operation_id`/`message_id` passthrough. | `crates/protocol-anp-profile/src/mapper.rs` |
| 5. State machine | `TaskProfileReady/MessagingOnly → Connecting` reconnect allowed; no unconditional `terminal → Connecting`; terminal task ≠ transport state; `Failed → Connecting` rejected. | `crates/driver-anp-client/src/state.rs` |
| 6. Docs consistency | `SelectedProfile` → `NegotiatedProfile` in transport-profile architecture doc; added architect's binding `docs/anp-security-trust-policy.md`. | `docs/anp-transport-profile-architecture.md` |

## 2. Architect decisions applied (from pool answers)

- **Task 1:** Full path through `AdapterCore` for both SQLite and Postgres; direct `events_after` tests only supplementary.
- **Task 2:** Schema is the canonical contract; DTO/mapper were aligned to it (not the reverse).
- **Task 3:** `valid_until` TTL on `NegotiatedProfile`; renegotiation required after expiry.
- **Task 5:** Transport reconnect allowed for active sessions; terminal **task** must not restart invocation.
- **Feature flag `anp`:** NOT added yet — deferred to P0.2 together with the pinned upstream SDK (avoiding a decorative flag).

## 3. Known deviations / notes

- **`model_O4.onnx` naming:** upstream TEI requires the file to be named `model.onnx`; the O4-optimized float32 file was renamed to `model.onnx` for the embeddings container (operational, outside this repo).
- **Fake transport vs security policy §1:** `FakeAnpTransport` still accepts `InsecureDev` on localhost **without** an explicitly configured expected identity. The trust policy requires an expected identity even for `InsecureDev`. This was left unchanged (trust policy is the architect's zone) and is flagged for P0-C.

## 4. What is NOT done (requires senior/architect)

| Item | Owner |
|---|---|
| Pin upstream `anp` SDK (immutable SHA `aaca169c`), feature `anp` | Senior (P0-C) |
| Real transport: DID/WNS resolver, HTTP Message Signatures, endpoint/key verification, `pinned_did`/`pinned_key` | Senior (P0-C) |
| `anp.get_capabilities` / `anp.negotiate` wiring | Senior (P0-C) |
| Local signed peer fixture (keep fake for unit tests) | Senior (P0-C) |
| Independent profile-capable peer (`agent-connector.anp-task.v1`) | Architect/Senior (P0-E) |
| E2EE / MLS decision (explicitly out of first transport PR) | Architect |

## 5. Deliverables returned per handoff §6

1. **Changed files:** listed above; commit `7d24f51` (24 files, +2342/−242).
2. **Cargo commands:** `cargo check --workspace`, `cargo test --workspace` (52 suites ok), `cargo clippy --workspace --all-targets -D warnings`, `cargo fmt --all` — all green.
3. **SDK revision/features:** not pinned yet (deferred to P0-C).
4. **ADR status:** `ADR-ANP-001` exists (`79d7063`); schema/security docs added but not yet pushed (awaiting GitHub write access confirmation).
5. **Profile schema & compatibility rules:** `docs/schemas/anp-task-v1.schema.json`; wire contract is canonical.
6. **Security/trust assumptions:** `docs/anp-security-trust-policy.md` (binding, architect-authored, included in commit).
7. **Tests added & results:** see table; all green.
8. **Known unsupported behavior:** no real peer interop; fake transport only; `InsecureDev` without expected identity (flagged).
9. **Required peer fixture details:** a peer that answers `get_capabilities`, `negotiate`, declares `agent-connector.anp-task.v1`, implements invoke/status/cancel/input and durable event history with `task.events(after_seq)`.
10. **Open questions for product decision:** P0 launch mode (outbound only — recommended); key custody source; E2EE policy timing.
