# P0-C: Real ANP Transport — Senior Implementation Spec

**Status:** ready for implementation.
**Owner:** senior/agent, with architect review before merge.
**Depends on:** `docs/anp-security-trust-policy.md` (binding), `docs/schemas/anp-task-v1.schema.json`, `crates/anp-transport` foundation (commit `ba83082`/`7d24f51`, schema-alignment fix `a48b12aa`).
**Blocked by:** none — the `FakeAnpTransport` InsecureDev expected-identity fix has already landed (commit `3a6fc2a` reported by agent; verify it is pushed and visible on GitHub before starting, since `RealAnpTransport` reuses the same expected-identity plumbing).

## 1. Why this doc instead of direct code push

The exact current signature of the `AnpTransport` trait, `PeerRef`, `VerifiedAnpPeer`, `TrustLevel`, `PeerIdentity` and capability/negotiation types already implemented by the agent in `crates/anp-transport/src/*.rs` is not fully visible through available tooling (file existence/paths were confirmed, full contents were not reliably retrievable for large/updated files). Implementing against a guessed signature risks producing code that does not compile or silently diverges from the already-tested foundation. This spec gives exact, unambiguous requirements; the agent/senior implements against the real trait with full local file access.

## 2. Dependency pin (exact)

Add to `crates/anp-transport/Cargo.toml`, gated behind a new `anp` feature — do not enable by default:

```toml
[features]
default = []
anp = ["dep:anp-sdk", "dep:reqwest", "dep:url"]

[dependencies]
anp-sdk = { package = "anp", git = "https://github.com/agent-network-protocol/AgentConnect", rev = "aaca169c3e5b051e48875b023b60364a1dd93022", default-features = false, features = ["jwt-pem", "network"], optional = true }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"], optional = true }
url = { version = "2", optional = true }
```

Rules:

- `rev` must be the exact 40-char SHA above — never `branch = "master"`, never a bare `version`.
- Do not add the `mls` feature (group E2EE is explicitly out of scope for P0 per `docs/anp-security-trust-policy.md` §4).
- `default-features = false` on `anp-sdk`, only `jwt-pem` + `network` enabled — this excludes MLS/openmls/rusqlite from the build.
- After adding, run `cargo deny check` (or `cargo tree -p anp-transport --features anp` at minimum) and record the resulting dependency list in `docs/protocol-compatibility.md`.
- `cargo check -p anp-transport` (no features) must still succeed with zero new dependencies pulled in — this proves the feature gate is real, not decorative.
- `cargo check -p anp-transport --features anp` must succeed and pull in `anp-sdk`.

## 3. New module: `src/real.rs` (feature-gated)

Add a new file `crates/anp-transport/src/real.rs`, compiled only under `#[cfg(feature = "anp")]`, and reference it from `lib.rs` with:

```rust
#[cfg(feature = "anp")]
mod real;
#[cfg(feature = "anp")]
pub use real::RealAnpTransport;
```

This is the only change to `lib.rs` — a two-line, additive, feature-gated `mod`/`pub use`, matching the same low-risk wiring pattern used for `ReliableTaskStream` (`docs/reliable-task-stream-wiring.md`). Do not touch any other part of `lib.rs`.

`RealAnpTransport` must implement the exact same `AnpTransport` trait that `FakeAnpTransport` already implements (find the trait definition in `crates/anp-transport/src/transport.rs` — read it directly, do not guess the signature). Every method `FakeAnpTransport` implements, `RealAnpTransport` implements with real behavior; do not add or remove trait methods.

## 4. Identity verification (must follow `docs/anp-security-trust-policy.md` §2 exactly)

Implement as a private helper used by `RealAnpTransport::connect(...)`, in this exact order — do not reorder or skip steps:

```text
1. Resolve peer DID document via anp_sdk::authentication::did_resolver
   (resolve_did_document / resolve_did_document_with_options).
   Use HTTPS unless TrustPolicy::InsecureDev AND the endpoint host is
   127.0.0.1 / ::1 / localhost.
2. Extract service endpoint: find the service entry whose type matches
   the ANP message service (see resolved DID document's `service[]` array)
   and read its `serviceEndpoint`.
3. Compare resolved endpoint against the configured endpoint for this peer.
   Reject with `anp_identity_untrusted` on any mismatch under
   TrustPolicy::PinnedDid.
4. From the DID document's `authentication` relationship specifically
   (never `assertionMethod`, never `keyAgreement`), locate the
   verification method whose key ID matches the one used to sign
   requests. If the key is only valid under another relationship,
   reject — mirror upstream's
   HttpSignatureError::VerificationMethodNotAuthorizedForAuthentication.
5. Under TrustPolicy::PinnedKey, additionally require that this
   verification method's key ID appears in the configured
   expected_key_ids list.
6. Under TrustPolicy::InsecureDev, require an explicitly configured
   expected identity (peer_did or key_id) passed into RealAnpTransport's
   constructor/config — reuse the same expected-identity plumbing already
   added to FakeAnpTransport (PeerIdentity::matches_expected, commit
   `3a6fc2a`) for consistency. Do NOT accept an unconfigured/first-seen
   identity even on localhost.
7. Only after steps 1-6 succeed, proceed to capability/negotiation calls.
8. If any HTTP redirect is followed during resolution or during an
   active session, invalidate the already-performed identity check and
   re-run steps 1-6 against the redirected target before continuing.
```

Any failure returns the existing `anp_identity_untrusted` error variant already defined in `crates/anp-transport/src/error.rs` (read it, reuse it — do not invent a new error type for this).

## 5. Signed requests

For every outbound request to the peer (`anp.get_capabilities`, `anp.negotiate`, and any `direct.send`/messaging call the existing `AnpTransport::send` method already defines):

```text
1. Build the JSON-RPC 2.0 envelope (existing message.rs types).
2. Sign it using anp_sdk::authentication::http_signatures::
   generate_http_signature_headers(did_document, request_url, method,
   private_key, headers, body, HttpSignatureOptions).
3. Attach the resulting Signature / Signature-Input / Content-Digest
   headers to the outbound reqwest request.
4. Never log the private key, the signature headers' raw values, or the
   full DID document (see redaction rules in
   docs/anp-security-trust-policy.md §5).
```

Private key material comes from a `KeyProvider` you introduce as a small trait (`fn private_key(&self) -> &anp_sdk::PrivateKeyMaterial`), backed by a config-supplied secret reference (env var name), never a literal key in config/YAML. Do not implement key generation, rotation, or storage beyond loading from the referenced source — that is explicitly out of scope per the security policy §3.

## 6. Capability negotiation wiring

Reuse the existing `crates/anp-transport/src/negotiation.rs` and `capabilities.rs` types the agent already built (including the `NegotiatedProfile { profile_id, capabilities, negotiation_id, valid_until }` TTL model). `RealAnpTransport` must:

1. Call the peer's `anp.get_capabilities` (built as a signed JSON-RPC request per §5).
2. Parse the response into the existing capability model (do not create a parallel one).
3. Call `anp.negotiate` offering the locally supported profiles (including `agent-connector.anp-task.v1`).
4. Feed the result into the existing `NegotiatedProfile` construction path, including `valid_until`.
5. Do not implement your own expiry logic — reuse whatever the agent already wired into `driver-anp-client` for `NegotiationExpired`.

## 7. Local signed peer fixture (for integration tests, not unit tests)

Add under `crates/anp-transport/tests/` (feature-gated `#[cfg(feature = "anp")]`, and network-touching tests marked `#[ignore]`):

- A fixture that starts a minimal local HTTP server serving a static DID document and verifying incoming HTTP Message Signatures, modeled on upstream's `examples/python/rust_interop_examples/python_auth_server.py` pattern but implemented in Rust using `anp_sdk` verification primitives directly (no Python dependency in this repo's test suite).
- Test: `RealAnpTransport` successfully connects to this fixture under `PinnedDid` with a matching configured DID/endpoint.
- Test: connection is rejected under `PinnedDid` when the fixture's DID document endpoint does not match the configured endpoint.
- Test: connection is rejected when the signing key is only present under `keyAgreement`/`assertionMethod`, not `authentication`.
- Test: `anp.get_capabilities` → `anp.negotiate` round-trip against the fixture returns a `NegotiatedProfile` with a `valid_until` in the future.

Keep `FakeAnpTransport` as-is for all existing non-network unit tests — do not replace it with `RealAnpTransport` anywhere tests don't require real HTTP/DID resolution.

## 8. Explicit non-goals for this PR

- No Direct E2EE / MLS wiring (policy §4 — separate follow-up).
- No inbound ANP server.
- No `driver-anp-client` task-profile behavior changes beyond swapping in `RealAnpTransport` where `FakeAnpTransport` was previously injected for manual/local testing — the state machine and profile dispatch logic the agent already wrote should not need changes.
- No changes to `AdapterCore`, `ReliableTaskStream`, A2A, or ACP.

## 9. Checks before PR

```bash
cargo fmt --all --check
cargo check --workspace
cargo check -p anp-transport --features anp
cargo test --workspace
cargo test -p anp-transport --features anp -- --ignored   # only with local fixture running
cargo clippy --workspace --all-targets -- -D warnings
```

## 10. Definition of done

- [ ] `anp-sdk` pinned by exact SHA, `anp` feature added, default build unaffected.
- [ ] `RealAnpTransport` implements the existing `AnpTransport` trait with no signature changes.
- [ ] Identity verification follows `docs/anp-security-trust-policy.md` §2 step-by-step, no shortcuts.
- [ ] `InsecureDev` requires explicit expected identity in `RealAnpTransport` too (consistent with the `FakeAnpTransport` fix in `3a6fc2a`).
- [ ] All outbound requests are signed; no private key or raw signature ever logged.
- [ ] Capability/negotiation reuses existing `NegotiatedProfile`/TTL model, no duplicate logic.
- [ ] Local signed peer fixture exists and integration tests pass under `--ignored`.
- [ ] `docs/protocol-compatibility.md` updated with the pinned SDK revision, enabled features, and dependency tree summary.
- [ ] All commands in §9 green.
