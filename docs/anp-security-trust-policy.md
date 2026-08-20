# ANP Security & Trust Policy (architect decision, binding)

**Status:** accepted, binding on `crates/anp-transport` and `crates/driver-anp-client`.
**Scope:** outbound ANP peer connections only (P0). Does not apply to A2A/ACP/MCP.

This document is the single source of truth for identity, trust and key-handling rules for ANP. `anp-transport` and `driver-anp-client` implementations must not diverge from it without a new ADR.

## 1. Trust modes

```rust
pub enum TrustPolicy {
    PinnedDid,   // default, production
    PinnedKey,   // alternative production mode
    InsecureDev, // tests only, localhost only
}
```

| Mode | Allowed in production | Allowed in tests | Requirement |
|---|---|---|---|
| `PinnedDid` | Yes (default) | Yes | Peer DID must exactly match configured `peer_did`; resolved DID document's `service[ANPMessageService].serviceEndpoint` must match configured endpoint |
| `PinnedKey` | Yes | Yes | Peer DID's `authentication` verification method key ID must be in configured `expected_key_ids` |
| `InsecureDev` | **No** | Yes, localhost only | Endpoint host must resolve to `127.0.0.1`, `::1`, or `localhost`; any non-localhost endpoint with this mode is a hard error, not a warning. **An explicit expected identity (peer DID or key ID) is still required even in this mode** — InsecureDev only relaxes the localhost/production restriction and the requirement for a full CA-style resolution chain, it never permits trust-on-first-use. A `FakeAnpTransport` or real transport implementation that accepts an unconfigured/arbitrary identity under `InsecureDev` violates this policy and must be fixed. |

Trust-on-first-use (accepting whatever identity is presented on first contact and remembering it) is **never** permitted, in any mode, including `InsecureDev`.

## 2. Identity verification sequence (mandatory order)

```text
1. Resolve peer DID document (over HTTPS unless InsecureDev + localhost).
2. Extract service endpoint from service[ANPMessageService].serviceEndpoint.
3. Compare resolved endpoint against configured endpoint (PinnedDid) — reject on mismatch.
4. Locate the verification method used to sign requests inside the DID document's
   `authentication` relationship specifically — NOT `assertionMethod`,
   NOT `keyAgreement`. A key valid only for those other relationships must be
   rejected the same way upstream's HttpSignatureError::
   VerificationMethodNotAuthorizedForAuthentication rejects it.
5. If PinnedKey: verification method key ID must appear in expected_key_ids.
6. Only after 1-5 succeed: proceed to anp.get_capabilities / anp.negotiate.
7. Any HTTP redirect encountered during resolution or during the session
   invalidates the identity check already performed; steps 1-5 must be
   re-run against the redirected target before continuing.
```

Failure at any step returns `anp_identity_untrusted` and is **non-retryable** without a configuration change (see error taxonomy in `docs/anp-driver-tz-revised.md` §10).

## 3. Key custody

- Private keys used to sign outbound ANP requests are **never** stored in:
  - `config/adapter.example.yaml` or any runtime YAML config;
  - `CoreEvent`/`CoreCommand` payloads;
  - logs, tracing spans, or metrics labels;
  - test fixtures committed to the repository, except keys generated at test-run time and discarded (never checked in as static files).
- Configuration holds only a **reference** to a secret (environment variable name, secret manager path). The reference format mirrors the existing `AuthConfig` pattern already used for bearer tokens (`docs/agent-connector-canonical-architecture.md` §"Security").
- Key rotation: on `anp_identity_untrusted` caused specifically by a stale key, the driver does not auto-retry with a different key; rotation requires an explicit config update and redeploy. Silent key-switching is not an acceptable driver behavior.

## 4. E2EE policy

- Direct E2EE (`require_e2ee: true`) is **optional** in P0 and defaults to `false` (transport-protected HTTPS + HTTP Message Signatures only).
- Enabling E2EE requires a separate, explicit follow-up decision covering: prekey publication endpoint, prekey rotation schedule, and where decrypted plaintext may transiently exist in memory/logs (never in logs).
- Group E2EE / MLS is out of scope for P0 entirely; do not add the `mls` upstream feature to the pinned dependency in the first transport PR.

## 5. Observability constraints (redaction rules)

Permitted fields (from `docs/anp-p0-integration-spec.md` §11, restated here as binding):

```text
protocol="anp", local_task_id, remote_task_id_hash, peer_did_hash,
endpoint_host, negotiated_version, negotiated_profile, stream_seq,
reconnect_attempt, capability_resume, terminal_state, latency_ms
```

Forbidden in any log/metric/trace at any verbosity level:

```text
raw peer_did, raw DID document, private keys, decrypted E2EE plaintext,
raw Authorization/Signature headers, task input/output content,
operation_id when it embeds sensitive caller data
```

`peer_did_hash` and `remote_task_id_hash` must use a stable one-way hash (not reversible, not raw truncation) so peers cannot be re-identified from logs while still allowing correlation across log lines for the same peer/task.

## 6. What this policy does NOT decide

Left to the transport implementation PR (senior-level, not architect-level):

- exact HTTP client configuration (timeouts, connection pooling);
- exact retry/backoff constants;
- exact DID resolution caching TTL.

These must not weaken any rule in §1–§5 above; they are implementation detail within the fixed trust boundary.
