# ANP Transport + Negotiated Task Profile Architecture

## Decision

ANP is integrated as a three-layer stack:

```text
ANP transport/security
  → capability/profile negotiation
  → selected application profile
```

`agent-connector` embeds and owns `agent-connector.anp-task.v1`, an application task profile above ANP. It is not an A2A dialect and does not replace `AdapterCore`.

## Connection flow

```text
1. Resolve peer: static URL | WNS handle | DID.
2. Resolve and verify peer DID document.
3. Bind DID service endpoint and authorized authentication key.
4. Establish HTTPS/WSS protected channel; use HTTP Message Signatures.
5. Establish Direct E2EE if both policy and peer capability require it.
6. Call anp.get_capabilities.
7. Call anp.negotiate with locally supported profiles.
8. Select highest-priority mutually supported profile.
9. Instantiate matching adapter/driver capability.
```

```text
AnpTransport::connect(peer)
  → VerifiedAnpPeer
  → negotiate(["agent-connector.anp-task.v1", ...])
  → SelectedProfile
     ├── agent-connector.anp-task.v1 → full AgentDriver
     ├── known external profile       → dedicated mapper/adapter
     └── no common task profile       → generic direct.send messaging only
```

## Profile selection policy

| Selected profile | Adapter behavior | Guarantees |
|---|---|---|
| `agent-connector.anp-task.v1` | Enable `AgentDriver` invoke/cancel/input/event stream | Task lifecycle, durable event history, `seq`, `after_seq`, resume |
| Other supported profile | Select explicit mapper registered in adapter | Only guarantees documented by that mapper |
| No task profile | Expose `direct.send` messaging capability | Delivery idempotency only; no task/status/cancel/resume claim |
| Identity validation failure | Reject connection | No fallback to unauthenticated connection |

No profile downgrade is silent. The selected profile, peer DID and negotiated capabilities are logged as redacted structured metadata.

## `agent-connector.anp-task.v1`

### Required messages

```text
task.invoke
task.accepted
task.get_status
task.cancel
task.input_required
task.provide_input
task.progress
task.artifact
task.events
task.completed
task.failed
task.cancelled
```

All messages are application payloads transported through ANP `direct.send` / `direct.incoming`; the JSON-RPC envelope remains ANP Core Binding.

### Required envelope fields

```json
{
  "profile": "agent-connector.anp-task.v1",
  "task_id": "canonical adapter UUID",
  "operation_id": "ANP idempotency key",
  "message_id": "per-message UUID",
  "causation_id": "message or command that caused this event",
  "seq": 42,
  "payload": {}
}
```

- `task_id` is canonical within `AdapterCore`.
- `operation_id` maps to ANP `direct.send` delivery idempotency.
- `seq` is monotonically increasing within a task and is persisted durably.
- `task.events { after_seq }` returns all events with `seq > after_seq`, then live updates.
- `Completed`, `Failed` and `Cancelled` are durable terminal events.

## Ownership boundaries

```text
ANP transport
  DID/WNS, HTTP signatures, E2EE, direct.send, direct.incoming,
  anp.get_capabilities, anp.negotiate

ANP Task Profile
  task DTOs, profile schema/version, remote task semantics,
  status/cancel/input, seq and resume contract

AdapterCore
  local canonical TaskId, authorization, task state machine,
  durable CoreEvent history, ReliableTaskStream, downstream subscriptions
```

The upstream ANP SDK provides the first layer but not the second: it has no standard task ID, task lifecycle, cursor-based stream resume or terminal task events. Therefore task semantics exist only after profile negotiation. [file:56]

## Rust module plan

```text
crates/anp-transport/
  did.rs              # DID/WNS resolution and endpoint binding
  auth.rs              # HTTP Message Signatures, credential hooks
  e2ee.rs              # optional Direct E2EE session
  rpc.rs               # ANP JSON-RPC request/notification envelope
  negotiate.rs         # capabilities and selected profile

crates/protocol-anp-profile/
  profile.rs           # ProfileId, selection policy, DTO validation
  task_v1.rs           # agent-connector.anp-task.v1 wire DTOs
  mapper.rs            # Task DTO ↔ CoreCommand/CoreEvent

a crates/driver-anp-client/
  transport_adapter.rs # AnpTransport abstraction
  task_profile.rs      # full AgentDriver only for selected task profile
  messaging.rs         # generic direct.send fallback capability
```

`anp-transport` is reusable without task semantics. `driver-anp-client` refuses `invoke` unless a compatible task profile is negotiated.

## Delivery roadmap

1. Add ADR: Transport → Negotiation → Selected Profile architecture.
2. Add pinned upstream ANP SDK and transport abstraction, behind feature `anp`.
3. Implement identity verification and capability negotiation with a local fixture.
4. Implement `protocol-anp-profile` with local `agent-connector.anp-task.v1` schema and mapper.
5. Complete `ReliableTaskStream` in `adapter-core`.
6. Implement profile-aware `driver-anp-client`.
7. Add a second agent-connector ANP profile peer for end-to-end invocation and reconnect tests.
8. Keep generic ANP peers interoperable through messaging-only fallback.
