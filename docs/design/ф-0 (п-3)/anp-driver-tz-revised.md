# ТЗ: ANP transport + negotiated task profile

## Статус

- **Статус:** revised implementation specification.
- **Дата:** 2026-08-20.
- **Scope:** outbound ANP integration in `agent-connector`.
- **Branch:** `feat/anp-p0-foundation`.
- **Upstream SDK:** `agent-network-protocol/AgentConnect`, Rust crate `anp`, immutable revision `aaca169c3e5b051e48875b023b60364a1dd93022`, version `0.9.4` at git HEAD, MSRV `1.88`, MIT.

## 1. Итоговое решение

ANP интегрируется не как альтернативный task-протокол и не как третий A2A dialect. Реализуется трёхслойная схема:

```text
ANP transport/security
        ↓
capability/profile negotiation
        ↓
selected application profile
```

### Слой 1: ANP transport

Используются upstream capabilities:

- DID/WBA identity;
- WNS/static discovery;
- DID document resolution;
- endpoint ↔ DID/key binding;
- HTTP Message Signatures;
- HTTPS/WSS protected transport;
- optional Direct E2EE;
- JSON-RPC envelope and `direct.send`/`direct.incoming` messaging.

### Слой 2: negotiation

Клиент получает capabilities через `anp.get_capabilities` и предлагает профили через `anp.negotiate`.

### Слой 3: selected profile

Основной встроенный профиль:

```text
agent-connector.anp-task.v1
```

Он реализует task semantics поверх ANP application payload. Полный `AgentDriver` включается только после успешного выбора этого профиля.

Если peer не поддерживает профиль:

- generic ANP messaging остаётся доступным через `direct.send`;
- известный внешний профиль подключается отдельным mapper;
- task lifecycle/cancel/resume не эмулируются и не заявляются.

## 2. Что upstream ANP даёт и не даёт

### Даёт

- DID/WBA document and key generation;
- WNS resolution;
- HTTP Message Signatures;
- Direct E2EE;
- `direct.send` / `direct.incoming`;
- `anp.get_capabilities`;
- `anp.negotiate`;
- delivery-level idempotency через `operation_id`.

### Не даёт

- `invoke/status/cancel/provide_input` task API;
- remote task ID;
- terminal task states;
- event `seq`/cursor;
- `after_seq`/`Last-Event-ID`;
- durable task event history;
- resumable stream.

Следовательно, `agent-connector.anp-task.v1` — прикладной профиль этого проекта, а не функция чистого upstream SDK.

## 3. Границы архитектуры

```text
crates/anp-transport/
  DID/WNS, signatures, E2EE, JSON-RPC, capabilities, negotiation
          ↓
crates/protocol-anp-profile/
  profile registry, task-v1 DTO, validation, Core mapping
          ↓
crates/driver-anp-client/
  profile-aware AgentDriver, generic messaging fallback
          ↓
AdapterCore
  TaskId, lifecycle, idempotency, durable CoreEvent history,
  ReliableTaskStream, downstream subscriptions
```

`AdapterCore` остаётся единственным владельцем:

- canonical local `TaskId`;
- task state machine;
- idempotency;
- durable events;
- downstream streaming/resume.

ANP не получает отдельный store, event log или state machine.

## 4. Negotiation contract

### Local offers

Минимальный список:

```json
{
  "profiles": [
    {
      "id": "agent-connector.anp-task.v1",
      "invoke": true,
      "status": true,
      "cancellation": true,
      "provide_input": true,
      "streaming": true,
      "resume": true,
      "artifacts": true
    },
    {
      "id": "anp.generic-messaging.v1",
      "messaging": true
    }
  ]
}
```

### Selection policy

1. Проверить peer identity до negotiation.
2. Получить remote capabilities.
3. Выбрать `agent-connector.anp-task.v1`, если peer подтверждает весь обязательный набор.
4. Иначе выбрать явно поддерживаемый внешний profile.
5. Иначе включить messaging-only fallback.
6. Никогда не понижать task profile молча.

```rust
pub enum SelectedProfile {
    AgentConnectorTaskV1(TaskProfileCapabilities),
    External(String),
    GenericMessaging,
}
```

`GenericMessaging` не реализует `AgentDriver`; это отдельная capability.

## 5. Профиль `agent-connector.anp-task.v1`

### Message types

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

### Envelope

```json
{
  "profile": "agent-connector.anp-task.v1",
  "message_type": "task.progress",
  "task_id": "profile-task-id",
  "remote_task_id": "peer-task-id",
  "operation_id": "stable-idempotency-key",
  "message_id": "unique-message-id",
  "causation_id": "parent-message-id",
  "seq": 42,
  "payload": {}
}
```

Правила:

- `operation_id` переиспользуется при safe retry.
- `message_id` уникален для каждой доставки.
- `seq` монотонен и durable внутри profile task.
- `task.events(after_seq=N)` возвращает все события с `seq > N`, затем live events.
- `Completed`, `Failed`, `Cancelled` immutable и terminal.
- Peer может объявлять `resume=true` только если реально хранит history и поддерживает catch-up.

## 6. Mapping в AdapterCore

| Profile message | Local mapping |
|---|---|
| `task.invoke` | `CoreCommand::Invoke` / `AgentDriver::invoke` |
| `task.cancel` | `CoreCommand::Cancel` / `AgentDriver::cancel` |
| `task.provide_input` | `CoreCommand::ProvideInput` / `AgentDriver::provide_input` |
| `task.get_status` | `CoreCommand::GetStatus` |
| `task.accepted` | `DriverEvent::Accepted` |
| `task.progress` | `DriverEvent::Progress` |
| `task.artifact` | `DriverEvent::Artifact` |
| `task.input_required` | `DriverEvent::InputRequired` |
| `task.completed` | `DriverEvent::Completed` |
| `task.failed` | `DriverEvent::Failed` |
| `task.cancelled` | `DriverEvent::Cancelled` |

Remote task ID — только correlation metadata. Canonical ID — local `AdapterCore::TaskId`.

## 7. Transport and trust

### Required flow

```text
resolve peer
  → resolve DID document
  → find ANP service endpoint
  → verify endpoint ↔ DID binding
  → verify signing key is authorized for authentication
  → apply pinned DID/key trust policy
  → establish HTTPS/WSS
  → add HTTP Message Signature
  → optional Direct E2EE
  → negotiate profile
```

### Trust modes

- `pinned_did` — production default;
- `pinned_key` — production alternative;
- `insecure_dev` — local tests only.

TOFU запрещён в production. Redirect endpoint требует повторной identity verification.

### Key handling

Private keys не хранятся в YAML, task context, logs или metrics. Конфигурация содержит только secret reference.

## 8. Streaming and recovery

### Profile stream

```text
open task.events(after_seq=N)
  → durable events seq > N
  → live events
```

Remote profile stream имеет собственный cursor. После normalization локальные события записываются в `AdapterCore`, где используется отдельный `CoreEvent.seq` и `ReliableTaskStream`.

```text
remote profile seq
        ↓
driver normalization
        ↓
local CoreEvent.seq + durable store
        ↓
A2A/ACP/downstream reliable streams
```

Если peer не поддерживает profile resume:

- generic messaging fallback;
- либо `stream_unavailable` после принятия сообщения;
- никакого ложного claim о reliable reconnect.

## 9. Driver contract

Полный `AgentDriver` доступен только для selected `agent-connector.anp-task.v1`:

```rust
pub trait AgentDriver: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> DriverCapabilities;
    async fn health(&self) -> Result<(), CoreError>;
    async fn invoke(&self, task_id: TaskId, request: InvokeRequest)
        -> Result<mpsc::Receiver<DriverEvent>, CoreError>;
    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError>;
    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>)
        -> Result<(), CoreError>;
}
```

For generic messaging peer:

```rust
capabilities() = messaging_only
```

Не возвращать `DriverCapabilities { cancellation: true, provide_input: true }`, если profile это не подтверждает.

## 10. Idempotency and errors

`InvokeRequest.idempotency_key` → ANP `operation_id`.

Retry допускается только с тем же `operation_id` и неизменной verified identity.

| Condition | Outcome |
|---|---|
| DID/key/proof failure | `anp_identity_untrusted`, non-retryable |
| Missing task profile | messaging fallback, not task failure |
| Unsupported required capability | `anp_unsupported_capability`, non-retryable |
| Timeout before accepted | retry with same operation ID |
| Disconnect after accepted + profile resume | reconnect after remote seq |
| Disconnect after accepted without resume | status if profile supports it, otherwise `stream_unavailable` |
| Duplicate message | ignore/deduplicate |
| Sequence gap | profile catch-up; if impossible `anp_stream_gap` |
| Local cancel | propagate once if negotiated |
| Remote cancel | emit one `Cancelled` |

## 11. Crate/file plan

```text
crates/anp-transport/
  Cargo.toml
  src/lib.rs
  src/did.rs
  src/wns.rs
  src/signatures.rs
  src/e2ee.rs
  src/rpc.rs
  src/capabilities.rs
  src/negotiation.rs

crates/protocol-anp-profile/
  Cargo.toml
  src/lib.rs
  src/profile.rs
  src/task_v1.rs
  src/mapper.rs
  src/errors.rs

crates/driver-anp-client/
  Cargo.toml
  src/lib.rs
  src/transport_adapter.rs
  src/profile_dispatch.rs
  src/task_v1_driver.rs
  src/messaging_fallback.rs
```

Dependency rules:

- `anp-transport` may depend on pinned upstream `anp`.
- `protocol-anp-profile` must not depend on HTTP, storage or upstream SDK.
- `driver-anp-client` binds transport + profile + `AgentDriver`.
- Workspace default remains unchanged; all ANP crates are optional behind `anp` feature.

## 12. Roadmap

### Phase 0 — foundation

- [x] ADR transport → negotiation → selected profile.
- [x] Upstream qualification: SDK commit, features, MSRV, known gaps.
- [x] Branch `feat/anp-p0-foundation`.
- [x] Feature/module design without upstream runtime coupling.
- [ ] Pin SDK in workspace when transport implementation starts.
- [ ] Security review for production key storage.

### Phase 1 — core streaming

- [x] Add `ReliableTaskStream` module and tests.
- [ ] Wire module into `adapter-core/src/lib.rs`.
- [ ] Migrate A2A mapper to reliable stream.
- [ ] Migrate ACP mapper to reliable stream.
- [ ] Add integration tests against SQLite/Postgres stores.

### Phase 2 — ANP transport

- [ ] Add `anp-transport` crate and feature flag.
- [ ] Implement DID/WNS resolver abstraction.
- [ ] Implement pinned DID/key verification.
- [ ] Implement signed JSON-RPC request envelope.
- [ ] Add optional Direct E2EE behind explicit config.
- [ ] Implement capabilities and negotiation interfaces.
- [ ] Add local ANP reference peer fixture.

### Phase 3 — selected profile and driver

- [ ] Add `protocol-anp-profile` local DTOs and validation.
- [ ] Implement `agent-connector.anp-task.v1` schemas.
- [ ] Implement mapper to `CoreCommand`/`DriverEvent`.
- [ ] Add `driver-anp-client` profile-aware dispatcher.
- [ ] Enable full `AgentDriver` only for selected task profile.
- [ ] Add messaging-only fallback for generic peers.
- [ ] Add remote task correlation and operation idempotency.
- [ ] Add task stream/reconnect tests with a profile-capable peer.

### Phase 4 — interoperability

- [ ] Profile-capable independent peer.
- [ ] Identity negative tests.
- [ ] Capability mismatch tests.
- [ ] Duplicate/gap/reconnect/terminal tests.
- [ ] Retry and cancellation tests.
- [ ] Documentation and compatibility matrix.

## 13. Acceptance criteria

- ANP is not parsed as an A2A dialect.
- Generic ANP peers can connect after identity verification and use messaging fallback.
- Full task operations are enabled only after negotiation of `agent-connector.anp-task.v1`.
- No ANP-specific task store or state machine exists.
- Local `AdapterCore` remains canonical for task lifecycle and event persistence.
- Resume is claimed only when the selected profile provides durable history and cursor semantics.
- Retry never duplicates side-effecting invoke when the same operation ID is unavailable.
- Default builds do not enable ANP.
- Independent profile-capable peer, security negative tests and stream recovery tests pass in CI.
