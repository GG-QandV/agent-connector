# ANP P0 — Execution Handoff

## 1. Что я могу сделать самостоятельно

В GitHub-ветке `feat/anp-p0-foundation` я могу самостоятельно подготовить и закоммитить:

- ADR-ANP-001 с архитектурой `transport → negotiation → selected profile`.
- Pin upstream SDK `anp` на commit `aaca169c3e5b051e48875b023b60364a1dd93022`.
- Workspace feature flag `anp`.
- Независимые локальные crate boundaries.
- Локальные DTO для `agent-connector.anp-task.v1`.
- `ProfileId`, capability model и deterministic profile selection policy.
- `AnpTransport` trait, fake transport и negotiation state machine.
- Mapping tests без реального peer.
- `ReliableTaskStream` в `adapter-core`, если текущие Core/store API позволяют это без изменения persistence contract.
- Документацию и configuration examples.
- CI checks, unit tests и negative tests для profile fallback.

## 2. Что нельзя честно завершить без внешнего решения/входа

### Реальный ANP task driver

Upstream ANP SDK является messaging/security SDK. Он предоставляет DID/WNS, signatures, E2EE, `direct.send`, `anp.get_capabilities` и `anp.negotiate`, но не предоставляет стандартные `invoke`, `status`, `cancel`, `provide_input`, remote task ID, task terminal states, `seq` или resumable event history. [file:56]

Следовательно, я могу реализовать transport и negotiation foundation, но не могу самостоятельно объявить, что произвольный ANP peer поддерживает наш task profile.

### Выбор wire contract profile

Нужно зафиксировать:

```text
profile = agent-connector.anp-task.v1
```

и его exact payload schema. Это архитектурное решение, потому что после публикации оно становится compatibility contract.

### Реальный peer для interoperability

Нужен хотя бы один peer, который:

- проходит ANP identity/authentication;
- отвечает на `anp.get_capabilities`;
- поддерживает `anp.negotiate`;
- объявляет `agent-connector.anp-task.v1`;
- реализует `task.invoke`, status/cancel/input и event replay;
- подтверждает `task.events(after_seq)`.

Без такого peer можно написать mock и тесты, но нельзя доказать interoperability или reliable resume.

### Security policy

Нужно выбрать production trust policy:

- pinned DID;
- pinned key fingerprint;
- trusted DID root/registry;
- allowed endpoint policy;
- key rotation/revocation behavior.

До этого возможен только `insecure_dev` для localhost fixture.

### Product scope

Нужно выбрать P0 launch mode:

```text
outbound only
```

или:

```text
outbound + inbound ANP server
```

Рекомендация: P0 = outbound only; inbound server оставить после первого interoperability pass.

## 3. Инструкция внешнему coding-agent: Phase 0

### Задача

В ветке `feat/anp-p0-foundation` от `main` реализовать ADR и foundation, не заявляя непроверенные task guarantees.

### Шаги

1. Прочитать `docs/protocol-integrations-roadmap.md`, `docs/anp-transport-profile-architecture.md` и upstream research.
2. Создать `docs/adr/ADR-ANP-001.md`.
3. Зафиксировать:
   - ANP = transport/security/discovery/negotiation substrate;
   - `agent-connector.anp-task.v1` = embedded application profile;
   - A2A dialects не меняются;
   - AdapterCore остаётся canonical lifecycle;
   - generic peer без нашего профиля получает messaging-only capability;
   - P0 outbound-only.
4. Проверить root workspace и текущий `rust-toolchain.toml`.
5. Добавить ANP dependency только feature-gated и immutable pinned.
6. Использовать минимальные upstream features (`jwt-pem`, `network`), пока E2EE не включён в отдельный feature.
7. Добавить в документацию SDK revision, MSRV, license и известные gaps.
8. Не реализовывать fake `cancel/status/resume` для generic ANP peer.
9. Запустить `cargo fmt --all`, `cargo check --workspace`, `cargo test --workspace`.

### DoD

- ADR существует и не противоречит фактам upstream.
- Default build не активирует ANP.
- SDK dependency pinned.
- Generic ANP peer не получает ошибочное `streaming/resume=true`.
- Tests проходят.

## 4. Инструкция внешнему coding-agent: Phase 1

### Задача

Реализовать reliable local stream в `adapter-core`.

### Шаги

1. Изучить `AdapterCore::subscribe`, `history`, event publication и store contract.
2. Добавить `ReliableTaskStream` или эквивалентный abstraction.
3. Гарантировать порядок:
   - открыть broadcast receiver;
   - прочитать durable history после `after_seq`;
   - устранить overlap по `seq`;
   - перейти в live mode.
4. Обработать:
   - `seq <= last_seq` — duplicate, skip;
   - `seq == last_seq + 1` — deliver;
   - `seq > last_seq + 1` — durable catch-up;
   - `RecvError::Lagged` — durable catch-up;
   - `Closed` — end;
   - terminal event — end after delivery.
5. Не менять внешний task lifecycle и не добавлять ANP-specific storage.
6. Обновить A2A/ACP stream wrappers только после сохранения совместимости API.
7. Добавить tests:
   - history/live race;
   - lag recovery;
   - sequence gap;
   - duplicate suppression;
   - terminal close;
   - resume from `after_seq`.

### DoD

- Ни одно durable event не теряется из-за broadcast lag.
- Повторное подключение с `after_seq=N` получает только `seq>N`.
- Event order monotonic.
- Existing A2A/ACP tests проходят.

## 5. Инструкция внешнему coding-agent: Phase 3 foundation

### Задача

Не писать сразу real task driver. Сначала реализовать profile-aware transport boundary.

### Crates

```text
crates/anp-transport/
crates/protocol-anp-profile/
crates/driver-anp-client/
```

### `anp-transport`

Реализовать traits:

```rust
trait AnpTransport {
    async fn connect(&self, peer: PeerRef) -> Result<VerifiedAnpPeer, AnpError>;
    async fn capabilities(&self, peer: &VerifiedAnpPeer) -> Result<AnpCapabilities, AnpError>;
    async fn negotiate(
        &self,
        peer: &VerifiedAnpPeer,
        offer: ProfileOffer,
    ) -> Result<NegotiatedProfile, AnpError>;
    async fn send(&self, peer: &VerifiedAnpPeer, message: AnpMessage)
        -> Result<AnpAccepted, AnpError>;
}
```

Добавить:

- fake transport;
- peer identity state;
- capability parsing;
- deterministic profile selection;
- no-auth/insecure mode только для test fixture;
- explicit errors for identity failure and no common profile.

### `protocol-anp-profile`

Определить:

```rust
const PROFILE_ID: &str = "agent-connector.anp-task.v1";
```

DTO:

```text
TaskInvoke
TaskAccepted
TaskStatus
TaskCancel
TaskInputRequired
TaskProvideInput
TaskProgress
TaskArtifact
TaskEvent
TaskCompleted
TaskFailed
TaskCancelled
```

Validation:

- profile ID/version;
- local `task_id`;
- `operation_id`;
- `message_id`;
- `seq` для events;
- terminal event rules;
- payload size limits;
- stable request/input IDs.

Mapper:

```text
TaskInvoke → CoreCommand::Invoke / InvokeRequest
TaskCancel → CoreCommand::Cancel
TaskProvideInput → CoreCommand::ProvideInput
TaskEvent → CoreEvent / DriverEvent
```

### `driver-anp-client`

Реализовать состояния:

```text
Disconnected
Connecting
IdentityVerified
Negotiating
MessagingOnly
TaskProfileReady
Failed
```

Rules:

- `MessagingOnly` нельзя использовать как полноценный `AgentDriver`.
- `TaskProfileReady` создаётся только после exact profile negotiation.
- `invoke` без task profile возвращает explicit `UnsupportedCapability`.
- Не передавать private keys в config/task context/logs.
- Remote task ID хранить как correlation metadata; local Core TaskId остаётся canonical.
- Не обещать resume, если peer profile не объявляет cursor/history contract.

### Tests

- profile selected;
- no common profile → messaging-only;
- identity failure → no fallback to insecure;
- generic ANP peer → no false task capabilities;
- duplicate operation ID;
- profile version mismatch;
- invalid terminal/seq payload;
- reconnect allowed only when selected profile advertises resume.

## 6. Что агент должен вернуть перед real SDK adapter

```text
1. Changed files
2. Cargo commands and results
3. Exact SDK revision/features
4. ADR decision summary
5. Profile schema and compatibility rules
6. Security/trust assumptions
7. Tests added and results
8. Known unsupported behavior
9. Required peer fixture details
10. Open questions requiring product decision
```

## 7. Запрещённые shortcuts

- Не считать успешный `direct.send` выполненной task.
- Не генерировать `seq` из времени или arrival order для remote peer.
- Не объявлять `resume=true` без durable history/cursor.
- Не делать `cancel/status/input` локальными no-op ради прохождения trait.
- Не добавлять ANP-specific event store.
- Не включать SDK default features без проверки MLS/E2EE impact.
- Не принимать DID/key по TOFU в production.
- Не менять A2A SDK/Spec/ACP behavior.
