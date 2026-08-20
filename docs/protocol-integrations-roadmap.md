# Protocol Integrations Roadmap — agent-connector

## Цель

Добавить внешние протоколы через изолированные Rust crates, не меняя канонический lifecycle в `adapter-core`:

```text
external protocol / SDK
        │
protocol-<name>-mapper  ── DTO / semantics
        │
driver-<name>-client    ── outbound transport (если нужен)
        │
protocol-<name>-server  ── inbound transport (если нужен)
        │
AdapterCore ── TaskId, Caller, CoreCommand, CoreEvent, TaskSubscription
        │
durable store + reliable stream / seq / after_seq
```

**Правило:** протоколы не получают собственный task lifecycle, storage или event log. Все адаптеры транслируют в `CoreCommand` и из `CoreEvent`; streaming/resume проходит через общий `ReliableTaskStream`.

## SDK: проверенные точки входа

| Протокол | Rust SDK / основа | Статус для интеграции |
|---|---|---|
| ANP | [agent-network-protocol/anp — `rust/Cargo.toml`](https://github.com/agent-network-protocol/anp/tree/master/rust) | Rust workspace найден; перед подключением зафиксировать commit и проверить public API/license/conformance |
| ANDA | [ldclabs/anda — `Cargo.toml`](https://github.com/ldclabs/anda/blob/main/Cargo.toml) | Rust-native framework; использовать как optional integration, не как replacement Core |
| ANS | [godaddy/ans-sdk-rust — `Cargo.toml`](https://github.com/godaddy/ans-sdk-rust/blob/main/Cargo.toml) | Rust SDK найден; использовать только как identity/discovery client |
| ANIP | [anip-protocol/anip](https://github.com/anip-protocol/anip) | В проверенном корне нет Rust workspace/`Cargo.toml`; не добавлять Rust dependency до появления официального crate или стабильного API |

Перед merge каждого SDK: pin exact Git revision/tag, `cargo deny`/license review, secret scan, conformance/interop test against independent server.

---

## P0 — ANP: Agent Network Protocol

### Кратко

ANP — протокол сетевого agent-to-agent взаимодействия: discovery, сообщение/делегирование задач, capability negotiation и transport bindings. Для `agent-connector` это внешний A2A-style peer protocol, а не замена внутренней модели задач.

### Целевая интеграция

```text
ANP peer/client
  │ request, message, capability, stream
  ▼
crates/protocol-anp-mapper
  │ ANP DTO ↔ CoreCommand/CoreEvent
  ▼
AdapterCore
  │
crates/driver-anp-client         (outbound ANP agent)
crates/protocol-anp-server       (inbound ANP endpoint; optional first release)
```

### TODO

- [x] Создать ADR: scope ANP v1 — inbound, outbound или оба направления. (ADR-ANP-001; P0 = outbound-only)
- [ ] Проверить спецификацию/SDK API, поддерживаемые transports, auth, task IDs, cancellation, streaming и version negotiation.
- [ ] Зафиксировать upstream revision в workspace dependency (`git` + `rev` или опубликованный crate version); не использовать floating branch.
- [x] Создать `crates/protocol-anp-profile` (`agent-connector.anp-task.v1` DTO/validation/mapper) без зависимости от HTTP/storage.
- [x] Определить ANP request/message → `CoreCommand::Invoke`, cancel → `CoreCommand::Cancel`, input → `CoreCommand::ProvideInput` (в `crates/protocol-anp-profile/src/mapper.rs`).
- [ ] Определить `CoreEvent` → ANP event mapping; передавать стабильный `TaskId` и `seq`.
- [x] Использовать общий `after_seq`/`ReliableTaskStream` для reconnect; не создавать отдельную ANP event history. (A2A/ACP mappers мигрированы)
- [x] Реализовать `crates/driver-anp-client` + `crates/anp-transport` (trait + fake) как foundation; подключение в бинарь за feature flag `anp` — при активации драйвера.
- [ ] Добавить capability adapter: remote ANP capabilities → `AgentDescriptor`/маршрутизация Core.
- [ ] Согласовать identity: ANP peer identity → `CallerId`, scopes → `Caller.scopes`.
- [ ] Добавить timeout, retry, idempotency key, cancellation propagation и error taxonomy.
- [ ] Реализовать interop fixture с независимым ANP peer, не только mock SDK.
- [ ] Тесты: invoke, duplicate idempotency, cancel, input-required, stream disconnect/resume, lagged recovery, terminal event.
- [ ] Обновить `docs/protocol-compatibility.md` и добавить `docs/anp-integration.md`.

### Done definition

Внешний ANP peer может создать/продолжить/отменить task; события упорядочены по `seq`, reconnect не теряет события, а SDK revision и compatibility matrix зафиксированы.

---

## P1 — ANDA: Autonomous Networked Decentralized Agent

### Кратко

ANDA — Rust-native decentralized agent framework/infrastructure. Релевантен для Web3, decentralized registry и autonomous services; не должен становиться обязательной основой gateway для обычных HTTP/A2A/ACP сценариев.

### Целевая интеграция

```text
ANDA runtime / Anda Cloud
       │
crates/driver-anda-client        (outbound execution)
crates/protocol-anda-mapper      (optional semantic adapter)
       │
AdapterCore
       │
optional: registry/identity bridge
```

### TODO

- [ ] Создать ADR: конкретный use case ANDA (Anda Cloud, registry, remote agent execution, Web3 identity) и исключения из scope.
- [ ] Проверить публичный Rust API `ldclabs/anda`, license, release policy, supported runtime и compatibility с текущим Rust toolchain.
- [ ] Зафиксировать revision SDK/framework и включить интеграцию только через feature `anda`.
- [ ] Создать `driver-anda-client`; `AgentDriver::invoke/cancel/provide_input` маппить в Anda operations.
- [ ] Сохранить `TaskId` agent-connector как correlation ID; не принимать chain/runtime ID как canonical task ID.
- [ ] Нормализовать ANDA outputs в `DriverEvent` и затем `CoreEvent`.
- [ ] Определить identity bridge: Anda principal/wallet/agent ID → `CallerId` / `AgentId`.
- [ ] Отдельно определить policy для signing, key custody, secrets, fees/gas и audit; ключи не передавать через task context.
- [ ] Проверить semantics finality/retry: не повторять действия с экономическим эффектом без idempotency/transaction state.
- [ ] Добавить contract tests для timeout, remote failure, duplicate delivery, cancellation и restart/recovery.
- [ ] Добавить deploy example с feature flag, отдельным credentials provider и observability.
- [ ] Обновить strategy doc: ANDA = optional decentralized deployment profile.

### Done definition

ANDA agent подключается как обычный `AgentDriver`; lifecycle, audit, retry и streaming подчинены `AdapterCore`, а Web3 credentials изолированы от общего transport layer.

---

## P2 — ANS: Agent Name Service

### Кратко

ANS — identity/discovery/registry слой: разрешает logical agent name в endpoint, metadata, keys/capabilities и trust information. Это control plane, не transport и не task protocol.

### Целевая интеграция

```text
ANS Registry / Rust SDK
          │ resolve / register / verify
          ▼
crates/adapter-discovery-ans
          │
AgentDescriptor + endpoint + identity metadata
          ▼
AdapterCore routing / policy
```

### TODO

- [ ] Проверить API `godaddy/ans-sdk-rust`, security model, namespace ownership, auth, expiry/TTL, revocation и license.
- [ ] Зафиксировать exact SDK revision; подключить за feature `ans`.
- [ ] Создать `crates/adapter-discovery-ans`; не смешивать с protocol mapper.
- [ ] Маппить ANS record → internal `AgentDescriptor`: `AgentId`, endpoint, protocol, capabilities, public key/version/TTL.
- [ ] Добавить resolver cache с TTL, negative-cache и forced refresh при transport failure.
- [ ] Добавить policy: какие ANS namespaces/trust anchors разрешены tenant'у.
- [ ] Реализовать startup registration только как явную opt-in конфигурацию; не регистрировать production agent автоматически.
- [ ] Проверять endpoint/protocol identity после resolution; registry record не является достаточной авторизацией.
- [ ] Тесты: TTL expiry, revoked record, endpoint rotation, lookup failure, cache poisoning protection.
- [ ] Добавить конфигурацию `config/adapter.example.yaml`: resolver URL, allowed namespaces, cache TTL, trust policy.

### Done definition

Core может безопасно находить агент по logical ID через ANS и затем передавать вызов существующему A2A/ANP/HTTP driver без появления ANS-specific task semantics.

---

## P2 — ANIP: Agent-Native Internet/Interface Protocol

### Кратко

ANIP — agent-facing interface layer для websites/services: machine-readable actions, schemas и capability discovery. Использовать как внешний API/interface adapter, а не замену A2A, MCP или внутреннего Core.

### Ограничение Rust SDK

На дату проверки в корне официального `anip-protocol/anip` нет Rust workspace или `Cargo.toml`. Поэтому P2 начинается со specification/conformance integration; собственный Rust client допустим только после API review или появления официального Rust crate.

### Целевая интеграция

```text
ANIP Service / specification
          │ REST / stdio binding
          ▼
crates/driver-anip-client        (после API stabilization)
crates/protocol-anip-mapper      (если ANIP task semantics нужны inbound)
          │
AdapterCore
```

### TODO

- [ ] Проверить `SPEC.md`, `proto`, schemas, conformance tests и поддерживаемые официальные bindings в `anip-protocol/anip`.
- [ ] Зафиксировать ADR: ANIP нужен как outbound client к agent-native services, inbound facade или оба режима.
- [ ] Не писать Rust SDK до определения stable API surface и conformance target.
- [ ] Если нужен MVP раньше official Rust SDK: реализовать тонкий HTTP/JSON client в `driver-anip-client`, без копирования всего SDK/protocol runtime.
- [ ] Маппить discovered ANIP actions в internal skill/agent capability model, но не исполнять action без policy authorization.
- [ ] Маппить ANIP calls в `InvokeRequest`; обеспечить idempotency key, deadline, cancellation и output/artifact mapping.
- [ ] Для stateful/streaming actions использовать общий `TaskId`, `seq`, `after_seq` и `ReliableTaskStream` только если ANIP binding это гарантирует.
- [ ] Добавить OAuth/user-delegation, consent and confirmation hooks для write/payment/high-risk actions.
- [ ] Добавить allowlist origins, SSRF protection, schema validation, rate limit и audit.
- [ ] Создать conformance fixtures against ANIP examples/contract tests.
- [ ] Документировать compatibility boundaries: ANIP = service interface; A2A/ANP = agent task delegation; MCP = tools/context.

### Done definition

Agent-connector вызывает безопасно разрешённые ANIP actions через тонкий driver, с policy enforcement и конформностью к официальным schemas/tests; нет непроверенной vendor-specific Rust SDK зависимости.

---

## Общие задачи и порядок

### Обязательные общие prerequisites

- [ ] Завершить `ReliableTaskStream`: durable history + `seq` + `after_seq` + recovery после `Lagged`/gap.
- [ ] Единая таблица mapping: protocol status/error/capability ↔ `CoreCommand`, `CoreEvent`, `PublicError`, `TaskState`.
- [ ] Feature flags: `anp`, `anda`, `ans`, `anip`; default build остаётся без optional SDK.
- [ ] E2E test harness с mock и independent upstream fixture на каждый protocol.
- [ ] Supply-chain checklist: pin, license, advisories, API compatibility, release health, maintainer/governance review.
- [ ] Metrics: protocol, peer, task_id, seq, reconnect count, latency, terminal state; без секретов/PII.

### Порядок реализации

1. **P0 ANP** — ближайшая интеграция для cross-agent networking.
2. **P1 ANDA** — только после формулировки decentralized/Web3 use case.
3. **P2 ANS** — discovery/control-plane integration, может идти параллельно с ANIP после P0.
4. **P2 ANIP** — spec-first; Rust driver только после подтверждения стабильного interface/binding.
