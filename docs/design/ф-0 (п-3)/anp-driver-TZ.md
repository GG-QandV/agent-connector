# ТЗ: driver-anp-client — входные данные для архитектора/сеньора

> Этот документ — **техническое задание**, а не архитектура и не спека. Он
> собирает все установленные факты об upstream ANP, контракты agent-connector
> и открытые вопросы, чтобы архитектор/сеньор на их основе сформулировал
> **архитектуру и/или спеку драйвера** (ADR-ANP-001 + дизайн-документ).
> Код драйвера в этом ТЗ **не реализуется**.

- **Статус:** черновик на утверждение (вход для архитектора/сеньора)
- **Дата:** 2026-08-20
- **Затрагивает (целевые):** `crates/driver-anp-client`, `crates/protocol-anp-mapper`,
  `adapterd`/`adapterctl` (`AgentTransportConfig`), корневой `Cargo.toml`
- **Связанные документы:**
  - `docs/design/ф-0 (п-3)/anp-phase3-upstream-research.md` — доказательная база об upstream (immutable SHA, paths, lines)
  - `docs/design/ф-0 (п-3)/anp-phase3-agent-input-guide.md` — исходные вопросы агента
  - `docs/anp-p0-integration-spec.md` — P0-спека интеграции (scope, trust, roadmap)
  - `docs/design/adr-0003-a2a-acp-client-drivers.md` — прецедент outbound-драйверов
  - `docs/design/TZ-driver-a2a-wire-format.md` — формат-прецедент ТЗ проекта

---

## 1. Проблема и контекст

P0-интеграция (`docs/anp-p0-integration-spec.md`) ставит ANP как **опциональный**
open-web профиль: A2A остаётся task-delegation wire, ANP даёт DID/WNS identity +
discovery + защищённый peer-канал. Адаптация — outbound: `agent-connector`
вызывает удалённого ANP peer как обычный `AgentDriver`.

Upstream-исследование (`anp-phase3-upstream-research.md`) **доказательно показало**:
Rust SDK `anp` (commit `aaca169c`) — это crypto/identity/messaging-фундамент,
**а не task-delegation клиент**. Операций invoke/status/cancel/stream/resume на
протокольном уровне нет. Полноценный `AgentDriver`-контракт поверх чистого
upstream не реализуется без прикладного контракта поверх `direct.send` или без
сокращения P0-scope.

**Задача архитектора:** на базе этого ТЗ выбрать траекторию (полный driver с
прикладным контрактом vs сокращённый P0), зафиксировать решения в ADR-ANP-001
и выдать спеку драйвера.

---

## 2. Цель ТЗ

Архитектор/сеньор должен получить из этого документа всё необходимое и выдать:

1. **ADR-ANP-001** — решения по §6 (обязательные open questions), включая
   суперсессию «ANP out-of-scope» из A2A-стратегии (только для опционального
   identity/discovery/transport профиля; A2A wire не меняется).
2. **Архитектуру драйвера** — границы, структура крейтов, интерфейсы, поток
   данных (см. §4 как обязательные пункты дизайна).
3. **Спеку драйвера** — контракт driver ↔ upstream, конфигурация, error/retry
   маппинг, поведение stream/reconnect, observability (§5).

ТЗ НЕ требует:
- написания кода драйвера;
- создания `protocol-anp-mapper` (только проектное описание, если архитектор
  решит его вводить);
- публичного inbound ANP-сервера (дефер из P0-спеки §3).

---

## 3. Установленные факты (доказательная база)

Все ссылки на SHA и paths — из `anp-phase3-upstream-research.md` (иммутабельно).

| # | Факт | Статус для драйвера |
|---|---|---|
| 1 | SDK `anp` 0.9.4, commit `aaca169c`, MSRV 1.88, MIT, crates.io максимум 0.9.3 | Пиновать по `rev = <SHA>`, не по тегу/версии |
| 2 | SDK: identity (did:wba), HTTP Message Signatures, WNS resolution, direct E2EE (X3DH-like), transport-neutral `{method,params}` | ✅ готово «из коробки» |
| 3 | JSON-RPC 2.0 envelope строит вызывающий слой; методы `direct.send` / `direct.incoming` / `group.send` / `anp.get_capabilities` / `anp.negotiate` | базис прикладного контракта |
| 4 | Idempotency `direct.send` по `(sender_did, target.did, method, operation_id)` + `message_id` dedup; повторный invoke с тем же key **не создаёт** новую remote task | ✅ воспроизводимо для `InvokeRequest.idempotency_key` |
| 5 | Ответ `direct.send` = `accepted=true` (**только** «принял ingress», нет remote task ID, нет статуса) | драйверу неоткуда взять remote task ID |
| 6 | `invoke/status/cancel/provide_input/stream` на protocol-level **отсутствуют** | не реализуемо без прикладного контракта |
| 7 | Push — Notification `direct.incoming`; **нет** `seq`/cursor/`event_id`/retention/`Last-Event-ID` | поток **non-resumable** (guide это допускает, фиксируется явно) |
| 8 | Terminal-состояния (Completed/Failed/Cancelled) — прикладной payload, не протокол | определяются нашим контрактом |
| 9 | Trust: DID resolution + `authentication` KID check (нет TOFU); `pinned_did`/`pinned_key` | ✅ P0-совместимо (спека §8) |
| 10 | Локальный reference peer: python auth server + interop fixtures | ✅ для integration-тестов |
| 11 | Error enums: `WnsError`, `HttpSignatureError`, `DirectE2eeError`, `anp.invalid_request_id`, `anp.batch_not_supported`, HTTP 429/5xx | маппинг по таблице §7 исследования |

---

## 4. Обязательные пункты архитектуры/спеки

Архитектор обязан раскрыть в своём документе каждый пункт ниже.

### 4.1 Границы системы (не ломать инварианты P0-спеки §2)

```text
Inbound protocol server        Outbound driver
protocol-a2a-server            driver-anp-client (ЦЕЛЬ)
protocol-acp-runtime                     │
        │                  ┌────────────┴───────────┐
        └──── mapper / AgentDriver ─────┘ (переиспользуется)
                    │
              AdapterCore
   CoreCommand / DispatchResult / CoreEvent / TaskSubscription
                    │
         task stores: memory / SQLite / PostgreSQL
```

Требования:
- `AdapterCore` остаётся единственным владельцем `TaskId`, lifecycle, durable
  events, idempotency и downstream streaming. ANP **не** добавляет свой task
  store / event log / state machine (P0-спека §2, §6).
- ANP DTO (если вводится `protocol-anp-mapper`) — локальные стабильные типы;
  upstream SDK API изолируется в `driver-anp-client`, не протекает в Core/сторы
  (P0-спека §4).
- Driver реализует `adapter_core::AgentDriver` (см. §4.2).

### 4.2 Контракт AgentDriver

```rust
pub trait AgentDriver: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> DriverCapabilities;   // { cancellation, provide_input }
    async fn health(&self) -> Result<(), CoreError>;
    async fn invoke(&self, task_id: TaskId, request: InvokeRequest)
        -> Result<mpsc::Receiver<DriverEvent>, CoreError>;
    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError>;
    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError>;
}
```

`DriverEvent`: `Accepted | Progress{message,percent} | Artifact(ArtifactRef) |
InputRequired(InputRequest) | Completed(Vec<Part>) | Failed(PublicError) | Cancelled`.

Решение, которое обязан зафиксировать архитектор — **семантика каждого события
в контексте ANP** (см. §5.2).

### 4.3 Identity и trust (P0-спека §8, исследование §4)

- `pinned_did` (default) / `pinned_key` / `insecure_dev` (только тесты).
- Проверка `authentication` KID (не `assertionMethod`/`keyAgreement`) —
  `HttpSignatureError::VerificationMethodNotAuthorizedForAuthentication`.
- `verified peer DID → CallerId("anp:<did>")`; `validated capabilities →
  scopes`; `configured local peer → AgentId`.
- TOFU в production запрещён; перенаправления без revalidation запрещены.
- Хранение ключей — **вне** YAML (secret provider; P0-спека §5). Спекой
  определяется формат ссылки (env/файл/secret manager).

### 4.4 Прикладной контракт поверх `direct.send` (главное решение)

Архитектор обязан выбрать ОДНУ из траекторий и оформить в ADR-ANP-001:

- **(A) Прикладной контракт поверх `direct.send`**: определить
  application-payload/мета-схему `{action: invoke/status/cancel, task_ref,
  input, context, deadline, idempotency_key}` + ответные сообщения
  (progress/result через `direct.incoming`). Это позволяет реализовать
  полный `AgentDriver`-API. Цена: контракт не стандартизован upstream —
  совместимость только с peer'ами, реализующими НАШУ схему.
- **(B) Сокращённый P0**: driver = identity/discovery/messaging (события
  `Accepted`/`Completed(Vec<Part>)` из ответных сообщений по простой схеме),
  `capabilities() = { cancellation: false, provide_input: false }`,
  `cancel`/`provide_input` → `CoreError::InvalidRequest` (как driver-mcp).
- **(C) Отложить driver**: P0-фаза остаётся на research/ADR; driver не пишем.

Для (A) и (B) архитектор даёт: схему `operation_id` ↔ `InvokeRequest.idempotency_key`
(идемпотентный retry обязан переиспользовать тот же `operation_id`, иначе —
новое сообщение, исследование §5/§7) и mapping `task_id ↔ operation_id/message_id`.

### 4.5 Streaming и reconnect (non-resumable)

- Отсутствие cursor/resume (факт §3) → поток **non-resumable**, заявляется явно
  (P0-спека §6, §13). Запрещено изобретать «надёжный resume» из порядка прихода.
- Дизайн поведения при disconnect: если прикладной контракт даёт авторитетный
  remote status — опросить и доэмитить терминальное состояние; иначе —
  `Failed(PublicError { code: "stream_unavailable", retryable: true })`
  (P0-спека §10).
- Duplicate suppression, gap handling — в терминах нашего локального
  `CoreEvent.seq` (драйверный cursor ≠ клиентский `CoreEvent.seq`).
- Требуется ли предварительный `ReliableTaskStream` в adapter-core — решает
  архитектор со ссылкой на P0-спека §2 («Streaming prerequisite»).

### 4.6 Configuration (аналог A2aClient в `AgentTransportConfig`)

Архитектор определяет вариант конфига (пример из P0-спеки §5):

```yaml
agents:
  - id: research-anp-peer
    driver: anp
    endpoint: https://peer.example/.well-known/anp
    anp:
      peer_did: did:wba:peer.example
      expected_key_ids: ["did:wba:peer.example#key-1"]
      allowed_protocol_versions: ["<validated-version>"]
      connect_timeout_ms: 5000
      request_timeout_ms: 30000
      stream_idle_timeout_ms: 60000
      reconnect_attempts: 8
      require_e2ee: true
      trust_policy: pinned_did
```

Требования: секреты (private keys, client credentials) — только ссылки на
secret provider; никогда в YAML/теле/теляметрии. `https`-check как у A2A/MCP
(`allow_http_development` для локали).

### 4.7 Зависимости и feature-флаг

- `anp-sdk` пин: `git = .../AgentConnect, rev = aaca169c...`, `optional = true`,
  feature за `default-features` под выбор архитектора (identity-only: `["jwt-pem","network"]`,
  полный: default). Никогда `branch = "master"`.
- Новые крейты (`driver-anp-client`, при необходимости `protocol-anp-mapper`) —
  в workspace members; feature `anp` на `adapterd`/`adapterctl`.
- `cargo deny`/dependency audit/license review на pinned graph (P0-спека §12
  Phase 0).

### 4.8 Observability (P0-спека §11)

Поля: `protocol="anp"`, `local_task_id`, `remote_task_id_hash`, `peer_did_hash`,
`endpoint_host`, `negotiated_version`, `stream_seq`, `reconnect_attempt`,
`capability_resume`, `terminal_state`, `latency_ms`.
Метрики: `anp_invocations_total{outcome}`, `anp_identity_verification_total{outcome}`,
`anp_stream_reconnects_total`, `anp_stream_gap_total`, `anp_request_duration_seconds`.
Запрещено логировать: контент задач, credentials, полные DID-документы,
расшифрованный E2EE, приватные ключи, raw auth-заголовки.

---

## 5. Контракт driver ↔ upstream (требует спецификации)

### 5.1 Маппинг ошибок (факты §3/№11 → P0-спека §10)

| Условие | Поведение | `PublicError` / outcome |
|---|---|---|
| DID/key/proof validation fails | retry запрещён до смены конфига | `anp_identity_untrusted`, non-retryable |
| Unsupported version/capability | fail до invoke | `anp_unsupported_capability`, non-retryable |
| Connection timeout до accepted | retry только при гарантии idempotency (тот же `operation_id`) | transport error, retryable |
| Disconnect после accepted, без resume | опросить авторитетный статус; иначе fail | `stream_unavailable`, retryable |
| Duplicate event | ignore | — |
| Remote cancel (если прикладной контракт) | эмитить один раз | `Cancelled` |
| Local cancel | propagate once; persist Core cancellation | normal cancellation lifecycle |
| `provide_input` без поддержки | — | `CoreError::InvalidRequest` (или по выбору §4.4) |

Retries **никогда не дублируют** side-effecting invoke, если peer не принимает
тот же `idempotency_key` и identity не изменилась.

### 5.2 Семантика `DriverEvent` для ANP

Архитектор фиксирует для каждой траектории (§4.4):

- `Accepted` — когда эмитится (успех `direct.send` = «принято в ingress»).
- `Progress`/`Artifact` — источник (ответные сообщения прикладного контракта).
- `Completed(Vec<Part>)` — из какого сообщения берутся части (`Part::{Text,Json,FileRef}`).
- `Failed(PublicError)` — `code`/`retryable` по §5.1.
- `InputRequired` — только если прикладной контракт поддерживает mid-call input
  (иначе отсутствует, capabilities=false).
- `Cancelled` — источник.

### 5.3 Idempotency-контракт

- `InvokeRequest.idempotency_key` → `operation_id` (стабильный формат).
- Повторный invoke с тем же ключом: не создавать новую remote task
  (доказано §3/№4); вернуть результат прежнего принятого сообщения или
  отложить dedup на приёмник.
- Поведение при провале до получения ответа: retry возможен только с тем же
  `operation_id` (исследование §7 «Retry invoke»).

---

## 6. Открытые вопросы (обязательны к решению в ADR-ANP-001)

| # | Вопрос | Варианты (рекомендация — первый) |
|---|---|---|
| 1 | Траектория driver (A/B/C из §4.4) | (A) прикладной контракт / (B) сокращённый P0 / (C) отложить |
| 2 | Конкретный peer + сценарий interop | один реальный ANP peer (требование P0-спека §12 Phase 0); до выбора — не начинать код |
| 3 | Нужен ли `protocol-anp-mapper` отдельным крейтом или mapping в driver | отдельный крейт (P0-спека §4) — если вводится ANP DTO |
| 4 | Схема `operation_id`/`message_id` и их связь с `TaskId` | формат + где хранить correlation (CoreEvent metadata) |
| 5 | Схема ответных сообщений (progress/result/error) для (A) | JSON-schema прикладного payload |
| 6 | `ReliableTaskStream` — обязателен ли до драйвера | да (P0-спека §2) — если вводится stream |
| 7 | E2EE в P0 (`require_e2ee: true`) или только transport-protected | default transport-protected; E2EE как опция |
| 8 | Key storage интерфейс | secret provider ссылка (env/файл), формат |
| 9 | Feature-flag composition `default-features` SDK | identity-only (`jwt-pem`,`network`) vs полный |
| 10 | Версии/профили ANP для `allowed_protocol_versions` | фактический набор из `anp.get_capabilities`/`anp.negotiate` тестового peer |

---

## 7. Критерии готовности (что считается выполненным ТЗ)

Архитектура/спека считается готовой, когда все верно:

- [ ] ADR-ANP-001 принят; conflict с A2A-стратегией разрешён (только опциональный профиль).
- [ ] Выбрана траектория §4.4 и выбран один peer + сценарий interop.
- [ ] Дизайн покрывает все пункты §4 (границы, AgentDriver, trust, контракт, stream, config, deps, observability).
- [ ] Контракт §5 (error/retry, DriverEvent, idempotency) специфицирован для выбранной траектории.
- [ ] Все open questions §6 имеют решения и записаны в ADR.
- [ ] Код драйвера в ТЗ не реализуется (это отдельная фаза после утверждения ADR).

---

## 8. Проверяемость и источники

- Установленные факты — только из `docs/design/ф-0 (п-3)/anp-phase3-upstream-research.md`
  (локальные клоны `/tmp/anp-upstream` @ `aaca169c`, `/tmp/anp-proto` @ `6c6aa9b8`).
- P0-спека: `docs/anp-p0-integration-spec.md`.
- Контракты Core: `crates/adapter-core/src/lib.rs` (`AgentDriver`, `DriverEvent`,
  `CoreError`), `crates/adapter-model/src/lib.rs` (`InvokeRequest`, `Part`,
  `InputRequest`, `PublicError`, `ArtifactRef`, `DriverCapabilities`),
  `crates/adapterd/src/config.rs` (`AgentTransportConfig`).
- Прецедент драйвера: `crates/driver-a2a-client/src/lib.rs`, `docs/design/TZ-driver-a2a-wire-format.md`.