# ТЗ: адаптация driver-a2a-client под управление двумя wire-форматами A2A

> **ЗАМЕНЕНО:** объединено в единое ТЗ
> `ACP-A2A_gateway/docs/TZ-a2a-dialects-gateway-adapter.md` → Раздел 2. Этот файл
> сохранён как исходник, правки — в объединённом документе.

- **Статус:** заменено (см. выше). Код не менялся.
- **Дата:** 2026-08-17
- **Затрагивает:** `crates/driver-a2a-client` (только этот crate); `adapterd-config`
  (`A2aClient` вариант — одно новое поле). Не затрагивает `adapter-core`,
  `protocol-a2a-server`, `protocol-acp-runtime`.
- **Связанные документы:** `docs/design/adr-0003-a2a-acp-client-drivers.md`,
  `docs/design/Коммент_драйвера_полностью_реализова.md`.

---

## 1. Проблема

`driver-a2a-client` написан под **один** wire-формат — JSON-RPC слой официального
SDK `a2a-rs` (метод `SendMessage`, proto-сериализация полей). Живая проверка
показала, что шлюз `ACP-A2A_gateway` (`/home/gg/projects/AGENTS/ACP-A2A_gateway`)
реализует **другой** wire-формат (метод `message/send`, плоский Task,
lowercase-состояния), который драйвер не понимает.

### Ключевой факт: «стандарта» на wire два

Официальный SDK `a2a-rs` сам предоставляет **два** wire-представления, и они
различаются:

| Слой SDK | Wire                             | Источник                                                                                                                                 |
| -------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| REST     | путь `/message:send`             | `a2a-client/src/rest.rs:17` (`REST_SEND_MESSAGE_PATH`)                                                                                   |
| JSON-RPC | метод `SendMessage` + proto-поля | `a2a/src/jsonrpc.rs:138` (`methods::SEND_MESSAGE`) + `a2a-server/src/jsonrpc.rs:73` (`protojson_conv::from_value::<SendMessageRequest>`) |

Шлюз `ACP-A2A_gateway` реализует третий вид — **семантический** JSON-RPC по
A2A-спеке (метод `message/send`):
`gatewayd/src/transport_http.rs:254` (`"message/send" =>`), свои типы в
`protocol/src/a2a.rs` (`SendMessageParams { message }`).

Итог: **три** wire-вида, из них два релевантны для драйвера:

1. **A2aSdkJsonRpc** — метод `SendMessage`, proto-поля (`TASK_STATE_*`, `{text}`,
   `ROLE_USER`). Это формат нашего `protocol-a2a-server` (собран на том же SDK).
2. **A2aSpecJsonRpc** — метод `message/send`, семантические поля
   (`completed`, `{kind: text}`, `user`, плоский Task). Это формат шлюза.

Драйвер должен уметь работать с обоими — по выбору на endpoint-уровне.

---

## 2. Цель

Сделать `driver-a2a-client` **wire-формат-нейтральным**: уметь отправлять запрос
и разбирать ответ в любом из двух форматов, выбираемом конфигурацией агента.
Это позволит одному драйверу подключаться и к нашему `adapterd` (SDK-формат),
и к шлюзу `ACP-A2A_gateway` (spec-формат) — без дублирования кода.

---

## 3. Точное сравнение двух форматов (обоснование по коду)

### 3.1 Метод запроса

|                 | SDK-формат               | Spec-формат                                          |
| --------------- | ------------------------ | ---------------------------------------------------- |
| JSON-RPC method | `"SendMessage"`          | `"message/send"`                                     |
| Источник        | `a2a/src/jsonrpc.rs:138` | `ACP-A2A_gateway/gatewayd/src/transport_http.rs:254` |

> **Почему нельзя «угадать» на отправку:** сервер принимает ровно одно имя
> метода. `SendMessage` → наш adapterd; `message/send` → шлюз. Неверный выбор =
> `method not found` (подтверждено живым тестом: наш adapterd вернул
> `-32601 METHOD_NOT_FOUND` на `message/send`).

### 3.2 Параметры запроса `message`

Обе стороны принимают объект `{ message: { role, parts } }` на верхнем уровне.
Различие — внутри `part` и `role`:

| Поле          | SDK-формат                                                                        | Spec-формат                                           |
| ------------- | --------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `role`        | `"ROLE_USER"`                                                                     | `"user"`                                              |
| part text     | `{ "text": "..." }`                                                               | `{ "kind": "text", "text": "..." }`                   |
| part resource | `{ "url": ..., "media_type": ... }`?                                              | `{ "kind": "resource", "uri": ..., "mimeType": ... }` |
| Источник      | protojson (SDK десериализация: `unknown field \`kind\`` на `{kind}` — живой тест) | `protocol/src/a2a.rs` (`Message`, `Part` с `kind`)    |

> Живое подтверждение несовместимости SDK-стороны: наш adapterd на part
> `{ "kind": "text" }` вернул `-32700 PARSE_ERROR: unknown field \`kind\``.
> Наоборот, шлюз ожидает именно `{ kind, text }` (его десериализатор).

### 3.3 Ответ `SendMessageResponse` / Task

| Аспект       | SDK-формат                                             | Spec-формат                               |
| ------------ | ------------------------------------------------------ | ----------------------------------------- |
| Обёртка      | `{ "task": { ... } }`                                  | плоский `{ id, context_id, status, ... }` |
| Состояние    | `"TASK_STATE_COMPLETED"`                               | `"completed"`                             |
| message.role | `"ROLE_AGENT"`                                         | `"agent"`                                 |
| part         | `{ "text": ... }`                                      | `{ "kind": "text", "text": ... }`         |
| Источник     | `a2a/src/types.rs` (`TaskState` serde: `TASK_STATE_*`) | `transport_http.rs` (плоский Task)        |

> Живое подтверждение: драйвер (ищет `result.task`) получил от шлюза плоский
> Task в `result` → ошибка `A2A response missing task: no task in result`.
> И наоборот, `a2a::TaskState` не распарсит `"completed"` (ожидает
> `"TASK_STATE_COMPLETED"`).

---

## 4. Проектирование

### 4.1 Конфигурация: новое поле `wire_format`

В `A2aClientConfig` (crates/driver-a2a-client) добавить:

```rust
/// Wire-формат A2A-агента.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum A2aWireFormat {
    /// Официальный a2a-rs SDK JSON-RPC слой: метод SendMessage, proto-поля.
    #[default]
    Sdk,
    /// Семантический A2A JSON-RPC (шлюз ACP-A2A_gateway): метод message/send.
    Spec,
}
```

В `adapterd-config` вариант `A2aClient` — поле:

```yaml
- id: hermes
  driver: a2a-client
  endpoint: https://agentmesh-labs.mnemostroma.com/agents/hermes/rpc
  wire_format: spec          # sdk (default) | spec
```

Десериализация: `#[serde(default)]` + enum строк (`sdk`/`spec`). Отсутствие
поля → `sdk` (обратная совместимость, текущее поведение).

### 4.2 Внутренняя структура: два wire-модуля

Разбить сериализацию/парсинг на два изолированных набора, без общих веток
`if wire_format` по всему коду:

```
crates/driver-a2a-client/src/
├── lib.rs            # AgentDriver impl, dispatch по wire_format
├── wire/mod.rs       # trait A2aWire { method(); build_message(); parse_task() }
├── wire/sdk.rs       # A2aSdkWire  — SendMessage + protojson + a2a::Task
└── wire/spec.rs      # A2aSpecWire — message/send + семантические поля
```

```rust
trait A2aWire: Send + Sync {
    fn jsonrpc_method(&self) -> &'static str;
    fn build_message(&self, input: &[Part], task_id: Option<&TaskId>) -> Value;
    /// Возвращает нормализованный Task (единый внутренний тип), либо ошибку.
    fn parse_response(&self, result: &Value) -> Result<NormalizedTask, A2aClientError>;
}
```

`A2aClientDriver` держит `wire: Box<dyn A2aWire>` (или enum), выбирается в
`A2aClientDriver::new(config)`.

### 4.3 Единый внутренний тип `NormalizedTask`

Чтобы `AgentDriver`-логика (invoke/cancel/provide_input) не знала про форматы,
оба wire-парсера возвращают **один** внутренний тип:

```rust
struct NormalizedTask {
    id: String,
    state: NormalizedState,      // Working | InputRequired | Completed | Failed | Cancelled
    message: String,
    output_parts: Vec<Part>,
}
```

- `A2aSdkWire::parse_response` — десериализует `a2a::Task` (типизированно),
  маппит `a2a::TaskState` → `NormalizedState`.
- `A2aSpecWire::parse_response` — парсит плоский Value вручную
  (`completed`/`failed`/`canceled`/`inputRequired` lowercase),
  части `{kind,text}` → `Part`.

`invoke()` работает только с `NormalizedTask` → `DriverEvent`.

> **Обоснование единого типа:** `AgentDriver::invoke` обязан выдавать
> `DriverEvent` — один набор событий. Формат wire — деталь транспорта,
> не должна протекать в логику драйвера. Тип `NormalizedTask` — точка
> нормализации, где оба формата сходятся.

### 4.4 cancel / provide_input

- `cancel` — локальный `CancellationToken` (формат-независим) + best-effort
  HTTP-cancel. Для SDK-формата сигнал отмены — отдельный вызов (в спецификации
  A2A — `tasks/cancel`; у шлюза — отдельная семантика). Уточнить в реализации.
- `provide_input` — повторный `message/send` с тем же `task_id`/`taskId` через
  выбранный wire. Имена полей зависят от формата (`task_id` vs `taskId`),
  поэтому `build_message` уже принимает `task_id` и сам решает, куда его класть.

---

## 5. Маппинг ошибок

| Ситуация                                                                   | Результат                                                                          |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| сервер вернул `error` (JSON-RPC)                                           | `DriverEvent::Failed` c кодом `a2a_remote_error`                                   |
| `result` нет / нет task                                                    | `DriverEvent::Failed` `a2a_no_task`                                                |
| неподдерживаемый wire (будущие форматы)                                    | ошибка в `new()`, агент не стартует                                                |
| несовпадение формата (отправлен `SendMessage`, сервер ждал `message/send`) | `-32601 METHOD_NOT_FOUND` → `DriverEvent::Failed`; в логе — hint про `wire_format` |

---

## 6. Тесты

1. **Unit: sdk-wire** — `build_message` даёт `SendMessage` + `ROLE_USER` +
   part `{text}`; `parse_response` из `{task:{...}, TASK_STATE_COMPLETED}` → `NormalizedTask`.
2. **Unit: spec-wire** — `build_message` даёт `message/send` + `user` +
   part `{kind,text}`; `parse_response` из плоского `{completed}` → `NormalizedTask`.
3. **Contract (mock axum server)** — два mock-сервера (SDK-формат и spec-формат),
   полный `invoke` → `Completed` через каждый wire. (по образцу
   `tests/contract.rs` из ADR-0003).
4. **Живой E2E** (после сборки, вручную/скриптом):
   - наш `adapterd` (SDK-формат) ← `driver-a2a-client` c `wire_format: sdk`
   - шлюз hermes (spec-формат) ← `driver-a2a-client` c `wire_format: spec`

DoD: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`.

---

## 7. Объём работ

- Мидл-уровень, ~0.5–1 день (код + unit/contract-тесты).
- Меняет только `driver-a2a-client` + одно поле в `adapterd-config`.
- Не меняет: `adapter-core`, `protocol-a2a-server`, `protocol-acp-runtime`,
  `driver-acp-client`.

---

## 8. Что остаётся вне скоупа

- Стриминг (`SendStreamingMessage` / SSE) — в обеих форматах остаётся
  нереализованным (как в текущем драйвере), фиксируется в комментарии.
- REST-слой a2a-rs (`/message:send`) — не добавляем (у нас JSON-RPC).
- Третьи форматы (прочие агенты) — при появлении добавляются новым `A2aWire`.
