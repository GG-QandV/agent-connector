# ADR-0001: MCP dynamic capabilities — list_changed, prompts/resources, input_required

- **Status:** Proposed
- **Date:** 2026-08-16
- **Project:** agent-connector
- **Version context:** 0.6.7
- **Affects:** `adapter-core`, `driver-mcp`, `protocol-acp-runtime`, `protocol-a2a-mapper`

## Контекст

`driver-mcp` (rmcp 0.8.5) уже реализует статический жизненный цикл MCP-клиента:
discovery через `list_tools` при `connect_stdio`, invoke через
`send_request_with_option` + `RequestHandle`, progress через встроенный
`ProgressDispatcher`, cancel через `CancellationToken` (см. README §Drivers).

Три следующих задачи — не про protocol API (он уже верифицирован построчным
чтением исходников rmcp), а про архитектурные решения на стыке `driver-mcp` и
`adapter-core`. Ниже — по одному решению на каждую задачу.

---

## Решение 1: hot-update capabilities после `tools/list_changed`

### Проблема

`AgentRegistry` (`crates/adapter-core/src/lib.rs`) хранит агентов как
`DashMap<AgentId, Arc<RegisteredAgent>>`. `RegisteredAgent.skills: Vec<String>`
заполняется один раз в `RegisteredAgent::new()` при старте и замораживается
внутри `Arc` — `register()`, `get()`, `resolve()` только читают/клонируют
`Arc`, никто не пишет в `skills` после старта.

MCP-серверы могут слать `notifications/tools/list_changed` в любой момент
работы сессии — набор доступных tools реально динамический, а не фиксируется
при подключении.

### Решение

Не менять публичный контракт `RegisteredAgent`/`AgentRegistry`. Вместо этого
заменить тип поля `skills`:

```rust
pub struct RegisteredAgent {
    pub id: AgentId,
    skills: Arc<tokio::sync::RwLock<Vec<String>>>,   // было: pub skills: Vec<String>
    pub driver: Arc<dyn AgentDriver>,
    pub limits: AgentLimits,
    permits: Arc<Semaphore>,
    queue_permits: Arc<Semaphore>,
}

impl RegisteredAgent {
    pub async fn skills(&self) -> Vec<String> {
        self.skills.read().await.clone()
    }
    pub async fn has_skill(&self, skill: &str) -> bool {
        self.skills.read().await.iter().any(|s| s == skill)
    }
    pub async fn update_skills(&self, new: Vec<String>) {
        *self.skills.write().await = new;
    }
}
```

`AgentRegistry::resolve()` становится `async fn` там, где раньше был
синхронный `entry.skills.iter().any(...)`. Blast radius — один `async` в
сигнатуре `resolve()`, публичный API `register`/`get`/`agents` не меняется.

### Триггер обновления

`McpDriver` получает `Weak<AgentRegistry>` в `connect_stdio()` (не `Arc`, чтобы
не создавать цикл владения между Registry → RegisteredAgent → Driver →
Registry). `ClientHandler` дополняется обработчиком
`notifications/tools/list_changed` (отдельно от `on_progress`), который:

1. Вызывает `discover_tools()` повторно, с полной пагинацией (как при
   первичном подключении).
2. Резолвит `Weak<AgentRegistry>` → `Arc`, находит себя по `self.id`, зовёт
   `registered_agent.update_skills(new_list).await`.
3. Если `Weak::upgrade()` вернул `None` (Registry уже сброшен) — молча
   игнорирует, это штатное завершение shutdown, не ошибка.

### Альтернативы, которые отклонены

- **`ArcSwap<Vec<String>>`** вместо `RwLock` — быстрее на read-heavy паттерне,
  но добавляет новую внешнюю зависимость ради marginal выигрыша; `RwLock`
  из tokio уже используется в проекте (`tool_names` в `driver-mcp`).
- **Полная пересборка `RegisteredAgent` и re-register в Registry** при
  каждом `list_changed` — отклонено: рвёт identity объекта, ломает любой код,
  держащий старый `Arc<RegisteredAgent>` (например, активные task).

---

## Решение 2: prompts/resources — не мапить в AgentDriver contract

### Проблема

MCP-сервер помимо `tools` может отдавать `prompts` и `resources`. Это не
protocol-незнание — открытый design-вопрос: как они должны появляться в
`InvokeRequest`/`DriverEvent`, если вообще должны.

### Решение

**Не расширять `adapter-core` типы.** `InvokeRequest`, `DriverEvent`, `Part` —
протокол-нейтральные типы, общие для ACP-раннтайма и A2A-маппера. Если
затащить туда MCP-специфичную семантику prompts/resources, оба протокола
получат поля, которые им не нужны и которые придётся либо игнорировать, либо
объяснять пользователям ACP/A2A, откуда они взялись.

Вместо этого — **namespace-конвенция внутри существующих строковых полей**,
без изменения типов:

- MCP `resources` попадают в `InvokeRequest.context` (уже
  `serde_json::Value`, см. `extract_parts`/ACP `context: params.get("metadata")`)
  под ключом `mcp_resources`, как массив `{uri, mimeType}` — то есть просто
  данные, не новая семантика ядра.
- MCP `prompts` регистрируются в `RegisteredAgent.skills` (после Решения 1)
  с префиксом `prompt:` вместо обычного `tool:`/безпрефиксного имени.
  `McpDriver::invoke()` разбирает префикс из `request.skill_id` и решает,
  вызывать `tools/call` или `prompts/get`.

### Почему не решать полностью сейчас

Осознанно отложено до появления второго реального MCP-сервера с prompts —
любое абстрактное решение "как это должно быть по-настоящему" рискует не
совпасть с тем, что реально нужно на практике. Namespace-префикс — обратимый,
низкий-commitment шаг: если окажется неверным, меняется только `driver-mcp`,
не публичный контракт `adapter-core`.

---

## Решение 3: SEP-1686 `input_required` — compile-time feature-gate

### Проблема

`DriverEvent::InputRequired(InputRequest)` и `TaskState::WaitingForInput` уже
полностью реализованы и рабочие — ACP-раннтайм маппит `session/input` →
`CoreCommand::ProvideInput` (`crates/protocol-acp-runtime/src/runtime.rs`,
метод `method_session_input`), `adapter-core::apply_driver_event` переводит
`Accepted|Running` → `WaitingForInput` на этом событии.

Задача не в новых типах — они есть. Задача в том, что MCP-стороны
`input_required` определён экспериментальным SEP-1686, который может
измениться до стабилизации.

### Решение

Feature-gate на уровне `Cargo.toml` `driver-mcp`, не runtime-флаг в конфиге:

```toml
# crates/driver-mcp/Cargo.toml
[features]
default = []
sep-1686-input-required = []
```

Код, парсящий MCP `input_required` notification и конструирующий
`DriverEvent::InputRequired`, компилируется только под этим feature.
`adapterd`/`config.rs` не получает новый YAML-флаг — если фича не
скомпилирована в бинарь, поведение просто не существует, не отключается
условно в runtime.

### Почему compile-time, не runtime-флаг

Runtime-флаг создал бы иллюзию стабильности там, где протокольной стабильности
нет — пользователь конфига мог бы включить/выключить экспериментальное
поведение, не читая, что это experimental SEP. Compile-time feature явно
требует пересборки с осознанным флагом `--features sep-1686-input-required`,
что соответствует уровню риска "может сломаться при обновлении SDK".

Когда SEP-1686 стабилизируется в основной MCP-спеке — feature убирается,
код становится default-путём без миграции тех, кто явно включал флаг (они уже
получали ровно то поведение, что стабилизировалось).

---

## Итог

| # | Решение | Меняет публичный API? | Когда реализовывать |
|---|---|---|---|
| 1 | `skills: RwLock<Vec<String>>` + `Weak<AgentRegistry>` в driver | `resolve()` становится `async` | Можно сразу — низкий риск |
| 2 | Namespace-префиксы в `context`/`skill_id`, без новых типов core | Нет | После первого реального MCP-сервера с prompts |
| 3 | `sep-1686-input-required` Cargo feature | Нет (типы уже есть) | По запросу, до стабилизации SEP держать за feature-gate |

Ни одно из решений не требует правки `AgentDriver` trait самого по себе —
весь blast radius ограничен `driver-mcp` и, для Решения 1, типом одного поля
в `adapter-core::RegisteredAgent`.
