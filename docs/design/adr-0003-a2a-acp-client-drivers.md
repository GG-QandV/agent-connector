# ADR-0003: driver-a2a-client и driver-acp-client — вызов внешних A2A/ACP агентов

- **Status:** Proposed
- **Date:** 2026-08-16
- **Affects:** два новых crate `driver-a2a-client`, `driver-acp-client`; одна
  правка в `adapterd-config` (`AgentTransportConfig` enum); НЕ меняет
  `adapter-core`, `protocol-a2a-server`, `protocol-acp-runtime`

## Контекст

`agent-connector` сейчас — сервер для A2A/ACP (`protocol-a2a-server`,
`protocol-acp-runtime`) и клиент для MCP/stdio/HTTP-SSE (`driver-mcp`,
`driver-stdio`, `driver-http-sse`). Отсутствует возможность самому
`adapterd` вызывать **другой** A2A- или ACP-сервер как подчинённого
агента — то есть выступать A2A/ACP клиентом, симметрично тому, как
`driver-mcp` уже выступает MCP клиентом.

## Почему это не требует изменений архитектуры

`AgentDriver` trait — единственная точка контакта между `adapter-core` и
любым транспортом:

```rust
pub trait AgentDriver: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> DriverCapabilities;
    async fn health(&self) -> Result<(), CoreError>;
    async fn invoke(&self, task_id: TaskId, request: InvokeRequest) -> Result<mpsc::Receiver<DriverEvent>, CoreError>;
    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError>;
    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError>;
}
```

`adapter-core` не знает и не должен знать, что происходит внутри `invoke()` —
HTTP-запрос, stdio pipe, MCP session или A2A/ACP клиентский вызов. Новые
драйверы реализуют этот же trait, ничего в `adapter-core` не меняется.
Единственная точка расширения — `AgentTransportConfig` enum в
`adapterd-config`, куда добавляются два новых варианта рядом с уже
существующими `Stdio`/`HttpSse`.

## Решение 1: driver-a2a-client

### Транспорт

A2A — HTTP/JSON-RPC протокол (подтверждено структурой `protocol-a2a-server`:
`SendMessageRequest`, `StreamResponse`, `TaskState`). Клиентский драйвер —
обёртка над HTTP-клиентом (`reqwest`), не требует нового транспортного
механизма — только новый **семантический** слой поверх HTTP, аналогично
тому, как `driver-http-sse` уже HTTP-based, но говорит другим протоколом
(UAIC/1, не A2A JSON-RPC).

### Маппинг AgentDriver -> A2A клиентские вызовы

```text
invoke(task_id, request)
  -> POST /v1/message:send (streaming=true) на удалённый A2A endpoint
  -> SendMessageRequest { message: parts_to_a2a_message(request.input), ... }
  -> ответ — SSE-стрим StreamResponse событий
  -> маппинг StreamResponse -> DriverEvent (обратный тому, что уже делает
     eventtostreamresponse() в executor.rs, но в другую сторону):
       TaskStatusUpdateEvent{state: Working, message}  -> DriverEvent::Progress
       TaskStatusUpdateEvent{state: InputRequired, ..}  -> DriverEvent::InputRequired
       Task{status: Completed, ...}                     -> DriverEvent::Completed
       Task{status: Failed, ...}                        -> DriverEvent::Failed
       Task{status: Canceled, ...}                       -> DriverEvent::Cancelled

cancel(task_id)
  -> POST /v1/tasks/{id}:cancel на удалённый endpoint

provide_input(task_id, input)
  -> POST /v1/message:send с тем же task_id (A2A multi-turn: повторный
     send_message с существующим taskId продолжает WaitingForInput задачу —
     это уже подтверждённый паттерн из protocol-a2a-server::DefaultRequestHandler)
```

### Ключевое отличие от driver-http-sse

`driver-http-sse` говорит собственным UAIC/1 протоколом adapter-connector —
подходит только для агентов, реализующих именно этот контракт.
`driver-a2a-client` говорит стандартным A2A protocol — подходит для **любого**
A2A-совместимого агента (включая другой инстанс `agent-connector`,
запущенный кем-то ещё). Это расширяет совместимость наружу, не дублирует
существующий `driver-http-sse`.

### provide_input — здесь работает по-настоящему, не заглушка

В отличие от `driver-mcp` (где `provide_input` отключён до стабилизации
SEP-1686 — ADR-0001 Решение 3), A2A **уже имеет** стандартный multi-turn
механизм через повторный `send_message` с тем же `taskId`. Значит
`driver-a2a-client.capabilities().provide_input = true` можно выставить
сразу, без feature-gate — это не экспериментальный протокол, а часть
стабильной A2A спеки, которую уже реализует сам `protocol-a2a-server`.

## Решение 2: driver-acp-client

### Транспорт

ACP в этом проекте — stdio JSON-RPC (подтверждено `protocol-acp-runtime`:
`AcpRuntimeConfig`, `StdinOut<R,W>`, `session/prompt`, `session/cancel`,
`session/input`, `session/update`). Клиентский драйвер — spawn child-процесса
через stdio, отправка JSON-RPC запросов, чтение построчных ответов —
структурно похоже на `driver-stdio`, но говорит ACP JSON-RPC методами,
не UAIC/1 NDJSON.

### Маппинг AgentDriver -> ACP клиентские вызовы

```text
connect: spawn child process, отправить `initialize` (аналог того, что
         сервер отдаёт в methodinitialize — тут наоборот, клиент его вызывает)

invoke(task_id, request)
  -> `session/new` (если нет активной сессии для caller) -> sessionId
  -> `session/prompt` { sessionId, requestId: task_id, prompt: parts_to_acp_blocks(request.input) }
  -> подписка на `session/update` события (тот же метод, что уже
     реализован на сервере — теперь клиент его периодически вызывает
     или получает push, если ACP runtime поддерживает notifications)
  -> маппинг ACP событий -> DriverEvent (обратное eventtojson() из runtime.rs)

cancel(task_id)
  -> `session/cancel` { taskId: task_id }

provide_input(task_id, input)
  -> `session/input` { taskId: task_id, prompt: parts_to_acp_blocks(input) }
     (уже подтверждённый метод в protocol-acp-runtime, симметричный вызов)
```

### provide_input — тоже работает по-настоящему

`session/input` — уже существующий, протестированный метод в
`protocol-acp-runtime` (сервер его принимает). Клиентский драйвер просто
вызывает его в обратную сторону. `capabilities().provide_input = true`
без feature-gate — ACP multi-turn не экспериментальный в этом проекте,
он уже в проде на серверной стороне.

## Общая структура двух новых crate (зеркалит driver-mcp)

```text
crates/driver-a2a-client/
├── Cargo.toml       # reqwest, eventsource-stream (для SSE), serde_json, tokio
├── src/
│   ├── lib.rs        # AgentDriver impl, config struct
│   └── mapper.rs     # StreamResponse <-> DriverEvent (обратный executor.rs::event_to_stream_response)
└── tests/
    └── contract.rs    # против mock A2A server (axum test server)

crates/driver-acp-client/
├── Cargo.toml       # tokio (process, io), serde_json
├── src/
│   ├── lib.rs        # AgentDriver impl, spawn+JSON-RPC клиент
│   └── mapper.rs     # ACP JSON-RPC <-> DriverEvent
└── tests/
    └── contract.rs    # против mock ACP stdio server (тестовый child process)
```

## Изменение конфигурации (adapterd-config)

```rust
// crates/adapterd/src/config.rs — AgentTransportConfig, добавить два варианта
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "driver", rename_all = "kebab-case")]
pub enum AgentTransportConfig {
    Stdio { command: PathBuf, #[serde(default)] args: Vec<String>, ... },
    HttpSse { endpoint: String, ... },
    // НОВОЕ:
    A2aClient {
        endpoint: String,
        #[serde(default)] token_env: Option<String>,
        #[serde(default)] allow_http_development: bool,
    },
    AcpClient {
        command: PathBuf,
        #[serde(default)] args: Vec<String>,
        #[serde(default)] working_dir: Option<PathBuf>,
    },
}
```

`main.rs::build_driver()` получает две новые ветки `match`, зеркалящие уже
существующие для `Stdio`/`HttpSse` — не меняет структуру функции, только
расширяет match.

## Итог

Ни `adapter-core`, ни `protocol-a2a-server`, ни `protocol-acp-runtime` не
меняются. Оба новых драйвера — независимые crate, реализующие уже
существующий `AgentDriver` trait, добавленные в `AgentTransportConfig` тем
же способом, что и предыдущие варианты. В отличие от `driver-mcp`, оба
новых драйвера могут включить `provide_input` без feature-gate — их
multi-turn механизмы уже стабильны и реализованы на серверной стороне
этого же проекта.
