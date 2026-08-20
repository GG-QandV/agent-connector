# ADR-0002: MCP-сервер как третий протокольный слой — что экспонировать и как не дублировать логику

- **Status:** Proposed
- **Date:** 2026-08-16
- **Project:** agent-connector
- **Version context:** 0.7.2
- **Affects:** новый crate `protocol-mcp-server`; не меняет `adapter-core`, `protocol-a2a-server`, `protocol-acp-runtime`
- **Depends on:** ADR-0001 (Решение 2 — namespace-конвенция prompts/resources; MCP-сервер и `driver-mcp` должны использовать одну и ту же конвенцию симметрично)

## Контекст

`agent-connector` сейчас выступает **сервером** для двух протоколов —
A2A (`protocol-a2a-server`, `DefaultRequestHandler` + `AdapterAgentExecutor`)
и ACP (`protocol-acp-runtime`, JSON-RPC stdio `Dispatcher`) — и **клиентом**
для MCP (`driver-mcp`, подключается к чужим MCP-серверам).

Вопрос: должен ли `agent-connector` также выступать MCP-**сервером**, чтобы
любой MCP client (Claude Desktop, другой agent-connector, IDE-плагин) мог
подключиться и видеть зарегистрированных агентов как MCP tools?

Оба существующих сервера следуют одному паттерну: тонкий mapper-слой над
`AdapterCore::dispatch()`/`subscribe()`, без собственной бизнес-логики —
вся бизнес-логика (lifecycle, idempotency, concurrency limits, retention)
живёт в `adapter-core` один раз. Задача ADR — определить, что именно
экспонировать через MCP, повторяя этот же паттерн, а не создавая четвёртое
место, где переизобретается task lifecycle.

---

## Решение: `protocol-mcp-server` — mapper, не отдельный runtime

### Что экспонировать

| MCP-примитив | Источник в adapter-core | Мэппинг |
|---|---|---|
| `tools/list` | `AgentRegistry::agents()` → каждый `RegisteredAgent.skills` | Один MCP `Tool` на каждый skill каждого зарегистрированного агента, имя `{agent_id}.{skill}` |
| `tools/call` | `AdapterCore::dispatch(Invoke)` + `subscribe()` | См. ниже — маппинг streaming-результата в MCP response |
| `notifications/tools/list_changed` | `AgentRegistry` изменения (после ADR-0001 Решение 1) | Сервер шлёт notification при `update_skills()` на любом агенте |
| `resources/list`, `prompts/list` | — | **Не экспонируется в v1.** Симметрично тому, что `driver-mcp` тоже не потребляет prompts/resources от других серверов (ADR-0001 Решение 2 отложено) |
| `sampling/createMessage` | — | **Не поддерживается.** `agent-connector` не является LLM-хостом, у него нет модели, которую можно было бы просить sampling'ить за клиента |

### Почему не resources/prompts в v1

Прямое следствие ADR-0001 Решение 2: сам `driver-mcp` пока не решил, как
MCP prompts/resources мапятся на внутренние типы. Экспонировать то, что
`agent-connector` сам не умеет консистентно принимать с другой стороны,
создало бы асимметричный, недоказанный контракт. Symmetric rule: MCP-сервер
поддерживает ровно то подмножество MCP, которое `driver-mcp` уже умеет
потреблять как клиент.

### Маппинг `tools/call` → `AdapterCore` lifecycle

`tools/call` в MCP — синхронный request/response (с опциональным progress
через `notifications/progress`), а `AdapterCore::dispatch(Invoke)` возвращает
`DispatchResult::Created(TaskSnapshot)` немедленно, с последующим streaming
через `subscribe()`. Мэппинг 1:1 с тем, что уже делает `AdapterAgentExecutor`
для A2A (`execute()` → `unfold` stream над `subscribe()`)[executor.rs]:

```text
tools/call { name: "agentX.skillY", arguments }
  -> AdapterCore::dispatch(Invoke {
       agent_id: Some(agentX), skill_id: Some(skillY),
       idempotency_key: <MCP request id>, input: json_to_parts(arguments),
     })
  -> subscribe(task_id, 0)
  -> замена CoreEvent -> MCP:
       CoreEventKind::Progress    -> notifications/progress (progress_token = MCP request id)
       CoreEventKind::Artifact    -> накопить, не слать отдельно (MCP CallToolResult не streaming artifacts)
       CoreEventKind::Completed   -> CallToolResult { content: parts_to_content_blocks(output), isError: false }
       CoreEventKind::Failed      -> CallToolResult { content: [text(error.message)], isError: true }
       CoreEventKind::InputRequired -> см. ниже
       CoreEventKind::Cancelled   -> ServiceError (request cancelled)
```

`idempotency_key` = MCP JSON-RPC request id — тот же паттерн, что ACP
использует `format!("acp:{request_id}")`[runtime.rs], здесь по аналогии
`format!("mcp:{request_id}")`.

### `InputRequired` — блокирующая проблема, отложена явно

ACP и A2A оба имеют многошаговый (multi-turn) канал: ACP — `session/input`
JSON-RPC метод, A2A — `TaskState::InputRequired` в статус-потоке, которое
клиент разрешает повторным `send_message`. Стандартный `tools/call` в MCP —
однократный запрос без встроенного "жду ответа от клиента" примитива
(это то, что решает experimental SEP-1686, уже зафиксированный как
compile-time feature-gate в ADR-0001 Решение 3).

**Решение:** MCP-сервер v1 не поддерживает `DriverEvent::InputRequired`
сценарий для тулов, вызванных через него — если `AdapterCore` эмитит
`CoreEventKind::InputRequired`, MCP-сервер возвращает `CallToolResult` с
`isError: true` и понятным сообщением ("this tool requires interactive
input, not supported over MCP tools/call in this version"), вместо того
чтобы зависать или обманывать клиента фальшивым success. Полная поддержка
привязывается к тому же `sep-1686-input-required` feature из ADR-0001 —
когда фича стабилизируется и включена, `protocol-mcp-server` получает
собственный маппинг `InputRequired` → SEP-1686 notification, но это
отдельная, следующая задача, не часть v1.

### Structural separation — как не дублировать логику трижды

`protocol-mcp-server` **не получает собственного** task-lifecycle,
idempotency-хендлинга, concurrency-лимитов или retention. Всё это уже есть
один раз в `AdapterCore` и используется всеми тремя протокольными слоями
одинаково:

```text
                    ┌─────────────────┐
   A2A clients ──── │ protocol-a2a-   │
                    │ server          │──┐
                    └─────────────────┘  │
                    ┌─────────────────┐  │      ┌──────────────┐
   ACP clients ──── │ protocol-acp-   │──┼─────▶│ AdapterCore  │
                    │ runtime         │  │      │ (единственный │
                    └─────────────────┘  │      │  lifecycle)   │
                    ┌─────────────────┐  │      └──────────────┘
   MCP clients ──── │ protocol-mcp-   │──┘             │
   (новый)          │ server (новый)  │                │
                    └─────────────────┘                ▼
                                              AgentRegistry, TaskStore,
                                              PolicyEngine, retention
```

Каждый протокольный crate реализует ровно **две функции**: (1) парсинг
своего wire-формата в `CoreCommand`, (2) маппинг `CoreEvent`/`DispatchResult`
обратно в свой wire-формат. Ни один из трёх не хранит task state сам —
`protocol-a2a-server`'s `ExecutionManager` хранит только *подписки*
(`broadcast::Sender`/`Receiver` для конкретной SSE-сессии), не бизнес-state
задачи — это зеркалит то, что должен делать и MCP-сервер: свой
progress-token-to-subscription mapping, не свою копию task lifecycle.

### Разграничение ответственности (RACI-style)

| Ответственность | Владелец |
|---|---|
| Task creation, idempotency, concurrency limits | `AdapterCore` (единственный) |
| Retry/timeout/deadline handling | `AdapterCore` (единственный) |
| Wire-протокол parsing (JSON-RPC для MCP, gRPC/REST для A2A) | Каждый protocol crate свой |
| Streaming adaptation (SSE для A2A, progress notifications для MCP) | Каждый protocol crate свой |
| `AgentRegistry` → protocol-specific capability listing (`tools/list`, AgentCard, ACP capabilities) | Каждый protocol crate свой read-only mapper |
| MCP `progress_token` ↔ `TaskId` correlation | `protocol-mcp-server` (новый, аналог `ExecutionManager` в A2A) |

### Crate-скелет

```text
crates/protocol-mcp-server/
├── Cargo.toml            # rmcp с server feature, не client
├── src/
│   ├── lib.rs             # ServerHandler impl, композиция с AdapterCore
│   ├── tool_catalog.rs    # AgentRegistry -> Vec<rmcp::model::Tool>
│   ├── call_mapper.rs     # tools/call <-> AdapterCore::dispatch/subscribe
│   └── progress_bridge.rs # progress_token <-> TaskId correlation (аналог ExecutionManager)
└── tests/
    └── mcp_server_contract.rs
```

### Что явно НЕ входит в v1

- `resources/*`, `prompts/*` — заблокировано ADR-0001 Решение 2.
- `sampling/createMessage` — `agent-connector` не хост модели.
- Полный multi-turn через `InputRequired` — заблокировано SEP-1686
  feature-gate (ADR-0001 Решение 3); v1 явно возвращает ошибку вместо
  тихого зависания.
- HTTP-транспорт для самого MCP-сервера — начинаем с stdio (симметрично
  тому, что `driver-mcp` сейчас поддерживает только stdio как клиент);
  HTTP+SSE транспорт для MCP-сервера — следующая задача, не блокирует v1.

---

## Итог

`protocol-mcp-server` — третий тонкий mapper поверх `AdapterCore`, реализующий
ровно тот же структурный паттерн, что уже доказан в `protocol-a2a-server` и
`protocol-acp-runtime`: разбор своего wire-формата → `CoreCommand`, и
`CoreEvent`/`DispatchResult` → свой wire-формат, без копии бизнес-логики.
Единственный новый компонент state — `progress_bridge` для `progress_token`
↔ `TaskId`, зеркалящий `ExecutionManager` из A2A-слоя. Три подмножества MCP
(resources, prompts, sampling, полный input_required) сознательно исключены
из v1, каждое — с явной причиной, привязанной к соответствующему решению
из ADR-0001.
