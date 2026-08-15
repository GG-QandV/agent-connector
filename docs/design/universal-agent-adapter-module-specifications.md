# Спецификации модулей Universal Agent Adapter Runtime

**Статус:** Architecture / implementation specification

**Цель:** построить framework- и language-neutral runtime, который подключает существующих агентов к общему lifecycle/runtime и публикует их через A2A. Первые универсальные integrations: local `stdio` и remote `HTTP/SSE`. A2A — отдельный внешний protocol module, а не внутренний транспорт агента.

---

## 1. Scope и неизменяемые принципы

### 1.1 Что делает runtime

Runtime принимает задачу, выбирает зарегистрированного агента, передаёт ему команду по универсальному контракту, надёжно ведёт состояние задачи, сохраняет события и публикует результат через внешний protocol adapter.

```text
ACP client / A2A client / application
                 │
                 ▼
        Protocol Adapter Layer
                 │ CoreCommand / CoreEvent
                 ▼
        Adapter Core Runtime
  registry | lifecycle | policy | scheduler | journal
                 │ UAIC command/event
                 ▼
           Agent Driver Layer
       stdio | HTTP/SSE | future WS/gRPC
                 │
                 ▼
          Any language / framework agent
```

### 1.2 Что не делает runtime

- не содержит LLM, RAG, embeddings, vector DB или model routing;
- не интерпретирует prompt и не принимает агентные решения;
- не требует LangChain, LangGraph, CrewAI, ADK, MCP или конкретного языка;
- не превращает arbitrary CLI/API в полного агента без минимального контракта;
- не хранит содержимое prompt/output в логах по умолчанию.

### 1.3 Ключевой инвариант

`AdapterCore` не зависит от конкретного транспорта, внешнего протокола, framework или языка.

Новый transport, storage backend, framework plugin или external protocol добавляется отдельным crate и подключается через trait. Изменение существующего core public API не допускается без versioned migration.

---

## 2. Universal Agent Integration Contract (UAIC)

UAIC — внутренний versioned контракт между runtime и любым подключаемым агентом. Он определяет смысл команд и событий; конкретный transport определяет только доставку.

### 2.1 Версия

```text
protocol: uaic/1
content_type: application/json
```

Любое сообщение содержит:

```json
{
  "protocol": "uaic/1",
  "message_id": "uuid",
  "task_id": "uuid",
  "timestamp": "2026-08-15T12:00:00Z",
  "payload": {}
}
```

### 2.2 Команды runtime → агент

| Command         | Обязательно | Назначение                                                   |
| --------------- | -----------:| ------------------------------------------------------------ |
| `initialize`    | нет         | Handshake, manifest/capability discovery                     |
| `health`        | да          | Liveness/readiness агента                                    |
| `invoke`        | да          | Запустить новую или идемпотентно вернуть существующую задачу |
| `cancel`        | нет         | Запросить отмену задачи                                      |
| `provide_input` | нет         | Передать ответ на `input_required`                           |
| `get_status`    | нет         | Получить snapshot задачи                                     |
| `resume`        | нет         | Возобновить event stream после sequence number               |

### 2.3 События агент → runtime

| Event            | Обязательно           | Назначение                   |
| ---------------- | ---------------------:| ---------------------------- |
| `initialized`    | при `initialize`      | Версия и capability manifest |
| `accepted`       | да для async          | Агент принял задачу          |
| `progress`       | нет                   | Статус выполнения            |
| `artifact`       | нет                   | Ссылка/metadata результата   |
| `input_required` | нет                   | Нужен дополнительный ввод    |
| `completed`      | да                    | Успешный terminal result     |
| `failed`         | да                    | Terminal error               |
| `cancelled`      | если cancel поддержан | Terminal cancellation        |
| `status`         | при `get_status`      | Snapshot состояния           |

### 2.4 Invoke message

```json
{
  "protocol": "uaic/1",
  "type": "invoke",
  "message_id": "b3154c52-3f64-42c2-a9b0-7fd7f7a1dc3e",
  "task_id": "47a51910-d090-47c3-b2f1-b509acdad1c6",
  "idempotency_key": "caller-42:request-188",
  "session_id": "a847ad77-4e9b-44a3-bd8d-ed2d4be2a3cc",
  "deadline_at": "2026-08-15T12:05:00Z",
  "input": [
    { "kind": "text", "text": "Проверь изменения на ошибки" }
  ],
  "context": {
    "workspace": "/workspace/project"
  },
  "requested_capabilities": {
    "streaming": true,
    "artifacts": true,
    "cancellation": true
  }
}
```

### 2.5 Completed message

```json
{
  "protocol": "uaic/1",
  "type": "completed",
  "message_id": "5b0056d2-c81b-4e9d-bc5d-dbc773507e89",
  "task_id": "47a51910-d090-47c3-b2f1-b509acdad1c6",
  "seq": 7,
  "output": [
    { "kind": "text", "text": "Найдены две проблемы..." }
  ],
  "usage": {
    "duration_ms": 1842
  }
}
```

### 2.6 Manifest

Manifest — явное описание того, что агент реально умеет. Adapter не должен симулировать отсутствующие возможности.

```json
{
  "protocol": "uaic/1",
  "agent": {
    "name": "reviewer-agent",
    "version": "0.3.0",
    "description": "Static code review agent"
  },
  "capabilities": {
    "streaming": true,
    "cancellation": true,
    "provide_input": false,
    "status": true,
    "resume": true,
    "artifacts": true,
    "idempotency": true
  },
  "skills": [
    {
      "id": "code-review",
      "name": "Code review",
      "description": "Reviews source code and produces findings",
      "input_modes": ["text", "file_ref"],
      "output_modes": ["text", "artifact_ref"]
    }
  ]
}
```

### 2.7 Совместимость

- Неизвестное optional поле игнорируется.
- Неизвестный обязательный `type` возвращает `unsupported_message_type`.
- Новый major UAIC version не принимается без explicit compatibility adapter.
- Адаптер проверяет manifest до публикации agent capabilities наружу.

---

## 3. Crate layout и dependency rules

```text
agent-adapter/
├── crates/
│   ├── adapter-model/
│   ├── adapter-core/
│   ├── adapter-config/
│   ├── adapter-storage-memory/
│   ├── adapter-storage-sqlite/
│   ├── adapter-storage-postgres/
│   ├── driver-stdio/
│   ├── driver-http-sse/
│   ├── protocol-a2a/
│   ├── protocol-acp/
│   ├── artifact-store-local/
│   ├── artifact-store-s3/             # future
│   └── adapterd/
└── docs/
```

Dependency graph:

```text
adapter-model
      ↑
adapter-core ← storage-* 
      ↑
protocol-a2a / protocol-acp / driver-stdio / driver-http-sse
      ↑
adapterd
```

Rules:

1. `adapter-model` содержит только DTO, enums, identifiers, schema version helpers.
2. `adapter-core` знает только traits, domain errors и lifecycle.
3. `driver-*` не импортируют `protocol-a2a`/`protocol-acp`.
4. `protocol-*` не вызывают drivers напрямую; только `AdapterService`.
5. `adapterd` — единственный composition root, где concrete dependencies собираются вместе.

---

## 4. `adapter-model` specification

### 4.1 Ответственность

- Versioned domain DTO.
- Stable serialization.
- Public error codes.
- Никакой I/O, storage, tokio или protocol-specific логики.

### 4.2 Идентификаторы

```rust
pub struct AgentId(pub String);
pub struct CallerId(pub String);
pub struct SkillId(pub String);
pub struct TaskId(pub uuid::Uuid);
pub struct SessionId(pub uuid::Uuid);
pub struct EventSeq(pub u64);
pub struct IdempotencyKey(pub String);
```

### 4.3 Task state

```rust
pub enum TaskState {
    Created,
    Accepted,
    Running,
    WaitingForInput,
    CancelRequested,
    Completed,
    Failed,
    Cancelled,
}
```

Terminal: `Completed`, `Failed`, `Cancelled`.

### 4.4 Public errors

```rust
pub enum ErrorCode {
    InvalidRequest,
    Unauthorized,
    Forbidden,
    AgentNotFound,
    NoEligibleAgent,
    AgentUnavailable,
    UnsupportedCapability,
    TaskNotFound,
    InvalidTaskState,
    DeadlineExceeded,
    ResourceExhausted,
    Conflict,
    TransportUnavailable,
    Internal,
}
```

Каждая ошибка содержит публичное безопасное сообщение и optional retry hint. Технический cause не уходит внешнему caller без debug policy.

---

## 5. `adapter-core` specification

### 5.1 Ответственность

- Приём `CoreCommand`.
- Authorization/policy check.
- Выбор агента через `SkillRouter`.
- Создание или идемпотентное получение task.
- Полностью контролируемые state transitions.
- Durable event journal.
- Запуск/контроль connector event pump.
- Cancel/input/status/resume.
- Scheduler и backpressure.

### 5.2 Public service interface

```rust
#[async_trait::async_trait]
pub trait AdapterService: Send + Sync {
    async fn dispatch(
        &self,
        caller: Caller,
        command: CoreCommand,
    ) -> Result<DispatchResult, CoreError>;

    async fn subscribe(
        &self,
        caller: Caller,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<TaskSubscription, CoreError>;
}
```

`TaskSubscription` сначала выдаёт durable catch-up events из journal, затем live stream. Это исключает потерю событий между `GET events` и подпиской на broadcast.

### 5.3 State transition ownership

Только `TaskManager` вызывает `TaskStore::append_event_and_transition`.

Drivers сообщают `AgentEventCandidate`; protocol adapters читают готовые `CoreEvent`. Ни driver, ни A2A/ACP handler не могут напрямую менять `TaskState`.

### 5.4 Task lifecycle algorithm

#### Invoke

1. Проверить caller identity, rate limit и policy.
2. Выбрать agent: explicit `agent_id` → explicit `skill_id` → capability match.
3. Вызвать `create_or_get_idempotent`.
4. Если task уже существует — вернуть существующий `task_id` и актуальный state.
5. Если новая — записать `Created`, затем `Accepted`.
6. Забрать scheduler permit и task lease.
7. Вызвать `AgentDriver::invoke`.
8. Передать события event pump в `TaskManager`.

#### Cancel

1. Проверить caller policy.
2. Если terminal — вернуть current snapshot; это идемпотентно.
3. Транзакционно записать `CancelRequested`.
4. Вызвать `driver.cancel()` только если capability поддерживается.
5. Если driver не поддерживает cancel, задача остаётся `CancelRequested`; policy определяет timeout → `Cancelled` или `Failed`.

#### Provide input

1. Разрешено только в `WaitingForInput`.
2. Проверить caller ownership/policy.
3. Записать input audit metadata без payload по default.
4. Вызвать `driver.provide_input()`.
5. При успешном принятии перейти в `Running`.

### 5.5 Event pump

```text
AgentDriver event stream
       │
       ▼
validate UAIC event + task_id + sequence rules
       │
       ▼
TaskManager apply_event
       │
       ├─ durable transaction: append event + transition state + revision
       ├─ publish live event to subscribers
       └─ release scheduler/lease on terminal state
```

Event pump обязан:

- проверять событие против текущего state;
- отбрасывать duplicate events с уже зафиксированным `seq`;
- не принимать seq gap без explicit recovery policy;
- не держать task lock во время network I/O;
- завершаться при terminal state или cancellation token.

---

## 6. `TaskStore`, `SessionStore` и concurrency

### 6.1 TaskStore trait

```rust
#[async_trait::async_trait]
pub trait TaskStore: Send + Sync {
    async fn create_or_get_idempotent(
        &self,
        new_task: NewTask,
    ) -> Result<CreateTaskResult, StoreError>;

    async fn get_snapshot(&self, task_id: TaskId)
        -> Result<Option<TaskSnapshot>, StoreError>;

    async fn append_event_and_transition(
        &self,
        mutation: TaskMutation,
    ) -> Result<AppliedTaskMutation, StoreError>;

    async fn list_events_after(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
        limit: u32,
    ) -> Result<Vec<CoreEvent>, StoreError>;

    async fn acquire_lease(
        &self, task_id: TaskId, owner: &str, ttl: std::time::Duration,
    ) -> Result<LeaseResult, StoreError>;

    async fn renew_lease(
        &self, task_id: TaskId, owner: &str, ttl: std::time::Duration,
    ) -> Result<bool, StoreError>;
}
```

### 6.2 Данные task

```text
tasks:
  task_id PK
  session_id nullable
  agent_id
  caller_id
  idempotency_key
  state
  revision
  last_seq
  deadline_at
  created_at
  updated_at
  terminal_at nullable
  lease_owner nullable
  lease_expires_at nullable

task_events:
  task_id
  seq
  type
  payload_json
  created_at
  PRIMARY KEY(task_id, seq)

idempotency_records:
  caller_id
  idempotency_key
  task_id
  expires_at
  PRIMARY KEY(caller_id, idempotency_key)
```

### 6.3 Обязательная транзакция

State mutation всегда выполняется в одной DB transaction:

```text
validate current state/revision
→ insert task_event(seq=N+1)
→ update task(state, revision+1, last_seq=N+1)
→ commit
→ publish event
```

Публикация до commit запрещена: client не должен увидеть event, который пропадёт при rollback.

### 6.4 Storage implementations

| Store    | Режим                 | Ограничения                                |
| -------- | --------------------- | ------------------------------------------ |
| Memory   | тесты/local demo      | нет recovery после restart                 |
| SQLite   | single node           | WAL, короткие write transactions           |
| Postgres | remote/multi-instance | distributed leases, HA, concurrent workers |

### 6.5 SessionStore

Session не является механизмом блокировки. Это logical context и metadata:

```rust
pub trait SessionStore: Send + Sync {
    async fn get(&self, id: SessionId) -> Result<Option<Session>, StoreError>;
    async fn create(&self, input: NewSession) -> Result<Session, StoreError>;
    async fn update_metadata(
        &self, id: SessionId, patch: SessionMetadataPatch,
    ) -> Result<Session, StoreError>;
}
```

Policy concurrency на session configurable:

- `concurrent`: default, tasks одной session могут идти параллельно;
- `serial`: следующая задача ждёт terminal state предыдущей;
- `reject_if_busy`: вернуть `Conflict`.

### 6.6 In-memory live fan-out

```rust
struct ActiveTaskBus {
    events: tokio::sync::broadcast::Sender<CoreEvent>,
    cancellation: tokio_util::sync::CancellationToken,
}
```

`broadcast` используется только для live delivery. При `Lagged` subscriber обязан догнать события из `TaskStore` по `last_seq`.

---

## 7. `AgentRegistry` и `SkillRouter`

### 7.1 AgentRegistry

```rust
pub struct RegisteredAgent {
    pub id: AgentId,
    pub manifest: AgentManifest,
    pub driver: std::sync::Arc<dyn AgentDriver>,
    pub limits: AgentLimits,
    pub policy: AgentPolicy,
    pub health: HealthState,
    pub priority: i32,
}

#[async_trait::async_trait]
pub trait AgentRegistry: Send + Sync {
    async fn register(&self, agent: RegisteredAgent) -> Result<(), RegistryError>;
    async fn get(&self, id: &AgentId) -> Result<Option<RegisteredAgent>, RegistryError>;
    async fn candidates(&self, query: &SkillQuery)
        -> Result<Vec<AgentCandidate>, RegistryError>;
    async fn update_health(&self, id: &AgentId, health: HealthState)
        -> Result<(), RegistryError>;
}
```

### 7.2 Router

`SkillRouter` детерминированный. Он не использует LLM в MVP.

Selection order:

1. `agent_id` явно указан caller → policy check → use exact agent.
2. `skill_id` указан → eligible agents that declare exact skill.
3. `tags/capabilities` → healthy candidates, ordered by policy priority, available permits and static priority.
4. Нет кандидата → `NoEligibleAgent`.

### 7.3 Несколько агентов

Один Adapter Daemon может обслуживать много зарегистрированных агентов. Отдельный adapter instance нужен только для tenant/security isolation, independent scaling/release cycle или отдельного public identity.

Локальная делегация между registered agents проходит через `AdapterCore` и `CoreCommand`, без HTTP/A2A serialisation. Внешняя граница использует A2A.

---

## 8. `AgentDriver` и transport boundaries

### 8.1 Driver trait

```rust
#[async_trait::async_trait]
pub trait AgentDriver: Send + Sync {
    fn id(&self) -> &str;
    fn manifest(&self) -> &AgentManifest;

    async fn health(&self) -> Result<Health, DriverError>;

    async fn invoke(
        &self,
        request: UaicInvoke,
    ) -> Result<DriverEventStream, DriverError>;

    async fn cancel(&self, task_id: TaskId)
        -> Result<(), DriverError>;

    async fn provide_input(
        &self, task_id: TaskId, input: UaicInput,
    ) -> Result<(), DriverError>;

    async fn resume(
        &self, task_id: TaskId, after_seq: EventSeq,
    ) -> Result<Option<DriverEventStream>, DriverError>;
}
```

Driver обязан вернуть `Unsupported` для capability, отсутствующей в manifest.

### 8.2 Transport vs driver

- **Transport**: bytes/frame delivery (`stdin/stdout`, HTTP requests/SSE, WebSocket, gRPC).
- **UAIC codec**: encode/decode UAIC messages.
- **Driver**: lifecycle-specific реализация поверх transport + codec.

Это позволяет иметь:

```text
GenericStdioDriver   = NDJSON UAIC codec + stdio transport
GenericHttpSseDriver = JSON UAIC codec + HTTP/SSE transport
```

Future transport добавляется без изменения `TaskManager`:

```text
GenericWebSocketDriver = UAIC codec + WebSocket transport
GenericGrpcDriver      = UAIC protobuf mapping + gRPC transport
```

---

## 9. `driver-stdio` specification

### 9.1 Назначение

Универсально подключает локальный agent process любого языка: Rust, Python, Node, Go, Java и т.д.

```text
Adapter ── stdin/stdout NDJSON ── Agent subprocess
```

### 9.2 Process model

- Adapter запускает command через `tokio::process::Command`.
- `stdin` — только UAIC NDJSON commands.
- `stdout` — только UAIC NDJSON events.
- `stderr` — diagnostics/logs агента; adapter redacts/stores according to policy.
- Process supervisor отвечает за restart/backoff и health state.

### 9.3 NDJSON framing

Одна JSON UAIC message на одну строку UTF-8.

Constraints:

- `max_line_bytes`: default 1 MiB;
- malformed JSON → protocol violation;
- unknown `task_id` event → reject/log;
- stdout, не соответствующий UAIC, не допускается;
- binary artifact передаётся только как `artifact_ref`, не raw bytes в NDJSON.

### 9.4 Lifecycle

- `initialize` выполняется на старте subprocess.
- При crash все принадлежащие process active tasks получают `AgentUnavailable` или переходят в recovery policy.
- Auto-restart не должен автоматически повторно запускать side-effecting task; допустим только `resume`, если manifest подтверждает resume.
- Cancel subprocess не означает kill process по default. Hard kill разрешён только per-agent policy после grace timeout.

### 9.5 Local security

- process запускается с отдельным uid/gid при доступности;
- рабочая директория и env allowlist;
- secrets только из controlled env;
- запрет shell interpolation в config;
- resource limits optional: cgroup/container, CPU/memory/time limit.

---

## 10. `driver-http-sse` specification

### 10.1 Назначение

Подключает удалённый агент как service: endpoint + credential. Не зависит от языка backend.

```text
Adapter ── POST UAIC command ───────────────► Agent
Adapter ◄─ SSE UAIC event stream ─────────── Agent
Adapter ── POST cancel/provide_input ───────► Agent
```

HTTP/SSE функционально двунаправлен через отдельные HTTP commands и обратный SSE stream; это не один bidirectional transport.

### 10.2 Required endpoints

```text
GET  /v1/uaic/manifest
GET  /v1/uaic/health
POST /v1/uaic/tasks
GET  /v1/uaic/tasks/{task_id}/events?after_seq=N
GET  /v1/uaic/tasks/{task_id}
POST /v1/uaic/tasks/{task_id}/cancel
POST /v1/uaic/tasks/{task_id}/input
```

Названия endpoint могут быть configurable, но semantic contract неизменен.

### 10.3 Invoke

```http
POST /v1/uaic/tasks
Authorization: Bearer <token>
Content-Type: application/json
Idempotency-Key: <key>
X-UAIC-Version: 1
```

Response:

```http
202 Accepted
Location: /v1/uaic/tasks/<task_id>
```

Или immediate terminal sync response допустим для простого агента.

### 10.4 SSE stream

```text
GET /v1/uaic/tasks/{task_id}/events?after_seq=17
Accept: text/event-stream
Last-Event-ID: 17
```

Каждый frame:

```text
id: 18
event: uaic.progress
data: {"protocol":"uaic/1","type":"progress",...}
```

Rules:

- `id` совпадает с durable `seq`;
- adapter сохраняет event в journal до публикации caller;
- reconnect начинается с `after_seq=last_durable_seq`;
- event payload имеет max size;
- heartbeat comment/`ping` configurable;
- network retry: exponential backoff with jitter;
- retry не создаёт новую task.

### 10.5 Fallback

Fallback на другой driver возможен только:

- до отправки invoke;
- при DNS/TLS/connect failure;
- при явно unsupported transport;
- при timeout до `Accepted`, только с тем же task_id и idempotency key.

После `Accepted` разрешены только status query и resume/reconnect. Новый invoke запрещён.

### 10.6 Remote security

MVP:

- HTTPS required outside explicitly marked development environment;
- bearer token from secret provider;
- connect/read/write deadlines;
- hostname allowlist.

Production:

- mTLS optional-required profile;
- certificate rotation;
- OIDC workload identity option;
- SSRF protections;
- no credential forwarding to external caller.

---

## 11. `protocol-a2a` specification

### 11.1 Ответственность

- Публиковать Agent Card для adapter или individual registered agent.
- Преобразовывать A2A request в `CoreCommand`.
- Преобразовывать `CoreEvent` в A2A task/message/artifact/update.
- Реализовать A2A server и optional A2A client.

### 11.2 Жёсткие границы

`protocol-a2a` не:

- выбирает transport к underlying agent;
- меняет state task;
- хранит task state вне `TaskStore`;
- содержит framework-specific code.

### 11.3 A2A server flow

```text
A2A request
  → authenticate caller
  → map to CoreCommand::Invoke
  → AdapterService::dispatch
  → return task snapshot/task id
  → subscribe(task_id, after_seq)
  → map durable/live CoreEvent to A2A event stream
```

### 11.4 Agent Card strategy

Два режима:

1. **Platform card**: один публичный adapter identity, skills всех registered agents. Router выбирает target internally.
2. **Per-agent cards**: отдельная card/URL для каждого registered agent. Router получает explicit `agent_id`.

Default для local multi-agent daemon: platform card. Per-agent cards включаются при независимой публичной identity/policy.

### 11.5 A2A client role

Optional module позволяет registered agent/core вызывать remote A2A agents:

```text
discover Agent Card → validate endpoint/auth → invoke remote task → consume events → map into local CoreEvent
```

Remote A2A agent представляется как `AgentDriver`. Поэтому local и remote agents обрабатываются одинаково `TaskManager`.

### 11.6 Compliance boundary

Exact A2A request/response schemas, method names и binding version фиксируются отдельным `a2a-wire` submodule после выбора official A2A spec version. Изменение A2A wire version не меняет UAIC и adapter-core.

---

## 12. `protocol-acp` specification

### 12.1 Назначение

Optional интеграция для editor/CLI client ↔ coding agent. ACP не заменяет A2A.

```text
ACP client ↔ stdio ACP adapter ↔ AdapterCore ↔ selected agent driver
```

### 12.2 Режимы

- `acp.server`: editor/CLI подключается к adapter, adapter маршрутизирует в registered coding agent.
- `acp.client`: future, если adapter должен вызывать внешний ACP agent.

### 12.3 Границы

ACP mapping:

- ACP session/request -> `CoreCommand`;
- Core task event -> ACP progress/session update;
- approval/input request -> ACP client interaction.

Exact ACP wire schemas фиксируются после выбора spec version. ACP transport stdio живёт в `protocol-acp`, не в `driver-stdio`: это разные уровни.

---

## 13. Scheduler, quotas и fault isolation

### 13.1 Limits

```rust
pub struct AgentLimits {
    pub max_concurrent_tasks: usize,
    pub max_queued_tasks: usize,
    pub max_input_bytes: usize,
    pub max_event_bytes: usize,
    pub default_timeout: std::time::Duration,
}
```

Enforcement layers:

- global semaphore;
- per-caller quota;
- per-agent semaphore;
- bounded queue per agent;
- deadline per task;
- token bucket rate limiter per caller.

### 13.2 Overload behavior

Никогда не создавать неограниченную in-memory очередь.

Если queue/permits исчерпаны:

```text
ResourceExhausted
retry_after_ms: optional
```

Queueing разрешён только когда policy явно включает `queue_until_deadline`.

### 13.3 Circuit breaker

Per-agent states:

```text
Healthy → Degraded → Unhealthy → HalfOpen → Healthy
```

Repeated transport/agent failures временно исключают агента из `SkillRouter`. Это не влияет на другие registered agents.

---

## 14. Security module requirements

### 14.1 Identity types

```rust
pub enum CallerIdentity {
    LocalProcess { uid: Option<u32> },
    BearerToken { subject: String, scopes: Vec<String> },
    Mtls { subject: String },
    Oidc { subject: String, tenant: Option<String> },
}
```

### 14.2 Policy decision

```rust
pub trait PolicyEngine: Send + Sync {
    async fn authorize(
        &self,
        caller: &Caller,
        action: Action,
        resource: Resource,
    ) -> Result<Decision, PolicyError>;
}
```

Actions: `Invoke`, `Cancel`, `ProvideInput`, `ReadTask`, `ReadArtifact`, `DiscoverAgent`.

Resources: `AgentId`, `SkillId`, `TaskId`, `SessionId`.

### 14.3 Security defaults

Local:

- no public listener;
- stdio/Unix socket preferred;
- filesystem permissions as boundary;
- no payload logging.

Remote:

- TLS required;
- credentials never relayed to caller;
- secret provider abstraction;
- explicit outbound endpoint allowlist;
- audit metadata only by default;
- request/event size limits;
- rate limits.

---

## 15. Observability specification

### 15.1 Logs

JSON structured logs:

```text
task_id, session_id, caller_id_hash, agent_id, driver_id,
state_from, state_to, seq, latency_ms, error_code
```

Запрещено по default: bearer tokens, raw prompt, raw output, artifact bytes, filesystem content.

### 15.2 Metrics

- `adapter_active_tasks` by agent/state;
- `adapter_task_transitions_total` by transition;
- `adapter_driver_errors_total` by driver/error;
- `adapter_event_stream_reconnect_total`;
- `adapter_queue_depth`;
- `adapter_task_duration_seconds`;
- `adapter_protocol_requests_total` by A2A/ACP;
- `adapter_resource_exhausted_total`.

### 15.3 Tracing

OpenTelemetry trace:

```text
external request
  → protocol mapping
  → core dispatch
  → routing/policy
  → driver invocation
  → event pump
```

Trace context передаётся агенту внутри UAIC `context.trace`, только если policy это разрешает.

---

## 16. Конфигурация

### 16.1 Minimal local

```yaml
mode: local

agents:
  - id: reviewer
    driver: stdio
    command: ./reviewer-agent

a2a:
  server: false
  client: true

storage:
  type: memory
```

### 16.2 Minimal remote

```yaml
mode: remote

agents:
  - id: remote-reviewer
    driver: http-sse
    endpoint: https://reviewer.internal
    credential:
      type: bearer_env
      name: REVIEWER_TOKEN

a2a:
  server: true
  client: true

storage:
  type: sqlite
  path: /var/lib/adapter/tasks.db

security:
  inbound_auth: bearer_env
  inbound_token_env: ADAPTER_TOKEN
```

### 16.3 Production multi-instance

```yaml
storage:
  type: postgres
  dsn_env: ADAPTER_DATABASE_URL

security:
  inbound_auth: oidc
  outbound_tls: mtls

scheduler:
  global_max_concurrent_tasks: 128
  per_caller_max_concurrent_tasks: 8

observability:
  otel_endpoint_env: OTEL_EXPORTER_OTLP_ENDPOINT
```

---

## 17. Test specifications

### 17.1 Unit tests

- Every allowed/forbidden state transition.
- Duplicate idempotency key returns same task.
- Duplicate event `seq` is not delivered twice.
- Terminal task rejects `provide_input`.
- `cancel` is idempotent.
- Router exact agent/skill/capability priority.
- Capability mismatch yields `UnsupportedCapability`.

### 17.2 Driver contract tests

Один reusable test suite запускается против каждого driver:

- health;
- invoke → accepted → completed;
- invoke → failed;
- invoke → progress stream;
- cancel when supported/unsupported;
- reconnect/resume when supported;
- malformed message;
- timeout;
- duplicate task id/idempotency key.

### 17.3 Integration tests

- stdio fake agent subprocess;
- HTTP/SSE fake service;
- SQLite restart recovery;
- Postgres concurrent state mutations;
- A2A request → core → fake driver → A2A stream;
- subscriber reconnect between durable write and live subscribe;
- one failing agent does not block second agent.

### 17.4 Load tests

- N independent tasks, bounded memory;
- one agent slow, other agents remain available;
- many SSE reconnects;
- concurrent same idempotency key;
- event fan-out with lagging subscribers;
- lease recovery after worker termination.

---

## 18. Delivery plan

### Phase 1 — Universal local MVP

Deliver:

- `adapter-model`, `adapter-core`, `MemoryTaskStore`;
- UAIC/1 NDJSON;
- `driver-stdio`;
- one registered agent;
- state machine, idempotency, timeout, basic event journal;
- no public listener.

### Phase 2 — Universal remote MVP

Deliver:

- `driver-http-sse`;
- SQLite event/task store;
- A2A server mapping over selected official binding;
- bearer auth, HTTPS-only production config;
- event resume by sequence;
- multiple `RegisteredAgent` + deterministic `SkillRouter`.

### Phase 3 — Reliability

Deliver:

- scheduler, per-agent quotas, circuit breaker;
- artifact-store abstraction;
- OpenTelemetry/Prometheus;
- local Unix socket option;
- A2A client module.

### Phase 4 — Production scale

Deliver:

- Postgres, leases, multi-instance recovery;
- mTLS/OIDC policies;
- remote-connect architecture;
- shared artifact store.

### Phase 5 — New transports

Deliver as independent crates:

- `driver-websocket` for persistent/NAT use cases;
- `driver-grpc` for typed high-throughput bidirectional streaming;
- transport discovery manifest and safe auto-selection.

No phase is allowed to rewrite UAIC, TaskManager, state machine or existing driver contract. New behavior is additive through optional capability flags and versioned DTO fields.

---

## 19. Acceptance criteria

1. Любой language/framework agent, реализующий UAIC NDJSON over stdio, подключается без framework plugin.
2. Любой service, реализующий UAIC JSON + HTTP/SSE, подключается URL + credential.
3. A2A mapping не знает, какой transport/язык у underlying agent.
4. Core не импортирует Axum, reqwest, tokio-tungstenite, tonic или A2A/ACP wire types.
5. После reconnect subscriber получает все durable events, начиная с requested sequence.
6. Повтор invoke не создаёт duplicate side effect при одинаковом idempotency key.
7. Agent failure изолирован per-agent и не останавливает adapter daemon.
8. Новый driver подключается отдельным crate и проходит общий driver contract test suite.
9. WebSocket/gRPC добавляются без изменения TaskStore, TaskManager, UAIC или A2A mapping.
10. В default logs отсутствуют secrets, prompt/output и artifact contents.

---

## 20. Итог

Universal Agent Adapter Runtime строится вокруг трёх стабильных сущностей:

```text
UAIC command/event contract
+ durable transport-neutral task lifecycle
+ pluggable AgentDriver
```

`stdio` и remote `HTTP/SSE` — первые concrete drivers. A2A — отдельный protocol module поверх общего core. Благодаря этому MVP остаётся небольшим, но имеет прямой путь к multi-agent routing, WebSocket, gRPC, distributed Postgres runtime и production security без переписывания базового кода.

---

# Дополнение к спекам: Production NFR — нагрузка, streaming, backpressure и надёжность

**Назначение:** вставить как отдельный раздел в `universal-agent-adapter-module-specifications.md`.

Этот раздел обязателен для remote MVP и production. Он фиксирует поведение runtime при высокой нагрузке, медленных клиентах, тяжёлом трафике, длинных stream, недоступных агентах, reconnect storm и storage pressure.

---

## 21. Production NFR: нагрузка и streaming reliability

### 21.1 Нормативные принципы

1. Adapter не должен потреблять неограниченную RAM из-за задач, очередей, SSE-клиентов, event stream или artifacts.
2. Медленный caller не должен замедлять agent, task lifecycle или других callers.
3. Один неисправный agent не должен блокировать другие registered agents.
4. Потеря transport connection не должна автоматически означать потерю task.
5. Retry/fallback не должен повторно выполнить side effect.
6. Durable task/event state важнее live delivery: сначала transaction/journal, потом publish.
7. Progress/telemetry — best effort; terminal events и state transitions — durable и обязательные.
8. Все лимиты должны быть configurable, иметь безопасные defaults и быть видимы через metrics.

---

### 21.2 Лимиты ресурсов

Runtime обязан применять лимиты минимум на четырёх уровнях:

```text
global runtime
  └─ caller / tenant
       └─ registered agent
            └─ task / subscriber / connection
```

Минимальная конфигурация:

```yaml
limits:
  global:
    max_concurrent_tasks: 128
    max_open_streams: 512
    max_queue_depth: 1000

  caller:
    max_concurrent_tasks: 8
    max_open_streams: 16
    requests_per_minute: 120

  agent:
    default_max_concurrent_tasks: 4
    default_max_queue_depth: 32

  task:
    max_input_bytes: 1048576
    max_context_bytes: 4194304
    max_event_bytes: 262144
    max_artifact_metadata_bytes: 65536
    max_events_per_task: 10000

  subscriber:
    max_pending_events: 256
    max_pending_bytes: 1048576
```

Значения являются defaults, а не жёсткими требованиями. Для каждого `RegisteredAgent` допускается override, но нельзя назначить лимит выше глобального без явной privileged policy.

---

### 21.3 Backpressure и очередь задач

#### Требования

* Все command queues должны быть bounded.
* Очередь не должна быть неявной и бесконечной.
* Scheduler обязан иметь global, per-caller и per-agent concurrency control.
* Network/DB I/O не выполняется под global registry lock.
* При перегрузке adapter возвращает явную ошибку, а не ждёт бесконечно.

```rust
pub enum QueuePolicy {
    Reject,
    QueueUntilDeadline,
}
```

Default: `Reject`.

При переполнении:

```json
{
  "code": "resource_exhausted",
  "message": "Agent queue is full",
  "retry_after_ms": 1000
}
```

#### Политика очереди

`QueueUntilDeadline` допустим только если:

* caller задал `deadline_at`;
* policy разрешает queue для выбранного agent;
* queue depth находится ниже per-agent limit;
* task не имеет side effect, который требует немедленного ответа, либо caller это явно подтвердил.

Task в очереди имеет state `Accepted`, но не `Running`. Event journal фиксирует `accepted` с причиной `queued`.

---

### 21.4 Медленные SSE/WS/gRPC subscribers

#### Риск

Агент может производить события быстрее, чем внешний клиент читает SSE, WebSocket или gRPC stream. Если runtime буферизует поток без ограничений, память будет расти до OOM.

#### Требования

* У каждого subscriber есть bounded buffer по числу events и bytes.
* Subscriber получает только события в порядке `seq`.
* Отставший subscriber не замедляет event pump, task или других subscribers.
* При превышении лимита subscriber отключается с причиной `slow_consumer`.
* Terminal task/event не теряется: subscriber обязан reconnect и выполнить durable replay из `TaskStore`.

```text
Agent event
  → durable journal write
  → non-blocking publish to active subscribers
       ├─ fast subscriber: immediate send
       └─ slow subscriber: bounded buffer overflow → disconnect
```

#### Поведение transport layers

* HTTP/SSE: закрыть stream; при возможности отправить final SSE event `adapter.error` с `slow_consumer`.
* WebSocket: close с application-defined close code и `slow_consumer` reason.
* gRPC: terminate stream with resource-exhausted status.

Повторное подключение:

```text
GET /events?after_seq=<last_received_seq>
```

Adapter сначала возвращает durable events batch-ами, затем подключает caller к live fan-out.

---

### 21.5 Event journal pressure и progress coalescing

#### Риск

LLM/coding agent может писать token-level или tool-level progress тысячи раз в секунду. Durable запись каждого события создаёт нагрузку на SQLite/Postgres и делает replay дорогим.

#### Классы событий

| Класс           | Примеры                                                          | Durable                      |
| --------------- | ---------------------------------------------------------------- | ---------------------------- |
| Critical        | accepted, input_required, artifact, completed, failed, cancelled | Всегда                       |
| Progress        | status text, percent, token count, heartbeat                     | Coalesced                    |
| Debug telemetry | trace detail, internal timing                                    | Не в task journal по default |

#### Coalescing policy

```yaml
events:
  progress:
    min_interval_ms: 500
    persist_on_percent_change: true
    max_per_task: 1000
    live_delivery: true
```

Rules:

* Сохранять progress не чаще `min_interval_ms`.
* Сохранять progress при смене процента или significant status change.
* Последний progress snapshot хранится в `tasks.current_progress`.
* После `max_per_task` adapter перестаёт journal'ить non-critical progress и увеличивает metric `progress_events_dropped_total`.
* Critical events никогда не coalesce и не drop.

#### Journal retention

```yaml
retention:
  terminal_task_ttl: 7d
  event_ttl: 7d
  artifact_ttl: 3d
  cleanup_interval: 1h
```

Retention cleanup не может удалять task/event, пока задача non-terminal или существует active subscription/retention hold.

---

### 21.6 Artifacts и тяжёлый трафик

#### Обязательное правило

`CoreEvent` и A2A/ACP event не должны содержать raw binary artifact bytes.

```rust
pub struct ArtifactRef {
    pub artifact_id: String,
    pub name: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub checksum: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}
```

#### Artifact flow

```text
Agent output stream
  → streaming upload / file write
  → ArtifactStore returns ArtifactRef
  → task journal stores only ArtifactRef
  → caller downloads through authenticated endpoint or signed short-lived URL
```

#### Требования

* Streaming upload/download; нельзя собирать большой artifact целиком в RAM.
* Configurable max artifact size и max total artifact bytes per task/caller.
* Проверка `Content-Length`, stream byte counter и abort при лимите.
* Optional checksum для integrity.
* Download требует отдельной authorization check.
* Поддержка HTTP Range желательна для больших artifacts.
* Artifact cleanup по TTL и deletion retry.

Storage implementations:

* local profile: filesystem store с path isolation;
* production: S3-compatible object store через отдельный `ArtifactStore` trait.

---

### 21.7 Таймауты и зависшие агенты

Каждый driver обязан поддерживать отдельные таймауты:

```yaml
timeouts:
  connect: 5s
  request_write: 15s
  first_event: 30s
  idle_stream: 60s
  task_default: 15m
  cancel_grace: 10s
  shutdown_grace: 30s
```

| Timeout         | Смысл                          | Реакция                                                        |
| --------------- | ------------------------------ | -------------------------------------------------------------- |
| `connect`       | Нет соединения с агентом       | driver error, fallback только до invoke acceptance             |
| `request_write` | Команда не передалась          | retry/fallback согласно idempotency policy                     |
| `first_event`   | Нет `accepted`/first event     | status probe; затем safe retry только с тем же idempotency key |
| `idle_stream`   | Stream открыт, но агент молчит | status/resume probe; затем cancel policy                       |
| `task_default`  | Общий deadline задачи          | `CancelRequested` → grace → terminal failure/cancel            |
| `cancel_grace`  | Агент не подтвердил cancel     | force policy для конкретного driver                            |

#### Force cancellation

* `stdio`: допускается SIGTERM, затем SIGKILL только после grace period и только по per-agent policy.
* HTTP/SSE: нельзя «убить» удалённый процесс; вызвать cancel/status, затем зафиксировать `Failed(DeadlineExceeded)` или `CancelledByPolicy`.
* После terminal transition поздние driver events не меняют state, но логируются как protocol violation.

---

### 21.8 Reconnect storm и replay protection

#### Риск

После сбоя proxy, сети или deploy много subscribers могут одновременно reconnect и запрашивать полный event history, перегружая DB и network.

#### Требования

* Exponential backoff with full jitter на client/driver reconnect.
* Per-caller/per-IP reconnect rate limit.
* Max replay events и max replay bytes на один request.
* Cursor-based pagination для длинной истории.
* Response `429`/`503` должен включать `Retry-After`.
* Не разрешать unlimited `after_seq=0`, если история превышает configured retention/replay limit.
* Если requested events уже очищены по retention: вернуть `history_expired` + current task snapshot, а не читать несуществующую историю.

```yaml
replay:
  max_events_per_request: 500
  max_bytes_per_request: 4194304
  reconnect_attempts_per_minute: 20
  initial_backoff_ms: 250
  max_backoff_ms: 30000
```

---

### 21.9 Database и storage pressure

#### Требования

* State transition + critical event записываются синхронно и атомарно.
* Progress events могут batch/coalesce, но не terminal events.
* DB transactions короткие; network calls запрещены внутри transaction.
* Connection pool bounded.
* Retry DB transaction только для transient serialization/deadlock failures, с bounded backoff.
* Не выполнять unbounded `SELECT * FROM task_events`.
* Все replay queries используют `(task_id, seq)` index и limit.

#### SQLite profile

* Включить WAL mode.
* Установить busy timeout.
* Один write-heavy node; multi-instance SQLite запрещён.
* При write pressure применять progress coalescing и reject queue policy раньше, чем допустить долгую lock contention.

#### Postgres profile

* Optimistic revision или row-level lock для transition.
* Lease ownership для active task worker.
* Partial indexes для active tasks и expired leases.
* Connection pool size согласуется с max concurrent tasks.
* Event table partitioning/archive — future requirement при большом retention/RPS.

### 21.10 Lease и recovery после crash

Для multi-instance режима:

```text
worker acquires task lease
  → invokes/resumes driver
  → renews lease while task active
  → terminal state releases lease
```

При crash worker lease истекает. Новый worker:

1. читает task state;
2. проверяет capability `resume` у driver;
3. вызывает `resume(task_id, last_seq)` или `get_status`;
4. продолжает event pump либо переводит task в controlled terminal failure;
5. не запускает новый invoke без idempotency/recovery guarantee.

Lease expiration не должен автоматически означать повторное исполнение non-idempotent task.

---

### 21.11 Head-of-line blocking и fairness

#### Риск

Долгие задачи одного caller/agent занимают worker slots и задерживают короткие задачи других callers/agents.

#### Требования

* Отдельные queues и semaphores per agent.
* Per-caller quotas.
* Global quota — только общий верхний предел, не единственная очередь.
* Long task не держит global lock.
* Priority scheduling optional; default FIFO внутри per-agent queue.
* Если priority включён, требуется anti-starvation: ageing или max wait guarantee.
* Healthcheck не проходит через перегруженную execution queue.

---

### 21.12 Graceful shutdown

При shutdown adapter обязан:

1. перестать принимать новые non-health requests;
2. вернуть readiness=false;
3. сохранить state активных задач;
4. перестать назначать новые leases;
5. попытаться graceful cancel/flush в пределах `shutdown_grace`;
6. закрыть SSE/WS/gRPC subscribers с retryable shutdown signal;
7. сохранить event cursor и закрыть storage;
8. дать новому instance resume active tasks через lease recovery.

Local `stdio` child process не должен убиваться немедленно без per-agent shutdown policy.

---

### 21.13 Обязательные metrics и alerts

#### Metrics

```text
adapter_active_tasks{agent,state}
adapter_queue_depth{agent}
adapter_queue_rejected_total{agent,caller}
adapter_slow_consumer_disconnect_total{transport}
adapter_event_journal_write_latency_seconds
adapter_event_journal_write_failures_total
adapter_progress_events_coalesced_total
adapter_progress_events_dropped_total
adapter_sse_reconnect_total{driver}
adapter_replay_requests_total
adapter_replay_history_expired_total
adapter_driver_idle_timeout_total{driver}
adapter_task_deadline_exceeded_total{agent}
adapter_artifact_bytes_total{agent}
adapter_artifact_rejected_total{reason}
adapter_lease_recovery_total
adapter_db_pool_in_use
```

#### Минимальные alert conditions

* queue rejection растёт стабильно;
* journal write latency/failed writes выше baseline;
* slow consumer disconnect spike;
* idle stream timeout spike;
* agent unhealthy/circuit open;
* DB pool exhausted;
* replay history expired unexpectedly;
* memory usage растёт при стабильном RPS.

---

### 21.14 Required load and failure tests

| Сценарий                        | Критерий                                                       |
| ------------------------------- | -------------------------------------------------------------- |
| Много независимых tasks         | Memory bounded; разные агенты не блокируют друг друга          |
| Один медленный SSE client       | Он отключается; task и fast subscribers продолжают работу      |
| 1000+ progress events           | Journal coalescing работает; terminal event не теряется        |
| Большой artifact                | Adapter не держит файл целиком в RAM                           |
| Потеря SSE после `Accepted`     | Task не стартует повторно; reconnect/resume продолжает stream  |
| Reconnect storm                 | DB/network ограничены rate limit и replay batch limit          |
| Agent hangs                     | Срабатывает idle/task timeout и controlled terminal transition |
| SQLite restart                  | Terminal events и task snapshot сохраняются                    |
| Worker crash в Postgres profile | Lease recovery не создаёт duplicate invoke                     |
| Один failing agent              | Другие registered agents остаются доступными                   |
| Queue overload                  | Явный `ResourceExhausted`, нет OOM/unbounded latency           |

---

### 21.15 Definition of Done для production readiness

Remote adapter не считается production-ready, пока не выполнены все пункты:

* bounded queues, buffers и connection pools;
* per-agent/per-caller/global limits;
* durable critical event journal;
* SSE reconnect/replay с cursor и pagination;
* slow-consumer isolation;
* event progress coalescing;
* artifact store без in-memory buffering больших файлов;
* all timeout classes;
* idempotency и safe retry/fallback rules;
* structured logs, metrics, tracing и alerts;
* graceful shutdown/recovery tests;
* load и fault-injection tests из раздела 21.14.
