# Архитектура модульного Agent Adapter на Rust

## 1. Цель

Создать лёгкий адаптер, который подключает существующий мини-агент без ACP/A2A к внешним клиентам и другим агентам.

Адаптер не реализует интеллект, RAG, prompt routing или бизнес-логику агента. Он:

- принимает задачу через внешний протокол;
- приводит её к единой внутренней модели;
- запускает существующий агент через выбранный транспорт;
- хранит состояние задачи и сессии;
- передаёт прогресс, результат, запрос ввода и ошибку;
- публикует возможности агента через A2A/ACP;
- обеспечивает минимальную локальную или полную удалённую безопасность.

Ключевая цель: MVP с HTTP/SSE должен расширяться до WebSocket, gRPC, durable storage, multi-tenant и production observability **без переписывания core**.

---

## 2. Архитектурные принципы

1. **Protocol-neutral core.** Core не импортирует HTTP, Axum, SSE, WebSocket, gRPC, ACP или A2A типы.
2. **Transport-neutral agent connection.** Core вызывает `AgentConnector`, а не HTTP client напрямую.
3. **Один внутренний контракт.** Все входы становятся `CoreCommand`, все результаты — `CoreEvent`.
4. **State machine — единственный источник истины.** Только core меняет статус задачи.
5. **Event journal до доставки.** Сначала событие и состояние фиксируются, потом отдаются SSE/WS/gRPC. Это нужно для resume и reconnect.
6. **Idempotency по умолчанию.** Повтор `invoke` не создаёт вторую задачу.
7. **Pluggable storage.** In-memory для local/MVP; SQLite для одного узла; Postgres для production.
8. **Explicit capabilities.** Адаптер не обещает streaming/cancel/files, если underlying agent этого не поддерживает.
9. **Secure by profile.** Local не открывает сеть; Remote требует TLS и авторизацию.
10. **Расширение добавлением модуля.** Новый транспорт = новая реализация trait, а не `if transport == ...` по всему проекту.

---

## 3. Границы системы

```text
                 Внешние клиенты / агенты
                  │                  │
             ACP client          A2A client
                  │                  │
         protocol-acp       protocol-a2a
                  └──────┬───────────┘
                         ▼
                   adapter-core
       task lifecycle | registry | policy | scheduler
                         │
                         ▼
                   AgentConnector
       HTTP/SSE | stdio | Unix socket | WebSocket | gRPC
                         │
                         ▼
                    existing mini-agent
```

### Не входит в adapter-core

- векторная БД;
- RAG;
- LLM provider routing;
- memory агента;
- обработка доменной логики;
- самостоятельное редактирование файлов;
- orchestration нескольких агентов.

Эти вещи остаются в мини-агенте или верхнем gateway.

---

## 4. Режимы развёртывания

### 4.1 Local profile

```text
ACP client / local A2A client
             │
         Adapter
             │ stdio или Unix socket
         Mini-agent
```

Назначение: одна машина, один контейнер, или соседний container/POD.

Default:

- agent transport: `stdio`;
- network listener: выключен либо `127.0.0.1`;
- storage: memory или SQLite;
- auth между adapter и agent: отсутствует при доверенной process boundary;
- защита: file/socket permissions, input limits, timeouts, no sensitive logs.

### 4.2 Remote profile

```text
A2A client ── HTTPS ── Adapter ── HTTPS/mTLS/WS ── Mini-agent
```

Назначение: разные хосты, cloud gateway, multi-user, external access.

Default:

- external A2A: HTTPS + JSON-RPC/REST + SSE;
- adapter-to-agent: HTTPS + bearer token, позднее mTLS;
- storage: Postgres;
- auth: signed token/OIDC/mTLS, policy per caller;
- observability: metrics, tracing, audit events.

Для агента за NAT предпочтителен future `remote-connect` mode: локальный sidecar сам создаёт исходящее WSS/mTLS соединение к gateway. Входящий порт на машине агента не нужен.

---

## 5. Внутренний контракт core

Ни ACP, ни A2A, ни HTTP-типов внутри core быть не должно.

```rust
// adapter-core/src/model.rs
pub type TaskId = uuid::Uuid;
pub type SessionId = uuid::Uuid;
pub type EventSeq = u64;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct InvokeRequest {
    pub task_id: TaskId,
    pub idempotency_key: String,
    pub session_id: Option<SessionId>,
    pub input: Vec<Part>,
    pub context: serde_json::Value,
    pub deadline_ms: Option<u64>,
    pub requested_capabilities: RequestedCapabilities,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CoreCommand {
    Invoke(InvokeRequest),
    Cancel { task_id: TaskId, reason: Option<String> },
    ProvideInput { task_id: TaskId, input: Vec<Part> },
    GetStatus { task_id: TaskId },
    Resume { task_id: TaskId, after_seq: EventSeq },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CoreEventKind {
    Accepted,
    Progress { message: String, percent: Option<u8> },
    Artifact { artifact: ArtifactRef },
    InputRequired { request: InputRequest },
    Completed { output: Vec<Part> },
    Failed { error: PublicError },
    CancelRequested,
    Cancelled,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CoreEvent {
    pub task_id: TaskId,
    pub seq: EventSeq,
    pub at: chrono::DateTime<chrono::Utc>,
    pub kind: CoreEventKind,
}
```

`Part`, `ArtifactRef`, `InputRequest`, `PublicError` должны быть простыми versioned DTO. Содержимое файлов не хранится внутри event по умолчанию: event содержит ссылку/metadata, а blob лежит в `ArtifactStore`.

---

## 6. Жизненный цикл задачи

### 6.1 Состояния

```text
Created
  └─> Accepted
       └─> Running ──────> Completed
            │                 
            ├─> WaitingForInput ─> Running
            ├─> CancelRequested ─> Cancelled
            └─> Failed

Created / Accepted / Running / WaitingForInput
  └─> Failed
```

Terminal states: `Completed`, `Failed`, `Cancelled`.

### 6.2 Разрешённые переходы

| Команда/событие           | Разрешено из                             | Новое состояние             |
| ------------------------- | ---------------------------------------- | --------------------------- |
| `Invoke`                  | нет задачи                               | `Created`, затем `Accepted` |
| connector `Accepted`      | `Created`                                | `Accepted`                  |
| connector `Progress`      | `Accepted`, `Running`                    | `Running`                   |
| connector `InputRequired` | `Accepted`, `Running`                    | `WaitingForInput`           |
| `ProvideInput`            | `WaitingForInput`                        | `Running`                   |
| `Cancel`                  | non-terminal                             | `CancelRequested`           |
| connector `Cancelled`     | `CancelRequested`                        | `Cancelled`                 |
| connector `Completed`     | `Accepted`, `Running`, `WaitingForInput` | `Completed`                 |
| connector `Failed`        | non-terminal                             | `Failed`                    |

Повтор `Cancel` должен быть идемпотентным. `Complete` после `Cancelled` отвергается и только логируется как protocol violation.

### 6.3 Правило ownership

Только `TaskManager` применяет переходы состояния. Connector не изменяет registry напрямую: он отправляет кандидаты событий в `TaskManager`.

---

## 7. Registry сессий и задач под нагрузкой

### 7.1 Разделение сущностей

- **Session** — логический диалог/контекст; может содержать много задач.
- **Task** — одно выполнение; имеет собственное состояние, deadline, connector lease и event sequence.
- **Event** — неизменяемая запись истории конкретной задачи.
- **Idempotency record** — соответствие `(caller_id, idempotency_key) -> task_id`.
- **Lease** — право одного worker управлять конкретной активной задачей.

### 7.2 Storage traits

```rust
#[async_trait::async_trait]
pub trait TaskStore: Send + Sync {
    async fn create_or_get_idempotent(
        &self,
        request: NewTask,
    ) -> Result<CreateTaskResult, StoreError>;

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskSnapshot>, StoreError>;

    async fn append_event_and_transition(
        &self,
        transition: Transition,
    ) -> Result<AppliedTransition, StoreError>;

    async fn events_after(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
        limit: usize,
    ) -> Result<Vec<CoreEvent>, StoreError>;

    async fn acquire_lease(&self, task_id: TaskId, owner: &str, ttl: Duration)
        -> Result<bool, StoreError>;

    async fn renew_lease(&self, task_id: TaskId, owner: &str, ttl: Duration)
        -> Result<bool, StoreError>;
}
```

### 7.3 MVP и production реализации

| Реализация          | Где использовать                  | Свойства                                       |
| ------------------- | --------------------------------- | ---------------------------------------------- |
| `MemoryTaskStore`   | local dev, тесты                  | быстро, нет recovery после restart             |
| `SqliteTaskStore`   | local daemon, single-node         | durable, WAL, один writer/умеренная нагрузка   |
| `PostgresTaskStore` | remote production, multi-instance | leases, durable events, HA, concurrent workers |

### 7.4 Concurrency правила

1. Разные `task_id` выполняются параллельно.
2. У одной task события упорядочены `seq = 1..N`.
3. Один переход изменяет state + добавляет event в **одной транзакции**.
4. Для Postgres: `UPDATE ... WHERE revision = expected_revision` или `SELECT ... FOR UPDATE`.
5. Для SQLite: WAL mode, короткие transactions, event payload ограниченного размера.
6. Большие artifact bytes не держать в task row и не пересылать через broadcast без лимита.
7. In-memory cache допустим как read-through cache, но durable store остаётся source of truth в remote profile.

### 7.5 Event fan-out

В memory layer на каждую active task:

```rust
struct ActiveTask {
    event_tx: tokio::sync::broadcast::Sender<CoreEvent>,
    cancel: tokio_util::sync::CancellationToken,
}
```

Но `broadcast` не является durable delivery: подписчик может отстать и получить lag. При reconnect transport должен запросить `events_after(task_id, last_seq)` из store, затем подписаться на live events.

---

## 8. Connector abstraction

```rust
#[async_trait::async_trait]
pub trait AgentConnector: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> ConnectorCapabilities;

    async fn health(&self) -> Result<Health, ConnectorError>;

    async fn start(
        &self,
        request: InvokeRequest,
    ) -> Result<ConnectorEventStream, ConnectorError>;

    async fn provide_input(
        &self,
        task_id: TaskId,
        input: Vec<Part>,
    ) -> Result<(), ConnectorError>;

    async fn cancel(&self, task_id: TaskId) -> Result<(), ConnectorError>;

    async fn resume(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<Option<ConnectorEventStream>, ConnectorError>;
}
```

Connector возвращает только normalized events. Парсинг HTTP/SSE frame, WebSocket message и protobuf выполняется внутри конкретной реализации.

### Реализации

- `HttpSseConnector` — MVP для remote.
- `StdioConnector` — MVP для local CLI agent.
- `UnixSocketConnector` — local sidecar.
- `WebSocketConnector` — remote-connect/NAT, позднее.
- `GrpcConnector` — высокая нагрузка/typed bidi stream, позднее.

---

## 9. HTTP/SSE: асинхронность и «двунаправленность»

HTTP/SSE не является одним bidi-stream. Это надёжная композиция:

```text
Adapter ── POST /tasks ────────────────> Agent
Adapter <─ SSE /tasks/{id}/events ───── Agent
Adapter ── POST /tasks/{id}/input ─────> Agent
Adapter ── POST /tasks/{id}/cancel ────> Agent
Adapter ── GET  /tasks/{id} ───────────> Agent
```

Она функционально двунаправленная:

- agent посылает события через SSE;
- adapter посылает команды через отдельные идемпотентные HTTP requests.

### Важные правила HttpSseConnector

- `POST /tasks` содержит adapter-generated `task_id` и `Idempotency-Key`.
- После `Accepted` нельзя делать fallback как новый `POST`; нужен `GET status` или reconnect/resume для той же `task_id`.
- SSE event обязан содержать `id: <seq>`.
- Adapter сохраняет последний durable sequence до выдачи наружу.
- SSE reconnect использует `Last-Event-ID` и/или `?after_seq=`.
- Backoff: exponential + jitter; deadline задачи не продлевается автоматически.
- SSE parser ограничивает event size и number of malformed frames.

### Внутренний event pump

```text
Connector SSE reader
       │ ConnectorEvent
       ▼
TaskManager command queue
       │ transaction: state + event journal
       ├── publish broadcast event
       └── wake SSE/WS/gRPC subscribers
```

Ни один transport handler не должен напрямую менять task state.

---

## 10. Protocol adapters

### A2A adapter

Отвечает за:

- Agent Card из `AgentDescriptor`;
- A2A request -> `CoreCommand`;
- `CoreEvent` -> A2A Task/Message/Artifact;
- A2A HTTP JSON-RPC/REST и SSE stream.

### ACP adapter

Отвечает за:

- stdio JSON-RPC transport;
- ACP session update -> core command;
- core event -> ACP session update/progress;
- lifecycle процесса при local mode.

Оба используют один `AdapterService`:

```rust
#[async_trait::async_trait]
pub trait AdapterService: Send + Sync {
    async fn dispatch(&self, caller: Caller, command: CoreCommand)
        -> Result<DispatchResult, CoreError>;
    async fn subscribe(&self, task_id: TaskId, after_seq: EventSeq)
        -> Result<TaskSubscription, CoreError>;
}
```

---

## 11. Transport discovery, выбор и fallback

### 11.1 Конфигурация

```yaml
agent:
  transport: auto # http-sse | stdio | unix-socket | websocket | grpc
  endpoint: https://mini-agent.internal
  token_env: MINI_AGENT_TOKEN

transport_policy:
  prefer: [http-sse, websocket, grpc]
  allow_fallback: true
  require_tls: true
  require_streaming: true
```

### 11.2 Discovery manifest

Опциональный endpoint:

```text
GET /.well-known/agent-adapter.json
```

Он возвращает identity, capabilities, transport endpoints и приоритеты.

### 11.3 Выбор

1. Явно указанный `transport` всегда побеждает.
2. При `auto` adapter читает manifest.
3. Отбрасывает не собранные Cargo features и не удовлетворяющие security/capability policy.
4. Делает `health`.
5. Выбирает highest-priority здоровый connector.
6. Кэширует выбор с TTL.

### 11.4 Безопасный fallback

Fallback разрешён только:

- до отправки invoke;
- при DNS/TLS/connection error;
- при явно `unsupported transport`;
- при timeout до `Accepted`, только с тем же `task_id`/idempotency key.

После `Accepted` допустимы только reconnect, `resume` или status query. Новый invoke запрещён: иначе side effect может выполниться дважды.

---

## 12. Security

### Local profile

- no public listener by default;
- stdio или Unix socket;
- socket permissions;
- не печатать secrets/prompt/artifacts в logs;
- max input/event/artifact metadata size;
- subprocess sandboxing/uid separation как optional hardening.

### Remote profile

- TLS обязательно;
- mTLS как production recommendation для adapter-to-agent;
- caller identity: OIDC/JWT/API token;
- credential provider abstraction, secrets only via env/secret manager;
- capability-based policy: caller может вызывать только разрешённые skills;
- rate limit и concurrency quota per caller/tenant;
- audit metadata без payload по умолчанию;
- SSRF protection для user-supplied endpoint; allowlist hostnames;
- signed/validated Agent Card при федерации агентов.

---

## 13. Структура Rust workspace

```text
agent-adapter/
├── Cargo.toml
├── crates/
│   ├── adapter-model/           # DTO, versions, no runtime
│   ├── adapter-core/            # TaskManager, lifecycle, policy
│   ├── adapter-storage-memory/
│   ├── adapter-storage-sqlite/
│   ├── adapter-storage-postgres/
│   ├── connector-http-sse/
│   ├── connector-stdio/
│   ├── connector-unix-socket/
│   ├── connector-websocket/     # future feature
│   ├── connector-grpc/          # future feature
│   ├── protocol-a2a/
│   ├── protocol-acp/
│   ├── adapter-config/
│   └── adapterd/                # binary/composition root
└── docs/
```

### Dependency rule

```text
adapter-model ← adapter-core ← protocol-* / connector-* ← adapterd
                    ↑
              storage-*
```

- `adapter-core` не зависит от protocol/connector concrete crates.
- `adapterd` создаёт concrete implementations и передаёт их через traits.
- future crate может быть добавлен без изменения core public API.

### Cargo features

```toml
[features]
default = ["http-sse", "stdio", "sqlite"]
http-sse = ["dep:connector-http-sse"]
stdio = ["dep:connector-stdio"]
websocket = ["dep:connector-websocket"]
grpc = ["dep:connector-grpc"]
postgres = ["dep:adapter-storage-postgres"]
```

---

## 14. Scheduler и backpressure

`TaskManager` не должен запускать unlimited tasks.

```rust
pub struct Scheduler {
    global: tokio::sync::Semaphore,
    per_caller: DashMap<CallerId, Arc<Semaphore>>,
    per_agent: DashMap<AgentId, Arc<Semaphore>>,
}
```

Политика:

- global `max_concurrent_tasks`;
- per-caller quota;
- per-agent capacity;
- bounded command channels;
- overload -> explicit `ResourceExhausted`, не бесконечная очередь;
- очередь допустима только как явная policy (`queue_until_deadline`).

Для long-running task worker регулярно renews lease. Если процесс умер, другой worker может забрать expired lease и выполнить `connector.resume()` либо отметить task `Failed/Unknown` согласно capability connector.

---

## 15. Observability

Минимум MVP:

- structured JSON logs: task_id, session_id, connector, state, duration, error code;
- `/healthz`, `/readyz`;
- без текстов prompt/output по умолчанию.

Production:

- OpenTelemetry spans: request -> core dispatch -> connector -> event pump;
- Prometheus: active tasks, queue depth, connector failures, stream reconnects, transition latency;
- audit event store;
- configurable redaction policy.

---

## 16. План реализации без rewrite

### Этап 0 — contracts и тесты

- `adapter-model`;
- state transition table как unit tests;
- fake connector;
- memory store.

### Этап 1 — local MVP

- `StdioConnector`;
- in-memory registry;
- core invoke/cancel/status;
- ACP stdio adapter;
- single process, no remote listener.

### Этап 2 — remote MVP

- `HttpSseConnector`;
- A2A HTTP/SSE server;
- bearer auth;
- SQLite task/event journal;
- resume SSE by sequence;
- idempotency.

### Этап 3 — reliable single-node production

- retry/reconnect policy;
- leases;
- resource quotas;
- artifact store interface;
- OpenTelemetry/metrics;
- Unix socket local sidecar.

### Этап 4 — multi-instance

- Postgres store;
- distributed leases;
- durable outbox/event journal;
- shared artifact store;
- mTLS and OIDC/JWT policy.

### Этап 5 — new transports

- WebSocket connector for remote-connect/NAT;
- gRPC connector for typed high-throughput streaming;
- manifest discovery and safe transport auto-selection.

На каждом этапе добавляется новая реализация trait или storage backend. `TaskManager`, state model и protocol mappings не переписываются.

---

## 17. Критерии готовности MVP

1. Агент с `POST /tasks` + SSE events подключается только URL и token.
2. A2A caller может discover adapter и выполнить задачу.
3. Повторный invoke с тем же idempotency key возвращает исходный task.
4. SSE reconnect с `last_seq` не теряет и не дублирует durable events.
5. Cancel безопасен при повторе.
6. Невозможные переходы состояния отклоняются.
7. Одновременно выполняются независимые tasks, события одной task упорядочены.
8. При падении SSE после `Accepted` задача не перезапускается автоматически.
9. Prompt, token и artifact content не появляются в default logs.
10. Новый connector можно добавить как отдельный crate без изменения adapter-core.

---

## 18. Итоговое решение

Adapter — это не proxy «HTTP в A2A». Это небольшой runtime с устойчивым task lifecycle.

Его центр:

```text
Normalized Command/Event + Task State Machine + Durable Registry
```

Вокруг центра подключаются независимо:

```text
вход: ACP / A2A
выход к агенту: stdio / Unix socket / HTTP-SSE / WebSocket / gRPC
storage: memory / SQLite / Postgres
```

Поэтому MVP остаётся лёгким: HTTP/SSE, SQLite и один binary. Но структура сразу допускает production-расширение — persistent WebSocket, gRPC, HA, multi-tenant security и distributed task ownership — без замены core и без изменения контракта существующих агентов.

## 19. Несколько локальных агентов: общий Adapter Daemon

### 19.1 Решение по умолчанию

Если на одной машине, в одном контейнере или в соседних контейнерах работают 2–3 и более агентов, по умолчанию используется **один общий Adapter Daemon**.

```text
Agent A ──┐
Agent B ──┼── stdio / Unix socket / localhost HTTP ──► Adapter Daemon
Agent C ──┘                                              │
                                                        ├─ A2A client → remote agents
                                                        └─ A2A server ← external callers
```

Каждый агент остаётся отдельным процессом или контейнером. Adapter не объединяет их внутреннюю логику и не делает их одним агентом. Он даёт им общие:

* A2A/ACP compatibility endpoints;
* task/session/event runtime;
* auth и policy;
* transport discovery;
* observability;
* scheduler и resource limits.

### 19.2 Зачем один adapter

Один daemon предпочтительнее отдельных adapter-процессов для каждого локального агента, потому что:

* один внешний A2A endpoint вместо нескольких портов;
* один policy/security boundary для доверенной локальной группы агентов;
* один `TaskRegistry`, `SessionRegistry` и durable event journal;
* единые quotas и backpressure для всей машины;
* один механизм discovery и transport fallback;
* локальные агенты могут делегировать задачи друг другу через core без сетевого round-trip;
* меньше процессов, конфигурации, ключей и operational overhead.

### 19.3 AgentRegistry

Добавить в `adapter-core` отдельный реестр агентов. Он не заменяет `TaskRegistry` и `SessionRegistry`.

```text
AgentRegistry   — какой агент доступен, какие у него skills, connector, health и limits.
TaskRegistry    — какая задача выполняется, её state, owner, события и result.
SessionRegistry — к какому логическому диалогу/контексту относится задача.
```

Минимальная модель:

```rust
pub struct RegisteredAgent {
    pub id: AgentId,
    pub descriptor: AgentDescriptor,
    pub connector: Arc<dyn AgentConnector>,
    pub skills: Vec<SkillDescriptor>,
    pub limits: AgentLimits,
    pub health: HealthState,
    pub policy: AgentPolicy,
}

pub trait AgentRegistry: Send + Sync {
    async fn register(&self, agent: RegisteredAgent) -> Result<(), RegistryError>;
    async fn get(&self, id: &AgentId) -> Result<Option<RegisteredAgent>, RegistryError>;
    async fn find_by_skill(
        &self,
        query: &SkillQuery,
        caller: &Caller,
    ) -> Result<Vec<AgentCandidate>, RegistryError>;
    async fn mark_health(&self, id: &AgentId, health: HealthState)
        -> Result<(), RegistryError>;
}
```

### 19.4 Конфигурация нескольких агентов

```yaml
mode: local

agents:
  - id: reviewer
    name: Code Reviewer
    transport: stdio
    command: ./bin/reviewer-agent
    skills:
      - id: code-review
        tags: [code, review, security]
    limits:
      max_concurrent_tasks: 2

  - id: docs-agent
    name: Documentation Agent
    transport: unix-socket
    socket: /run/docs-agent.sock
    skills:
      - id: docs-search
        tags: [documentation, retrieval]
    limits:
      max_concurrent_tasks: 4

  - id: test-agent
    name: Test Agent
    transport: http-sse
    endpoint: http://127.0.0.1:8088
    skills:
      - id: test-run
        tags: [tests, qa]
    limits:
      max_concurrent_tasks: 1
```

Каждый элемент `agents[]` создаёт отдельный `AgentConnector`. Новая реализация connector добавляется без изменения `TaskManager` или A2A/ACP mappings.

### 19.5 Маршрутизация задач

Добавить `SkillRouter` как отдельный модуль между protocol adapter и `TaskManager`.

```text
Incoming A2A/ACP task
        │
        ▼
  Authorization policy
        │
        ▼
     SkillRouter
        │
        ├─ explicit agent_id → указанный агент
        ├─ explicit skill_id → агент-владелец skill
        └─ capability match → лучший eligible agent
        │
        ▼
 TaskManager + выбранный AgentConnector
```

Порядок выбора:

1. Если caller явно указал `agent_id` и имеет право его вызывать — использовать его.
2. Если указан `skill_id` — выбрать агента, который объявил skill.
3. Если указан только capability/tag — выбрать здорового кандидата по policy, priority и свободной capacity.
4. Если кандидатов нет — вернуть явную ошибку `NoEligibleAgent`; не отправлять задачу случайному агенту.

Router не использует LLM для выбора в MVP. Это должен быть детерминированный policy-based выбор. LLM-routing может быть отдельным future module, но не частью compatibility runtime.

### 19.6 A2A client и A2A server — отдельные роли

У adapter должны быть независимо включаемые роли:

```text
A2A Client role:
  local agent / core → discover remote Agent Card → invoke remote task → consume events

A2A Server role:
  external caller → Adapter → authorize → route to local agent → stream task events
```

Минимальная безопасная настройка для локальной машины, где агентам нужно только вызывать удалённых агентов:

```yaml
a2a:
  client: true
  server: false
```

В таком режиме adapter не публикует локальные агенты во внешнюю сеть и не открывает входящий listener. Он выполняет только исходящие A2A-вызовы.

Если другие агенты должны вызывать локальных исполнителей:

```yaml
a2a:
  client: true
  server: true
```

Для `server: true` в local profile listener по умолчанию ограничивается `127.0.0.1` или Unix socket. Публикация наружу допускается только в remote profile через HTTPS reverse proxy, mTLS или outbound remote-connect tunnel.

### 19.7 Локальная агент-агент делегация

Когда один зарегистрированный local agent должен вызвать другой зарегистрированный local agent, не нужно делать HTTP/A2A round-trip.

```text
Reviewer Agent
      │ CoreCommand::Invoke(target = test-agent)
      ▼
Adapter Core → SkillRouter → TestAgentConnector → Test Agent
```

Core сохраняет обычный `Task`, события и policy как для внешнего A2A-вызова. Поэтому audit, cancellation, limits и resume работают одинаково.

A2A serialisation применяется только на внешней границе:

```text
external agent ⇄ A2A wire protocol ⇄ Adapter
local agent    ⇄ normalized CoreCommand/Event ⇄ Adapter
```

Это уменьшает задержку, исключает лишнюю сериализацию и сохраняет transport-neutral core.

### 19.8 Лимиты и изоляция внутри общего daemon

Общий daemon не означает отсутствие изоляции. Обязательные механизмы:

* `max_concurrent_tasks` на каждого агента;
* отдельный bounded queue на каждого агента;
* отдельный timeout/deadline policy;
* circuit breaker при repeated connector failures;
* restart supervision для stdio subprocess;
* healthcheck и временное исключение unhealthy agent из router;
* per-agent access policy: caller может иметь доступ не ко всем локальным agents/skills;
* максимальный размер input/output/artifact metadata.

Отказ одного агента не должен останавливать daemon или отменять задачи других агентов.

### 19.9 Когда нужен отдельный adapter на агента

Отдельные adapter instances оправданы, если требуется хотя бы одно из условий:

* разные tenant/владельцы;
* недоверенные агенты;
* отдельные secrets и security policies;
* независимое горизонтальное масштабирование;
* отдельный публичный A2A identity/endpoint на каждого агента;
* разные release cycles;
* агент потребляет ресурсы так, что его нужно изолировать на уровне container/host.

Во всех остальных случаях default — один Adapter Daemon и много `RegisteredAgent`.

### 19.10 Модель расширения

Multi-agent поддержка не требует rewrite MVP. Она добавляется последовательно:

1. В MVP есть один `RegisteredAgent` и один connector.
2. Добавляется `AgentRegistry` и `SkillRouter`; одиночный агент становится записью в registry.
3. Добавляется несколько connector instances и per-agent limits.
4. Добавляется optional A2A client role для outgoing delegation.
5. Добавляется optional A2A server role для входящих внешних задач.
6. В remote/multi-instance deployment registry metadata и policies переносятся в Postgres/control plane, а task ownership остаётся через leases.

`TaskManager`, task state machine, event journal и transport abstraction на этих этапах не меняются.
