# Канон архитектуры agent-connector (ex agent-adapter) + installer/runtime разделение

Этот документ — авторская эталонная спецификация, присланная явно в этом разговоре. Она заменяет мою предыдущую реконструкцию (`agent-connector-context-handoff.md`) как источник истины по намерению; реконструкция остаётся полезной как отражение **текущего** состояния кода в `github.com/GG-QandV/agent-connector`. Ниже — сведённые правила и явные расхождения между тем, что задумано, и тем, что реально в репозитории на текущий момент.

## 1. Installer/runtime разделение (Postgres + Docker) — принято как обязательное правило

### Инвариант

```text
adapterd (runtime):
  НИКОГДА не требует Docker и не управляет контейнерами.

adapterctl install (bootstrapper):
  Может управлять Docker ТОЛЬКО в install/bootstrap phase,
  ТОЛЬКО при user-selected managed-postgres profile.
```

`adapterd` не выполняет `docker run postgres`, не создаёт volume, не обновляет контейнер, не управляет сетью, паролями или бэкапом. Всё это — обязанность отдельного инструмента `adapterctl install` (или эквивалентного bootstrapper), который запускается один раз при установке.

### Разделение ответственности

```text
installer / bootstrapper — один раз
  ├─ проверяет Docker
  ├─ при явном согласии ставит Docker
  ├─ создаёт Docker Compose project
  ├─ создаёт volume
  ├─ генерирует секреты
  ├─ запускает Postgres
  ├─ создаёт database/user/schema
  ├─ запускает migrations
  └─ создаёт adapter config

adapterd runtime — постоянно
  ├─ читает DSN/config
  ├─ использует Postgres через TaskStore
  ├─ делает healthcheck
  └─ не трогает Docker
```

### CLI-флоу

```text
adapterctl install

Storage backend:
  1. SQLite (recommended for local / one daemon)
  2. Existing PostgreSQL
  3. Install managed PostgreSQL with Docker

Choose: 3
```

При выборе 3 installer:

1. Проверяет наличие Docker Engine + Docker Compose.
2. Если Docker нет — показывает, что будет установлено, запрашивает **отдельное** подтверждение, ставит Docker только при explicit yes.
3. Генерирует: случайный пароль Postgres, `database: agent_adapter`, `user: agent_adapter`, `schema: agent_adapter`, Compose project name.
4. Создаёт install directory `~/.local/share/agent-adapter/postgres/`.
5. Пишет `compose.yaml`, `.env` (права `0600`), persistent Docker volume.
6. Поднимает контейнер.
7. Ждёт readiness через `pg_isready`.
8. Создаёт schema/user/database.
9. Запускает adapter migrations.
10. Записывает в adapter config **только ссылку** на DSN environment variable — не сам секрет.

### Итоговый runtime config

```yaml
storage:
  type: postgres
  dsn_env: ADAPTER_DATABASE_URL
  schema: agent_adapter

migrations:
  on_start: validate
```

Секрет остаётся только в installer-managed `.env` или OS secret store:

```bash
ADAPTER_DATABASE_URL=postgres://agent_adapter:<generated-secret>@127.0.0.1:5432/agent_adapter
```

Runtime может проверять (не создавать): доступен ли Postgres, существует ли нужная schema, совместима ли текущая migration version.

### Варианты установки

| Выбор                     | Результат                                                               |
| ------------------------- | ----------------------------------------------------------------------- |
| SQLite                    | Binary + SQLite WAL файл, ничего больше                                 |
| Existing Postgres         | Installer спрашивает DSN/schema, проверяет доступ, применяет migrations |
| Managed Docker Postgres   | Installer поднимает изолированный Postgres-контейнер только для adapter |
| External managed Postgres | То же, что Existing Postgres: RDS, Neon, Supabase, Cloud SQL и т.д.     |

### Обязательные безопасные правила installer

- Docker ставится только после явного выбора/подтверждения, не молча.
- Postgres по умолчанию не публикуется наружу: `127.0.0.1:5432`, лучше — только внутренняя Docker network.
- Пароли — криптографически случайные.
- `.env` — права `0600`.
- Volume не удаляется при `adapterctl uninstall` без отдельного `--purge-data`.
- Compose file и версия образа фиксируются; обновление Postgres — отдельная команда с backup.
- Installer показывает итог: порты, directory, volume, backup-команду, способ удалить.
- Если Docker уже есть — installer не трогает чужие контейнеры, сети, volumes, global Docker config.

### Статус в текущем коде agent-connector

**Реализовано** (`crates/adapterctl`). CLI-флоу `adapterctl install` с вариантами
storage (`sqlite` / `existing-postgres` / `managed-docker-postgres` /
`external-managed`) работает:

- `managed_docker.rs` — подъём изолированного Postgres-контейнера через
  bollard (network/volume/container с ownership-лейблом
  `io.agent-connector.managed=true`, `pg_isready` readiness, `--confirm-docker`,
  graceful Ctrl+C при pull);
- `install_flow.rs` — выбор storage → `SELECT 1` валидация → запись
  `adapter.yaml` + `.env` (секрет только в `.env`, в конфиге — имя
  переменной) → копирование бинаря → регистрация службы;
- `postgres_lifecycle.rs` — `backup-postgres` (atomic `.tmp`+rename) и
  `upgrade-postgres` (обязательный backup перед сменой образа);
- `platform/{linux,macos,windows}.rs` — systemd unit / launchd plist /
  sc.exe, с `start`/`stop`/`restart`/`uninstall` (`stop` останавливает,
  не удаляет службу);
- `config_template.rs` — валидация агентов через реальные типы
  `adapterd::config`.

`StorageConfig::Postgres { dsn_env, schema, max_connections }` в `config.rs`
читает DSN из env — соответствует целевому runtime-контракту, daemon не
трогает Docker.

Из canonical CLI-флоу не реализовано: установка самого Docker Engine/Compose
(installer требует уже установленный Docker, не ставит его сам) и запуск
migrations (обязанность пользователя/верхнего уровня; runtime имеет
healthcheck-путь).

## 2. Канон архитектуры adapter-core (полная спецификация от автора)

### Цель

Лёгкий адаптер, подключающий существующего мини-агента без ACP/A2A к внешним клиентам и другим агентам. Явно **не реализует**: интеллект, RAG, prompt routing, бизнес-логику агента, самостоятельное редактирование файлов, orchestration нескольких агентов — это остаётся в мини-агенте или верхнем gateway.

### 10 архитектурных принципов (canonical, дословно)

1. Protocol-neutral core — core не импортирует HTTP/Axum/SSE/WebSocket/gRPC/ACP/A2A типы.
2. Transport-neutral agent connection — core вызывает `AgentConnector`, не HTTP client напрямую.
3. Один внутренний контракт — все входы становятся `CoreCommand`, все результаты — `CoreEvent`.
4. State machine — единственный источник истины; только core меняет статус задачи.
5. Event journal до доставки — событие и состояние фиксируются раньше, чем отдаются SSE/WS/gRPC (нужно для resume/reconnect).
6. Idempotency по умолчанию — повтор `invoke` не создаёт вторую задачу.
7. Pluggable storage — memory (local/MVP) → SQLite (single node) → Postgres (production).
8. Explicit capabilities — адаптер не обещает streaming/cancel/files, если underlying agent их не поддерживает.
9. Secure by profile — local не открывает сеть; remote требует TLS и авторизацию.
10. Расширение добавлением модуля — новый transport = новая реализация trait, не `if transport == ...` по всему коду.

### Границы системы

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

### Режимы развёртывания

**Local profile**: одна машина/контейнер/сосед-под. `stdio`/Unix socket, listener выключен или `127.0.0.1`, storage memory/SQLite, auth между adapter и agent не требуется при доверенной process boundary.

**Remote profile**: разные хосты, cloud gateway, multi-user. External A2A — HTTPS + JSON-RPC/REST + SSE; adapter→agent — HTTPS + bearer, позже mTLS; storage Postgres; auth signed token/OIDC/mTLS с policy per caller; observability metrics/tracing/audit.

Для агента за NAT — future `remote-connect` mode: локальный sidecar сам инициирует исходящее WSS/mTLS соединение к gateway, входящий порт на машине агента не нужен.

### Внутренний контракт core (canonical Rust types)

```rust
pub type TaskId = uuid::Uuid;
pub type SessionId = uuid::Uuid;
pub type EventSeq = u64;

pub struct InvokeRequest {
    pub task_id: TaskId,
    pub idempotency_key: String,
    pub session_id: Option<SessionId>,
    pub input: Vec<Part>,
    pub context: serde_json::Value,
    pub deadline_ms: Option<u64>,
    pub requested_capabilities: RequestedCapabilities,
}

pub enum CoreCommand {
    Invoke(InvokeRequest),
    Cancel { task_id: TaskId, reason: Option<String> },
    ProvideInput { task_id: TaskId, input: Vec<Part> },
    GetStatus { task_id: TaskId },
    Resume { task_id: TaskId, after_seq: EventSeq },
}

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

pub struct CoreEvent {
    pub task_id: TaskId,
    pub seq: EventSeq,
    pub at: chrono::DateTime<chrono::Utc>,
    pub kind: CoreEventKind,
}
```

`Part`/`ArtifactRef`/`InputRequest`/`PublicError` — простые versioned DTO. Blob-содержимое файлов **не** хранится внутри event — event содержит ссылку/metadata, сам blob лежит в `ArtifactStore`.

### Расхождение с текущим кодом: `Resume` как явная команда

Канон определяет `CoreCommand::Resume { task_id, after_seq }` как first-class команду. В текущем `adapter-core/src/lib.rs` есть только `AdapterCore::subscribe(task_id, after_seq)` как отдельный публичный метод — не вариант `CoreCommand`. Это осознанное решение: `subscribe` не мутирует state (только читает history + подписывается на live), поэтому не проходит через `dispatch()`/`authorize`-цепочку — read-path намеренно исключён из неё. Зафиксировано как известное расхождение.

### Жизненный цикл задачи — таблица переходов (canonical)

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

Повтор `Cancel` — идемпотентен. `Complete` после `Cancelled` — отвергается, логируется как protocol violation (не просто silently ignored).

**Сверка с кодом**: прочитанный мной `AdapterCore::cancel()` действительно допускает `Created, Accepted, Running, WaitingForInput → CancelRequested` — совпадает. `fail_active()` допускает переход в `Failed` из `Created, Accepted, Running, WaitingForInput, CancelRequested` — шире, чем canonical таблица (canonical говорит "non-terminal", что включает `CancelRequested`, так что это тоже совпадает). Хорошее совпадение реализации с каноном по этой части.

### Ownership правило

Только `TaskManager` (в текущем коде — эквивалент `AdapterCore`) применяет переходы состояния. Connector (в текущем коде — `AgentDriver`) не изменяет registry напрямую — отправляет кандидатов событий, а не мутирует state.

### Registry сессий/задач под нагрузкой

Разделение сущностей: **Session** (логический контекст, может содержать много задач) / **Task** (одно выполнение) / **Event** (неизменяемая запись истории) / **Idempotency record** (`(caller_id, idempotency_key) -> task_id`) / **Lease** (право одного worker управлять активной задачей).

**Расхождение с кодом: Lease не реализован.** Canonical `TaskStore` trait включает `acquire_lease`/`renew_lease` для multi-instance production — этого нет в прочитанном `adapter-store-contract`/`postgres-task-store-adapter`. Это ожидаемо, т.к. canonical roadmap помещает distributed leases в "Этап 4 — multi-instance", а текущий код — ещё в рамках single-node. Не баг, а confirmed roadmap gap.

### Concurrency правила (canonical)

1. Разные `task_id` выполняются параллельно.
2. У одной task события упорядочены `seq = 1..N`.
3. Один переход изменяет state + добавляет event в одной транзакции.
4. Postgres: `UPDATE ... WHERE revision = expected_revision` или `SELECT ... FOR UPDATE`.
5. SQLite: WAL mode, короткие transactions, ограниченный размер event payload.
6. Большие artifact bytes не хранятся в task row и не пересылаются через broadcast без лимита.
7. In-memory cache допустим как read-through cache, но durable store — source of truth в remote profile.

**Сверка**: пункт 3 подтверждён — `AdapterCore::transition()` делает `append_event_and_transition` (store) и `tx.send()` (broadcast) как единый шаг, это совпадает с каноном. Пункт 6 — **не проверено**: не видел код `Artifact`-обработки достаточно детально, чтобы подтвердить лимиты размера.

### Event fan-out и известный race — разрешено canonical-совместимым способом

Canonical явно предупреждает:

> `broadcast` не является durable delivery: подписчик может отстать и получить lag. При reconnect transport должен запросить `events_after(task_id, last_seq)` из store, затем подписаться на live events.

Это **буквально описывает источник** того race-condition бага, который был найден в `AdapterCore::subscribe` (чтение history → потом подписка, теряет событие в окне). Канон говорит "запросить history, **потом** подписаться" — то есть порядок в каноне **совпадает** с порядком в коде (history-first).

**Итоговое решение (в коде, `subscribe()`):** история читается первой, затем открывается live-receiver. Потери не возникает: `transition()` пишет в store и делает `tx.send()` последовательно в одной функции, store-write первым — событие, отправленное между чтением history и подпиской, либо уже видно в history, либо придёт через receiver как дубликат. Потребитель фильтрует дубликаты по `history_end_seq`. Lag при переполнении broadcast обрабатывается в `executor.rs` через `RecvError::Lagged` → явный resume-сигнал. Ранее предложенный фикс (subscribe-first) **не применялся** — он противоречил бы canonical design intent.

### Connector abstraction (canonical, dословно как `AgentConnector`)

```rust
pub trait AgentConnector: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> ConnectorCapabilities;
    async fn health(&self) -> Result<Health, ConnectorError>;
    async fn start(&self, request: InvokeRequest) -> Result<ConnectorEventStream, ConnectorError>;
    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), ConnectorError>;
    async fn cancel(&self, task_id: TaskId) -> Result<(), ConnectorError>;
    async fn resume(&self, task_id: TaskId, after_seq: EventSeq) -> Result<Option<ConnectorEventStream>, ConnectorError>;
}
```

**Сверка с кодом**: текущий `AgentDriver` trait (в `adapter-core/src/lib.rs`) имеет `id`, `capabilities`, `health`, `invoke`, `cancel`, `provide_input` — очень близко, но **нет метода `resume`** на уровне driver. Это соответствует тому, что resume в текущем коде реализован только на уровне `AdapterCore::subscribe` (читает store history), не требуя от driver повторной трансляции событий — архитектурно разумная разница. Имя `AgentConnector` (canonical) переименовано в `AgentDriver` (implementation); терминологический ребрендинг crates остаётся отложенным решением.

Дополнительно в `driver-mcp` реализована hot-update capabilities: `on_tool_list_changed` (типизированный метод rmcp 0.8.5) → mpsc-сигнал → background-задача → re-discovery + `RegisteredAgent.update_skills()` (ADR-0001 Решение 1).

### Protocol adapters — единый `AdapterService`

```rust
pub trait AdapterService: Send + Sync {
    async fn dispatch(&self, caller: Caller, command: CoreCommand) -> Result<DispatchResult, CoreError>;
    async fn subscribe(&self, task_id: TaskId, after_seq: EventSeq) -> Result<TaskSubscription, CoreError>;
}
```

**Сверка**: это **точно совпадает** с публичным API `AdapterCore` (`dispatch`, `subscribe`), которое я читал — имена методов и сигнатуры почти идентичны. Хорошее подтверждение, что реализация следует канону на этом уровне.

### Transport discovery/fallback — не реализовано в коде

Canonical описывает `transport_policy` config с `prefer`/`allow_fallback`/`require_tls`, discovery через `GET /.well-known/agent-adapter.json`, безопасный fallback **только до `Accepted`** (после — только reconnect/resume, никогда новый invoke, иначе side effect может выполниться дважды). В прочитанном `config.rs` нет `transport_policy` секции и нет discovery manifest endpoint. Это подтверждённый roadmap gap, не баг.

### Security — сверка с текущим состоянием

Canonical для remote profile требует: TLS обязательно, mTLS как production recommendation adapter→agent, caller identity через OIDC/JWT/API token, credential provider abstraction, capability-based policy per caller, rate limit/concurrency quota per caller/tenant.

**Текущее состояние:**

- **Bearer-token auth — реализовано.** `BearerTokenPolicy` (production-ready `PolicyEngine`) + `TokenGrant` (caller_id + allowed_scopes), `AuthConfig` в конфиге задаёт имена env-переменных с токенами. В `adapterd::main.rs` middleware `require_bearer_auth` защищает JSON-RPC; `agent_card` и `health` остаются публичными. `AllowAllPolicy` — default при пустой `auth:` секции (local profile).
- **Per-caller concurrency quota — реализовано.** `CoreInner.per_caller_permits: DashMap<CallerId, Arc<Semaphore>>` через `AdapterCore::with_caller_quota(max_concurrent, default_caller_max_concurrent)`; `remote` profile использует его. Проверяется до global и per-agent лимитов.
- **TLS**: HTTP/SSE endpoint агентов требует `https://` (кроме `allow_http_development`), MCP HTTP — то же. mTLS, OIDC/JWT — не реализовано (roadmap).

### Структура workspace — расхождение в именах crates

Canonical:

```text
agent-adapter/
├── crates/
│   ├── adapter-model/
│   ├── adapter-core/
│   ├── adapter-storage-memory/
│   ├── adapter-storage-sqlite/
│   ├── adapter-storage-postgres/
│   ├── connector-http-sse/
│   ├── connector-stdio/
│   ├── connector-unix-socket/
│   ├── connector-websocket/     # future
│   ├── connector-grpc/          # future
│   ├── protocol-a2a/
│   ├── protocol-acp/
│   ├── adapter-config/
│   └── adapterd/
```

Текущий реальный repo (`GG-QandV/agent-connector`):

```text
crates/
├── adapter-model
├── adapter-store-contract      # canonical: adapter-storage-* (раздельные crates per backend)
├── adapter-core
├── protocol-a2a-mapper         # canonical: protocol-a2a (без разделения mapper/server)
├── protocol-a2a-server         # доп. crate, не в каноне явно, но логичен как wire-layer
├── protocol-acp-mapper
├── protocol-acp-runtime        # аналогично
├── driver-stdio                # canonical: connector-stdio
├── driver-http-sse             # canonical: connector-http-sse
├── driver-mcp                  # MCP client driver (rmcp 0.8.5, stdio + HTTP)
├── memory-task-store           # canonical: adapter-storage-memory
├── sqlite-task-store-adapter   # canonical: adapter-storage-sqlite
├── postgres-task-store-adapter # canonical: adapter-storage-postgres
├── adapterd                    # daemon binary; config.rs ре-экспортируется через lib.rs
└── adapterctl                  # installer / service manager CLI
```

Наименование `driver-*` вместо canonical `connector-*` и `*-task-store-adapter` вместо `adapter-storage-*` — то же самое "adapter vs connector" терминологическое расхождение. Canonical документ явно использует "connector" как имя абстракции — это подтверждает решение назвать репозиторий `agent-connector`, но код внутри до сих пор использует старую терминологию `driver`/`adapter-core`. Ребрендинг crates остаётся отложенным решением, как и было зафиксировано ранее.

Отсутствует в текущем коде: `connector-unix-socket`, `connector-websocket`, `connector-grpc` — ожидаемые roadmap gaps по этапам 3-5. `adapter-config` как отдельный crate не выделен (конфиг — часть `adapterd`, ре-экспорт через `lib.rs` для `adapterctl`).

### Scheduler/backpressure — сверка

Canonical:

```rust
pub struct Scheduler {
    global: tokio::sync::Semaphore,
    per_caller: DashMap<CallerId, Arc<Semaphore>>,
    per_agent: DashMap<AgentId, Arc<Semaphore>>,
}
```

**Сверка**: текущий код имеет `global_permits: Semaphore` на уровне `CoreInner`, `permits`/`queue_permits: Semaphore` на уровне `RegisteredAgent` (per-agent) **и** `per_caller_permits: DashMap<CallerId, Arc<Semaphore>>` (per-caller quota через `with_caller_quota`). Все три уровня canonical `Scheduler` покрыты. Порядок проверки в `invoke()`: queue → per-caller → global → per-agent — один caller не может вытеснить остальных при `remote` profile (`with_caller_quota`).

### Observability — не проверено

Canonical требует `/healthz`/`/readyz` (реализовано, подтверждено), structured JSON logs без prompt/output по умолчанию (не проверено — не видел logging statements достаточно широко), OpenTelemetry spans и Prometheus metrics для production (явно не реализовано — не видел упоминаний `opentelemetry`/`prometheus` crates в коде).

### План реализации по этапам (canonical roadmap) — маппинг на текущий статус

| Этап                                | Canonical содержимое                                                                                         | Текущий статус в agent-connector                                                                                           |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| 0 — contracts и тесты               | model, transition tests, fake connector, memory store                                                        | ✅ Есть (adapter-model, memory-task-store); transition tests как unit tests — не подтверждено полное покрытие               |
| 1 — local MVP                       | StdioConnector, in-memory registry, core invoke/cancel/status, ACP stdio adapter                             | ✅ driver-stdio, AgentRegistry, AdapterCore, protocol-acp-runtime — все есть                                                |
| 2 — remote MVP                      | HttpSseConnector, A2A HTTP/SSE server, bearer auth, SQLite journal, resume by seq, idempotency               | ✅ Полностью: driver-http-sse, protocol-a2a-server, **bearer auth (BearerTokenPolicy)**, SQLite, idempotency, resume by seq |
| 3 — reliable single-node production | retry/reconnect policy, leases, resource quotas, artifact store interface, OTel/metrics, Unix socket sidecar | ⚠️ Частично: **per-caller quota реализована**; нет leases, artifact store abstraction, OTel, connector-unix-socket         |
| 4 — multi-instance                  | Postgres store, distributed leases, durable outbox, shared artifact store, mTLS/OIDC                         | ⚠️ Postgres store есть (не прогнан против живого PG), но без leases это не даёт реальной multi-instance safety             |
| 5 — new transports                  | WebSocket connector, gRPC connector, manifest discovery                                                      | ❌ Не начато                                                                                                                |

### Критерии готовности MVP (canonical, дословно) — сверка

1. Агент с `POST /tasks` + SSE events подключается только URL и token. — ✅ URL + bearer token (BearerTokenPolicy).
2. A2A caller может discover adapter и выполнить задачу. — ✅ Agent card + JSON-RPC подтверждены.
3. Повторный invoke с тем же idempotency key возвращает исходный task. — ✅ Подтверждено (`create_or_get_idempotent`).
4. SSE reconnect с `last_seq` не теряет и не дублирует durable events. — ✅ Решено canonical-совместимым способом: history-first + `history_end_seq` фильтр дубликатов + `Lagged` → resume.
5. Cancel безопасен при повторе. — ✅ `cancel()` идемпотентен по коду (terminal state check в начале).
6. Невозможные переходы состояния отклоняются. — ✅ `transition()` проверяет `allowed_states`.
7. Одновременно выполняются независимые tasks, события одной task упорядочены. — ✅ Подтверждено семафорами (включая per-caller) + per-task broadcast channel.
8. При падении SSE после `Accepted` задача не перезапускается автоматически. — ✅ Подтверждено комментарием в `executor.rs` ("disconnect не отменяет task").
9. Prompt, token и artifact content не появляются в default logs. — ❓ Не проверено.
10. Новый connector можно добавить как отдельный crate без изменения adapter-core. — ✅ Архитектурно верно (trait-based), driver-mcp добавлен без правки core.

## 3. Итоговые действия с учётом канона — актуальный статус

1. **Race-condition в `subscribe()`** — ✅ **Решено.** Применён canonical-совместимый порядок (history-first) + фильтр дубликатов `history_end_seq` + `Lagged` → resume в `executor.rs`. Ранее предложенный fix (subscribe-first) отклонён как противоречащий канону.
2. **Per-caller quota** — ✅ **Реализовано.** `per_caller_permits` через `AdapterCore::with_caller_quota`, используется в remote profile.
3. **`adapterctl install`** — ✅ **Реализовано** (`crates/adapterctl`): storage-профили, managed Docker Postgres, service manager (systemd/launchd/sc.exe), backup/upgrade.
4. **Аутентификация remote profile** — ✅ **Реализовано.** `BearerTokenPolicy` + `TokenGrant`, middleware в adapterd; `AllowAllPolicy` — только default для local.
5. **Терминологический ребрендинг** `driver-*`→`connector-*` — ⏸ Отложено (осознанно, репозиторий уже `agent-connector`).
6. **`CoreCommand::Resume`** — ⏸ Решено оставить `subscribe()` отдельным read-путём вне `dispatch()`/`authorize` (задокументировано в §2).
7. **MCP hot-update skills** (`tools/list_changed`) — ✅ Реализовано (ADR-0001 Решение 1): `on_tool_list_changed` → `RegisteredAgent.update_skills()`.
8. **MCP драйвер** — ✅ driver-mcp (stdio + HTTP, progress, cancel, input-schema валидация, проверка версии протокола).
