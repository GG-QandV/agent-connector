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

| Выбор | Результат |
|---|---|
| SQLite | Binary + SQLite WAL файл, ничего больше |
| Existing Postgres | Installer спрашивает DSN/schema, проверяет доступ, применяет migrations |
| Managed Docker Postgres | Installer поднимает изолированный Postgres-контейнер только для adapter |
| External managed Postgres | То же, что Existing Postgres: RDS, Neon, Supabase, Cloud SQL и т.д. |

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

**Не реализовано.** В прочитанном мной `crates/adapterd/src/main.rs`/`config.rs` нет `adapterctl` бинарника и нет installer-flow вообще. `StorageConfig::Postgres { dsn_env, schema, max_connections }` в `config.rs` **уже правильно** соответствует целевому runtime-контракту (читает DSN из env, не создаёт Docker-ресурсы) — это хорошо совпадает с каноном. Но сам `adapterctl install` как отдельный инструмент — открытая задача, ничего из CLI-флоу выше не начато.

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

Канон определяет `CoreCommand::Resume { task_id, after_seq }` как first-class команду. В прочитанном мной `adapter-core/src/lib.rs` есть только `AdapterCore::subscribe(task_id, after_seq)` как отдельный публичный метод — не вариант `CoreCommand`. Функционально близко, но не идентично: canonical-модель предполагает, что resume идёт через тот же `dispatch()`-путь, что и остальные команды (с policy-check и unified error handling), а текущий код обходит `dispatch()` для subscribe. Это стоит явно сверить и решить: либо привести `subscribe` под `CoreCommand::Resume`, либо явно задокументировать, почему subscribe — отдельный путь (вероятная причина: subscribe не мутирует state, значит не нуждается в той же authorize-цепочке — но тогда `PolicyEngine::authorize` не защищает read-путь, что тоже нужно явно решить, особенно ввиду открытого пробела с аутентификацией).

### Жизненный цикл задачи — таблица переходов (canonical)

| Команда/событие | Разрешено из | Новое состояние |
|---|---|---|
| `Invoke` | нет задачи | `Created`, затем `Accepted` |
| connector `Accepted` | `Created` | `Accepted` |
| connector `Progress` | `Accepted`, `Running` | `Running` |
| connector `InputRequired` | `Accepted`, `Running` | `WaitingForInput` |
| `ProvideInput` | `WaitingForInput` | `Running` |
| `Cancel` | non-terminal | `CancelRequested` |
| connector `Cancelled` | `CancelRequested` | `Cancelled` |
| connector `Completed` | `Accepted`, `Running`, `WaitingForInput` | `Completed` |
| connector `Failed` | non-terminal | `Failed` |

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

### Event fan-out и известный race — теперь с полным контекстом канона

Canonical явно предупреждает:

> `broadcast` не является durable delivery: подписчик может отстать и получить lag. При reconnect transport должен запросить `events_after(task_id, last_seq)` из store, затем подписаться на live events.

Это **буквально описывает источник** того race-condition бага, который я нашёл в `AdapterCore::subscribe` (чтение history → потом подписка, теряет событие в窗). Канон говорит "запросить history, **потом** подписаться" — то есть порядок в каноне **совпадает** с порядком в текущем багованном коде (history-first), а не с моим предложенным фиксом (subscribe-first)!

Это меняет диагноз: я должен явно пересмотреть свой фикс. Если canonical порядок — history-first, то защита от потери событий должна быть другой: не переставлять порядок операций, а либо (а) делать `events_after` и `tx.subscribe()` в одной атомарной операции относительно `active` map (например, под одним `DashMap`-guard, не через два отдельных `.get()` вызова), либо (б) принять, что broadcast lag — ожидаемый canonical trade-off, и полагаться на `RecvError::Lagged` → explicit resume-signal (что текущий код **уже делает** в `executor.rs`) как основной механизм защиты, а не пытаться устранить окно гонки полностью на уровне `subscribe()`.

**Это меняет приоритет моего предыдущего фикса.** Мой `lib_fix_subscribe_race.rs` может быть избыточным или даже неверным относительно canonical design intent — нужно явно решить, какая стратегия принята: "no gap" (мой fix) vs "explicit lag detection + resume" (canonical + текущий код в `executor.rs`). Рекомендую: если canonical документ — source of truth, то правильный fix — не переставлять порядок в `subscribe()`, а сделать чтение history и подписку атомарными относительно вставки в `active` DashMap (например, через短-lived lock/guard на конкретной записи), либо просто задокументировать lag как expected behavior, покрытый `Lagged` handling.

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

**Сверка с кодом**: текущий `AgentDriver` trait (в `adapter-core/src/lib.rs`) имеет `id`, `capabilities`, `health`, `invoke`, `cancel`, `provide_input` — очень близко, но **нет метода `resume`** на уровне driver. Это соответствует тому, что resume в текущем коде реализован только на уровне `AdapterCore::subscribe` (читает store history), не требуя от driver повторной трансляции событий — архитектурно разумная разница, но стоит явно отметить, что имя `AgentConnector` (canonical) переименовано в `AgentDriver` (implementation) без объяснения в коде.

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

### Security — сверка с открытым пробелом аутентификации

Canonical для remote profile требует: TLS обязательно, mTLS как production recommendation adapter→agent, caller identity через OIDC/JWT/API token, credential provider abstraction, capability-based policy per caller, rate limit/concurrency quota per caller/tenant.

**Это прямое подтверждение** ранее найденного критичного пробела: `PolicyEngine` в коде имеет только `AllowAllPolicy`, значит **ни одно** из этих canonical security-требований для remote profile сейчас не реализовано. Canonical явно ожидает, что это будет сделано — значит это не "возможно нужно", а "canonically required and currently missing".

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
├── memory-task-store           # canonical: adapter-storage-memory
├── sqlite-task-store-adapter   # canonical: adapter-storage-sqlite
├── postgres-task-store-adapter # canonical: adapter-storage-postgres
└── adapterd
```

Наименование `driver-*` вместо canonical `connector-*` и `*-task-store-adapter` вместо `adapter-storage-*` — то же самое "adapter vs connector" терминологическое расхождение, которое мы уже обсуждали раньше в разговоре про переименование репозитория. Canonical документ явно использует "connector" как имя абстракции — это подтверждает решение назвать репозиторий `agent-connector`, но код внутри до сих пор использует старую терминологию `driver`/`adapter-core`. Ребрендинг crates остаётся отложенным решением, как и было зафиксировано ранее.

Отсутствует в текущем коде: `connector-unix-socket`, `connector-websocket`, `connector-grpc`, `adapter-config` как отдельный crate (сейчас конфиг — часть `adapterd`, не отдельный crate) — все ожидаемые roadmap gaps по этапам 3-5.

### Scheduler/backpressure — сверка

Canonical:

```rust
pub struct Scheduler {
    global: tokio::sync::Semaphore,
    per_caller: DashMap<CallerId, Arc<Semaphore>>,
    per_agent: DashMap<AgentId, Arc<Semaphore>>,
}
```

**Расхождение**: текущий код имеет `global_permits: Semaphore` на уровне `CoreInner` и `permits`/`queue_permits: Semaphore` на уровне `RegisteredAgent` (per-agent) — это покрывает global и per-agent, но **нет per-caller quota** (`DashMap<CallerId, Arc<Semaphore>>`). Это значит один caller теоретически может забить весь global/per-agent лимит один, вытеснив остальных — ещё один открытый gap, ранее не пойманный в моём code review, потому что я не сверял с каноном.

### Observability — не проверено

Canonical требует `/healthz`/`/readyz` (реализовано, подтверждено), structured JSON logs без prompt/output по умолчанию (не проверено — не видел logging statements достаточно широко), OpenTelemetry spans и Prometheus metrics для production (явно не реализовано — не видел упоминаний `opentelemetry`/`prometheus` crates в коде).

### План реализации по этапам (canonical roadmap) — маппинг на текущий статус

| Этап | Canonical содержимое | Текущий статус в agent-connector |
|---|---|---|
| 0 — contracts и тесты | model, transition tests, fake connector, memory store | ✅ В основном есть (adapter-model, memory-task-store); transition tests как unit tests — не подтверждено покрытие |
| 1 — local MVP | StdioConnector, in-memory registry, core invoke/cancel/status, ACP stdio adapter | ✅ driver-stdio, AgentRegistry, AdapterCore, protocol-acp-runtime — все есть |
| 2 — remote MVP | HttpSseConnector, A2A HTTP/SSE server, bearer auth, SQLite journal, resume by seq, idempotency | ⚠️ Частично: driver-http-sse и protocol-a2a-server есть, SQLite есть, idempotency есть; **bearer auth — нет** (PolicyEngine = AllowAllPolicy) |
| 3 — reliable single-node production | retry/reconnect policy, leases, resource quotas, artifact store interface, OTel/metrics, Unix socket sidecar | ❌ Не начато: no leases, no per-caller quota, no artifact store abstraction, no OTel, no connector-unix-socket |
| 4 — multi-instance | Postgres store, distributed leases, durable outbox, shared artifact store, mTLS/OIDC | ⚠️ Postgres store есть (не прогнан против живого PG), но без leases это не даёт реальной multi-instance safety |
| 5 — new transports | WebSocket connector, gRPC connector, manifest discovery | ❌ Не начато |

### Критерии готовности MVP (canonical, дословно) — сверка

1. Агент с `POST /tasks` + SSE events подключается только URL и token. — ⚠️ URL да, token — нет реального auth.
2. A2A caller может discover adapter и выполнить задачу. — ✅ Agent card + JSON-RPC подтверждены.
3. Повторный invoke с тем же idempotency key возвращает исходный task. — ✅ Подтверждено (`create_or_get_idempotent`).
4. SSE reconnect с `last_seq` не теряет и не дублирует durable events. — ⚠️ Именно здесь живёт race-condition спор выше — нужно решить canonical-совместимый fix.
5. Cancel безопасен при повторе. — ✅ `cancel()` идемпотентен по коду (terminal state check в начале).
6. Невозможные переходы состояния отклоняются. — ✅ `transition()` проверяет `allowed_states`.
7. Одновременно выполняются независимые tasks, события одной task упорядочены. — ✅ Подтверждено семафорами + per-task broadcast channel.
8. При падении SSE после `Accepted` задача не перезапускается автоматически. — ✅ Подтверждено комментарием в `executor.rs` ("disconnect не отменяет task").
9. Prompt, token и artifact content не появляются в default logs. — ❓ Не проверено.
10. Новый connector можно добавить как отдельный crate без изменения adapter-core. — ✅ Архитектурно верно (trait-based), не протестировано explicitly.

## 3. Итоговые действия, которые нужно предпринять с учётом канона

1. **Пересмотреть фикс race-condition в `subscribe()`** — canonical порядок (history-first) противоречит моему предыдущему fix (subscribe-first). Нужно явное решение: атомарность через lock на `active`-записи, или явное принятие lag+resume как canonical-approved стратегии.
2. **Добавить per-caller quota** в `Scheduler`/`CoreInner` — canonical явно требует `DashMap<CallerId, Arc<Semaphore>>`, текущий код это пропускает.
3. **Спроектировать и начать `adapterctl install`** — installer/runtime разделение принято как обязательное правило, но полностью не начато.
4. **Аутентификация remote profile** — теперь подтверждена каноном как required, не как "желательно". `AllowAllPolicy` — единственная реализация, это блокер этапа 2 canonical roadmap, не просто nice-to-have.
5. **Явно решить терминологический ребрендинг** `driver-*`→`connector-*`, `*-task-store-adapter`→`adapter-storage-*`, или явно задокументировать причину сохранения текущих имён.
6. **CoreCommand::Resume** — решить, приводить ли `subscribe()` под unified `dispatch()`-путь с policy-check, или явно документировать read-path как исключение из authorize-цепочки.
