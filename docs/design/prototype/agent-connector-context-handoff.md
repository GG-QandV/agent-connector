# Контекст для переноса в новую сессию: agent-connector

Репозиторий: [github.com/GG-QandV/agent-connector](https://github.com/GG-QandV/agent-connector), branch `main`. [memory:1]

## Цель проекта

Универсальный, transport-agnostic Rust runtime, который хостит и связывает AI-агентов локально или удалённо — не узкий "ACP↔A2A мост", а самостоятельный переиспользуемый connector/edge-node. Изначально обсуждался как часть `ACP-A2A_gateway`, затем осознанно выделен в отдельный репозиторий с именем **agent-connector** (не "adapter" — connector лучше отражает роль связывания агентов, а не только протокольного преобразования).

## Ключевые архитектурные принципы (не менять без явного решения)

- **Framework/language-independent core.** `adapter-core` не знает про Axum, конкретный SQL, Docker, A2A SDK или ACP transport — только domain-модель задач, agent registry, лимиты, policy hook.
- **Transport как plugin, не как контракт.** stdio, remote HTTP/SSE, будущие WebSocket/gRPC — равноправные driver-реализации trait `AgentDriver`, не встроены в core.
- **Local и remote режимы равноправны.** Local — минимальная security модель (доверенный host/container), remote — полная security модель (TLS, auth, лимиты).
- **Session/task registry живёт в core**, не привязан к конкретному transport — один и тот же task lifecycle обслуживает и A2A SSE, и ACP stdio.
- **Эволюционность без rewrite.** MVP начинается с HTTPS/SSE, затем добавляются WebSocket/gRPC с transport selection и auto-switching — без переписывания `adapter-core`.
- **Два адаптера/коннектора могут общаться напрямую** peer-to-peer (Adapter A ↔ HTTPS/SSE ↔ Adapter B), без обязательного central gateway. Gateway (`ACP-A2A_gateway`) — опциональный control plane сверху: discovery, policy, audit, multi-tenant routing. Каждый connector имеет двойную роль: server (принимает задачи для своих агентов) и client (отправляет задачи другим connectors/agents).

## Workspace layout (актуальный, уже реализован)

```text
agent-connector/
├── Cargo.toml (workspace, resolver=2)
├── config/adapter.example.yaml
├── docs/{architecture,operations,protocol-compatibility}.md + docs/design/ (исходные спеки, включая переданные задания)
├── crates/
│   ├── adapter-model          — DTO/identifiers (AgentId, TaskId, CallerId, Part, TaskState, CoreEvent...)
│   ├── adapter-store-contract — TaskStore trait, StoreError, events_after, append_event_and_transition
│   ├── adapter-core           — AdapterCore, AgentRegistry, AgentDriver trait, PolicyEngine trait, лимиты, timeout, subscribe
│   ├── protocol-a2a-mapper
│   ├── protocol-acp-mapper
│   ├── protocol-a2a-server    — НОВЫЙ: AdapterAgentExecutor, AdapterTaskStore, AdapterCardProducer, health.rs
│   ├── protocol-acp-runtime   — НОВЫЙ: JSON-RPC 2.0 stdio loop, codec.rs, runtime.rs
│   ├── driver-stdio
│   ├── driver-http-sse
│   ├── memory-task-store
│   ├── sqlite-task-store-adapter
│   ├── postgres-task-store-adapter
│   └── adapterd                — main.rs (composition root), config.rs
├── tests/{contract,integration,fixtures}
├── scripts/{check.sh,run-local.sh}
└── deploy/{docker-compose.postgres.yaml,systemd/adapterd.service}
```

Зависимость `a2a`, `a2a-server`, `a2a-pb` — git-deps на `github.com/a2aproject/a2a-rs`, pinned commit `02ee56024a485a5f184cbc55d1706918ee1ff809`. Axum версия 0.8.

## История commits на main

1. `7e50720985c5d011237f055b87bd711c77a094a8` — scaffold: 11 crates из плоского прототипа, config/docs/tests/scripts/deploy layout, `adapter-core` версия помечена `adapter_core_v2_fix`.
2. `215d3c981dee20d9236347e07ae1371a8d879c70` — wire servers: добавлены `protocol-a2a-server` и `protocol-acp-runtime`, Блок 3 concurrency в `adapter-core`, tracing-subscriber в `adapterd`. Commit message утверждает живой E2E тест (`healthz`/`readyz`/`agent-card` ok).

## Что реально реализовано (подтверждено чтением кода)

### `adapter-core` (crates/adapter-core/src/lib.rs)
- `AgentDriver` trait: `id`, `capabilities`, `health`, `invoke` (возвращает stream `DriverEvent`), `cancel`, `provide_input`.
- `RegisteredAgent`: держит `permits: Semaphore` (max_concurrent_tasks) и `queue_permits: Semaphore` (max_queued_tasks) — реальные bounded semaphores, не "лучшие намерения".
- `AgentRegistry`: `DashMap`, resolve по `agent_id` → `skill_id` → fallback первый доступный.
- `PolicyEngine` trait + единственная реализация `AllowAllPolicy` (пропускает всё) — **аутентификация НЕ реализована**, это открытый гэп.
- `AdapterCore::invoke`: idempotency check → resolve agent → `create_or_get_idempotent` в store → transition to `Accepted` → acquire queue/global/per-agent permits (в этом порядке, typed reject если превышен любой лимит, permits освобождаются в обратном порядке при ошибке) → `tokio::spawn(run_driver)` с `tokio::time::timeout` вокруг — timeout реально отменяет driver через `driver.cancel()` и переводит task в failed/timeout state.
- `AdapterCore::subscribe`: **известный race-bug** — читает `history` из store, потом подписывается на `broadcast::Receiver`; событие, отправленное в этом окне, теряется. Фикс подготовлен (подписка до чтения history + dedup по seq на стороне потребителя), но не влит в repo.
- `transition()`: atomic append_event + broadcast send через store contract.

### `protocol-a2a-server` (новый crate)
- `executor.rs` — `AdapterAgentExecutor: AgentExecutor`. `execute()` создаёt task в `AdapterCore`, стримит history затем live broadcast как `StreamResponse`. `cancel()` — отдельный path. Известные проблемы: `Lagged` error возвращает generic `A2AError::internal` вместо specific resume-code; `CancelRequested`/`Cancelled` оба маппятся в один A2A `TaskState::Canceled` (неразличимы для клиента); `caller_id` — статическая строка при конструировании, **не привязана к реальному HTTP-запросу** (все remote-клиенты видны core как один и тот же caller).
- `task_store.rs` — `AdapterTaskStore: a2a_server::TaskStore`. `create`/`update` — **заглушки**, возвращают `Ok(1)` без реального persist; `get`/`list` частично реализованы (`list` — всегда пустой список, "MVP, полноценный list — отдельная задача").
- `health.rs` — `/healthz` (без I/O) и `/readyz` (проверяет draining flag, `registry.agents().is_empty()`, `task_store.ping()`).
- `card.rs`, полный `lib.rs` (`build_router`) — **не прочитаны построчно**, есть только commit diff stats (77 и 57 строк соответственно).
- Discovery path подтверждён из самого SDK: `/.well-known/agent-card.json` (не `/agents/:id/.well-known/agent.json`, как в раннем неверном плане).
- Аутентификация inbound-запросов на HTTP-уровне: **не подтверждена** — не видел middleware/extractor для Authorization header, bearer token или mTLS.

### `protocol-acp-runtime` (новый crate)
- `codec.rs` — typed JSON-RPC 2.0: `JsonRpcRequest::parse` различает malformed JSON / non-object / missing jsonrpc / missing method, извлекает `id` где возможно.
- `runtime.rs` — `AcpRuntime::run()`: построчный read loop, max_line_bytes check, `drain_token: CancellationToken`, flush после каждой записи. Методы: `initialize`, `shutdown`, `session/new|prompt|cancel|input|update|get`.
- Известные проблемы: `shutdown_grace: Duration` объявлен в конфиге, но не используется где-либо; draining молча дропает новые top-level requests без ответа клиенту (`continue` без `write_line`); `session/update` создаёт `broadcast::Receiver` через `core.subscribe`, но не читает из него — возвращает только one-shot history snapshot, то есть push-модель не реализована, хотя название метода на неё намекает.
- `caller_id` — тоже статическая строка при конструировании `AcpRuntime::new`, что нормально для локального stdio (собеседник аутентифицирован фактом owning stdin/stdout), но не подходит, если ACP когда-либо будет проксироваться через сеть.
- Тесты (5): valid request/id echo, malformed JSON → parse error → продолжение, notification → no stdout, invalid envelope → invalid_request, EOF → clean shutdown. Не покрыт: top-level non-object JSON, string/array top-level.

### `adapterd`
- `config.rs` (302 строки), `main.rs` (186 строк изначально, потом +60/-17) — **не прочитаны полностью**. Commit message: "A2A HTTP server + health на `ADAPTERD_LISTEN`; draining на SIGINT". Раньше (`7e50720`) был явный TODO/комментарий "started here as independent tasks" — по всей видимости, замещён реальным запуском в `215d3c9`, но полный текст не подтверждён.

## Известные баги/пробелы — приоритетный список для следующей сессии

| # | Компонент | Проблема | Severity | Fix готов? |
|---|---|---|---|---|
| 1 | `adapter-core::subscribe` | TOCTOU race: событие между чтением history и подпиской на broadcast теряется | 🔴 Критично | ✅ Да, файл подготовлен, не влит |
| 2 | `protocol-a2a-server::task_store.rs` | `create`/`update` не проверяют/не сохраняют реальное состояние — потенциальный silent data loss, если SDK REST-binding полагается на честный persist | 🔴 Критично (нужна проверка SDK contract, не только фикс) | ⚠️ Частично |
| 3 | **Аутентификация remote-подключений** | `AllowAllPolicy` — единственная реализация `PolicyEngine`; `caller_id` статичен, не извлекается из HTTP-запроса; не видел auth middleware в Axum router | 🔴 Критично для remote profile | ❌ Не начато |
| 4 | `protocol-acp-runtime::runtime.rs` | `shutdown_grace` не используется; draining не отвечает клиенту | 🟡 Средне | ✅ Да, подготовлен `run_with_shutdown` |
| 5 | `protocol-acp-runtime::runtime.rs` | `session/update` не push-based — вероятно недописанная фича, нужно сверить с ACP v1 spec | 🟡 Средне | ❌ Нужно сверить spec |
| 6 | `protocol-acp-runtime::codec.rs` | Недостаточное покрытие тестами (non-object top-level JSON) | 🟡 Средне | ✅ Да, тесты подготовлены |
| 7 | `protocol-a2a-server::executor.rs` | `Lagged` → generic internal error, не specific resume-code | 🟢 Незначительно | ❌ Нужно найти specific error variant в SDK |
| 8 | `protocol-a2a-server::executor.rs` | `CancelRequested`/`Cancelled` неразличимы для A2A клиента | 🟢 Незначительно | ❌ Не начато |
| 9 | `protocol-acp-runtime::runtime.rs` | `format!("{:?}", ...)` для protocol-facing строк (antipattern, ломается при переименовании enum variant) | 🟢 Незначительно | ❌ Не начато |

## Подготовленные, но не влитые фиксы (написаны, ждут ручного применения)

1. **`lib_fix_subscribe_race.rs`** — переставляет порядок в `AdapterCore::subscribe`: подписка до чтения history, плюс `TaskSubscription::last_history_seq()`.
2. **`executor_fix_dedup_seq.rs`** — `enum State::Ready` в `executor.rs` хранит `last_history_seq`, добавлен dedup loop при `recv()`.
3. **`runtime_fix_shutdown_grace.rs`** — `run_with_shutdown(CancellationToken)` с `tokio::select!` + `tokio::time::timeout(grace, ...)`, явный JSON-RPC error "server is shutting down" вместо молчаливого drop во время draining.
4. **`codec_add_test_non_object.rs`** — 3 новых теста для `codec.rs`/`runtime.rs` test module.

Все четыре файла даны как "инструкция для ручного применения" — не закоммичены, потому что запись в repo требует подтверждённого diff через `push_files`/`confirm_action`, а не парного добавления в сессии.

## Что нужно для следующего шага (приоритет)

1. **Аутентификация remote.** Спроектировать `BearerTokenPolicy`/аналог `PolicyEngine`, читающий allowed tokens из env (паттерн `{env:GW_TOKEN_MAIN}` уже проверен и работает в `ACP-A2A_gateway` — hard startup error при отсутствии env, 401 на invalid token). [memory:2] Нужен Axum middleware/extractor, который валидирует `Authorization` header **до** вызова executor и передаёт реальный per-request `caller_id`/`scopes` в `Caller`, а не статическую строку из конструктора.
2. **Влить 4 подготовленных фикса** в реальные файлы, прогнать `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`.
3. **Дочитать непрочитанные файлы**: `protocol-a2a-server/src/card.rs`, `protocol-a2a-server/src/lib.rs` (build_router целиком), `crates/adapterd/src/main.rs` целиком, `crates/adapterd/src/config.rs` — особенно важно для подтверждения TLS-требований на inbound A2A server и для правильной интеграции auth middleware.
4. **Проверить SDK contract** для `TaskStore::create`/`update`: прочитать `a2a-server/src/handler.rs`/`rest.rs` внимательно, понять точно, зависит ли `DefaultRequestHandler`/REST-binding от честного возврата версии, иначе пункт 2 в таблице багов может ждать.
5. **Сверить ACP v1 spec** (`agentclientprotocol.com/protocol/v1/schema`) на предмет push vs pull модели `session/update`.
6. **Решить архитектурно**: пока WebSocket/gRPC drivers не начаты — это следующий transport после HTTP/SSE MVP, явно запланировано в исходных целях, но не начато. [memory:1]

## Технический контекст окружения

Инструмент `github_mcp_direct::get_file_contents` в этой среде **не возвращает текст файла** — только подтверждает "successfully downloaded" с SHA. `search_code` не индексирует этот репозиторий (мал/приватный). `fetch_url` на raw.githubusercontent.com не работает (вероятно приватный репо без публичного доступа). Единственный рабочий способ получить реальный код в сессию — пользователь прикладывает файлы как attachments напрямую в диалог. Это стоит повторить в новой сессии сразу, если нужен code review или правки конкретных файлов.

## Терминология и решённые вопросы

- Название репозитория: **agent-connector** (не adapter, не gateway) — осознанное решение, connector отражает bidirectional peer-to-peer роль лучше, чем "adapter" (которое звучит как однонаправленный protocol translator).
- `ACP-A2A_gateway` — отдельный, opzionale control-plane репозиторий (`/home/gg/projects/AGENTS/ACP-A2A_gateway/`), не сливать с `agent-connector`. Gateway решает access/routing/audit; connector решает hosting/connectivity/execution.
- Внутренние Rust имена (`adapterd`, `adapter-core`, `adapter.yaml`) осознанно оставлены как есть на раннем этапе — ребрендинг в `connector-*` откладывается до первого публичного release, чтобы не создавать churn.
