# Задание локальному агенту: A2A/ACP wire servers (уточнённая версия, с подтверждённым SDK API)

## Зафиксированные факты (проверено через GitHub code search)

```text
Repository:      github.com/a2aproject/a2a-rs
Pinned commit:   02ee56024a485a5f184cbc55d1706918ee1ff809
```

Крейты SDK:

| Crate | Назначение |
|---|---|
| `a2a` | core types, errors, events, JSON-RPC types, wire serde |
| `a2a-server` | async server framework на `axum`, REST + JSON-RPC bindings |
| `a2a-client` | async client, transport negotiation через agent card |
| `a2a-pb` / `a2a-grpc` / `a2a-slimrpc` | protobuf/gRPC/SLIMRPC — вне текущего scope |
| `a2acli` | CLI клиент — вне текущего scope |

Подтверждённые сигнатуры в `a2a-server`:

```rust
// a2a-server/src/agent_card.rs
// Route: /.well-known/agent-card.json  (константа WELL_KNOWN_AGENT_CARD_PATH)
pub fn agent_card_router<P: AgentCardProducer>(producer: Arc<P>) -> axum::Router;

// a2a-server/src/jsonrpc.rs
// POST "/" — JSON-RPC endpoint; state = JsonRpcState { handler }
pub fn jsonrpc_router<H: RequestHandler>(handler: Arc<H>) -> axum::Router;

// a2a-server/src/rest.rs
pub fn rest_router<H: RequestHandler>(handler: Arc<H>) -> axum::Router;

// examples/src/lib.rs
pub trait AgentExecutor {
    fn execute(&self, ctx: ExecutorContext /* , ... */);
}
```

`GetExtendedAgentCardRequest` / `GET_EXTENDED_AGENT_CARD` существуют как JSON-RPC method — SDK поддерживает extended agent card поверх базового discovery route.

## Обязательный шаг перед кодом

Локальный агент должен клонировать/открыть `a2a-server` crate целиком на pinned commit и прочитать полностью:

```text
a2a-server/src/agent_card.rs
a2a-server/src/jsonrpc.rs
a2a-server/src/rest.rs
a2a-server/src/handler.rs      # RequestHandler trait — сюда попадает бизнес-логика
examples/src/lib.rs            # AgentExecutor trait, ExecutorContext
examples/src/helloworld/server.rs
examples/tests/transports_e2e.rs
```

Из `handler.rs` и `jsonrpc.rs` выписать: точный набор методов `RequestHandler`, точные request/response/error типы, точный error type `A2AError` и его конструкторы (`A2AError::unsupported_operation(...)` подтверждён). Из `agent_card.rs` выписать точный `AgentCardProducer` trait.

Запрещено писать `protocol-a2a-server` до того, как этот список полностью прочитан и типы скопированы буквально (имена методов, порядок параметров, тип возврата).

## Правильный canonical discovery path

```text
GET /.well-known/agent-card.json
```

Не `/agents/:id/.well-known/agent.json` — это неверный путь из более раннего черновика задания. Если нужна карточка на per-agent path (`/agents/:id/...`), это custom multi-agent routing поверх `agent_card_router`, а не часть самого SDK; такой routing нужно явно задокументировать как расширение поверх стандартного discovery endpoint.

## Архитектура интеграции

```text
axum::Router (a2a-server: agent_card_router + jsonrpc_router [+ rest_router опционально])
        ↓ implements
RequestHandler  (наша реализация)
        ↓ calls
protocol-a2a-mapper
        ↓ calls
AdapterCore
```

Наша задача — **реализовать** `RequestHandler` (и/или `AgentExecutor`, если SDK строит handler поверх executor — это нужно подтвердить чтением `handler.rs`), не переизобретая JSON-RPC dispatch, SSE framing или Agent Card serialization: это уже делает `a2a-server`.

### `protocol-a2a-server` crate

```rust
pub struct AdapterRequestHandler {
    core: Arc<AdapterCore>,
    mapper: /* protocol-a2a-mapper types */,
}

impl RequestHandler for AdapterRequestHandler {
    // методы — точно как в handler.rs; не выдумывать
}

pub fn build_router(
    handler: Arc<AdapterRequestHandler>,
    card_producer: Arc<AdapterCardProducer>,
) -> axum::Router {
    agent_card_router(card_producer)
        .merge(jsonrpc_router(handler))
        // health/readiness добавляются отдельным merge, см. ниже
}
```

`AdapterCardProducer` реализует `AgentCardProducer` и строит `AgentCard` из agent registry/config (id, skills, endpoint, auth scheme), не из hardcoded JSON.

### Ошибки

Все domain-ошибки `AdapterCore` конвертируются в `A2AError` через mapper, используя реальные конструкторы из `a2a` crate (`A2AError::unsupported_operation`, и другие — дочитать полный список в `a2a/src/error.rs` или аналогичном файле перед реализацией). Никаких строковых `"error"` полей мимо typed `A2AError`.

## Health / readiness

SDK не предоставляет health/readiness — это наш собственный router, который **merge**-ится с `a2a-server` router-ами:

```rust
fn health_router(state: HealthState) -> axum::Router {
    axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(readyz))
        .with_state(state)
}
```

`/healthz` — процесс жив, без I/O к storage.
`/readyz` — конфигурация валидна, `TaskStore` доступен, registry не пуст, daemon не в shutdown/draining — возвращает 503 иначе, без утечки DSN/token/path в теле ответа.

## ACP stdio runtime

Для ACP пока не подтверждён отдельный Rust SDK crate в текущем workspace — это отдельный от `a2a-rs` протокол (`agentclientprotocol.com`, JSON-RPC 2.0 over stdio, newline-delimited, без embedded newlines в одной строке). Перед реализацией:

1. Подтвердить, есть ли официальный/сторонний Rust crate ACP schema types, или нужен собственный typed слой по JSON schema из `agentclientprotocol.com/protocol/v1/schema`.
2. Если готового crate нет — `protocol-acp-runtime` реализует минимальный typed JSON-RPC 2.0 stdio loop самостоятельно, использует `protocol-acp-mapper` для перевода в команды `AdapterCore`, и пишет только валидные ACP JSON-RPC строки в stdout; все логи — в stderr через `tracing`.

Это отдельная задача от A2A; не смешивать typed error handling A2A (`A2AError`) с ACP JSON-RPC error object — они разные протоколы с разной схемой ошибок.

## Тесты

### A2A

Использовать реальные типы SDK в in-process tests (`axum::Router` + `tower::ServiceExt::oneshot`, без реального TCP, кроме одного smoke bind test):

- `GET /.well-known/agent-card.json` возвращает валидный `AgentCard` JSON, соответствующий реально зарегистрированным агентам.
- Ровно один JSON-RPC method end-to-end: request → `AdapterRequestHandler` → mapper → `AdapterCore` (test store + test driver) → response, с сохранением request `id`.
- Unsupported method возвращает `A2AError::unsupported_operation` в правильном JSON-RPC error envelope.
- `GetExtendedAgentCardRequest`, если реализован, возвращает `AgentCard` либо явный `unsupported_operation`, если extended card не поддерживается текущим scope.
- `/healthz` и `/readyz` — 200/503 по описанным выше условиям, смонтированы в тот же router без конфликта путей с `a2a-server` routes.

### ACP

- In-memory async stdin/stdout harness (без реального процесса).
- Valid JSON-RPC line → одна корректная response line с тем же id.
- Malformed line → JSON-RPC parse error, loop продолжает работу.
- Notification (без `id`) → нет ответа в stdout.
- stdout никогда не содержит tracing/log вывода — только protocol JSON lines.

## Acceptance criteria

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

В PR обязательно указать:

- какие именно файлы `a2a-server`/`a2a` были прочитаны и на каком commit;
- итоговый список поддержанных JSON-RPC methods с явной пометкой "supported" / "explicitly unsupported (`unsupported_operation`)";
- подтверждение, что `/.well-known/agent-card.json` — единственный discovery path, либо обоснование multi-agent routing поверх него;
- статус ACP: использован сторонний crate или собственная typed реализация, и почему.
