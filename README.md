# agent-connector

**Version: 0.6.7**

Universal Agent Adapter Runtime — transport-neutral middleware that exposes
local (stdio) and remote (HTTP/SSE/MCP) agents through a uniform task lifecycle
with durable storage, idempotency, retention and A2A/ACP protocol mappers.

## Layout

```text
agent-connector/
├── Cargo.toml                      # workspace root (resolver = "2"), version = "0.6.7"
├── config/adapter.example.yaml     # образец конфигурации adapterd
├── crates/
│   ├── adapter-model/              # DTO, identifiers, schema (no runtime)
│   ├── adapter-store-contract/     # TaskStore trait + retention
│   ├── adapter-core/               # task lifecycle, registry, policy
│   ├── protocol-a2a-mapper/        # A2A <-> Core semantic mapper
│   ├── protocol-a2a-server/        # A2A HTTP router, health/readiness
│   ├── protocol-acp-mapper/        # ACP <-> Core semantic mapper
│   ├── protocol-acp-runtime/       # ACP stdio runtime
│   ├── driver-stdio/               # UAIC/1 NDJSON subprocess driver
│   ├── driver-http-sse/            # UAIC/1 HTTP+SSE driver
│   ├── driver-mcp/                 # MCP client driver (rmcp 0.8.5)
│   ├── memory-task-store/          # in-memory TaskStore (tests/demo)
│   ├── sqlite-task-store-adapter/  # durable single-node TaskStore
│   ├── postgres-task-store-adapter/# multi-instance TaskStore
│   └── adapterd/                   # composition root / daemon binary
├── docs/                           # entry docs + design/ (исходные спеки)
├── tests/                          # contract / integration / fixtures
├── scripts/                        # check.sh, run-local.sh
└── deploy/                         # docker-compose.postgres.yaml, systemd unit
```

## Quick start (SQLite, local)

```bash
cp config/adapter.example.yaml adapter.yaml
./scripts/check.sh                  # fmt + clippy + test
cargo run -p adapterd -- adapter.yaml
```

Adapterd стартует, создаёт `./data/adapter.db`, поднимает агентов из конфига
(stdio / HTTP+SSE / MCP-транспорт) и выполняет фоновую retention-cleanup.
Логи пишутся через `tracing`.

## Drivers

| Driver | Транспорт | Статус |
|---|---|---|
| `driver-stdio` | UAIC/1 NDJSON subprocess | готов |
| `driver-http-sse` | UAIC/1 HTTP+SSE | готов |
| `driver-mcp` | MCP (rmcp 0.8.5), stdio child-process | готов; discovery/invoke/progress/cancel через `CancellationToken` |

`driver-mcp` подключается к любому MCP-серверу через stdio, обнаруживает
инструменты через `list_tools`, вызывает их через `send_request_with_option`
+ `RequestHandle`, подписывается на progress-события через встроенный
`ProgressDispatcher`, и поддерживает cancel — `RequestHandle` остаётся внутри
spawn'нутой задачи, снаружи передаётся только `CancellationToken`, чтобы
избежать moved-value конфликта между `await_response(self)` и `cancel(self, ..)`.

## Design documents

Исходные спеки и архитектура перенесены в `docs/design/`:

- `rust-agent-adapter-architecture.md` — архитектура рантайма
- `universal-agent-adapter-module-specifications.md` — поcrate-спеки
- `universal-agent-adapter-production-nfr.md` — производственные NFR
- `agent-adapter-multi-agent-section.md` — мультиагентная секция
- `a2a-sdk-pinned-dependencies.toml` — пины A2A SDK

Краткая вводная: `docs/architecture.md`. Эксплуатация: `docs/operations.md`.
Совместимость протоколов: `docs/protocol-compatibility.md`.

## Status

Версия **0.6.7**. Workspace, crates перенесены из плоского прототипа
(`adapter_core_v2_fix.rs` → `adapter-core`, DTO → `adapter-model`),
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` проходят.

Реализовано: A2A server (`protocol-a2a-server`), ACP runtime
(`protocol-acp-runtime`), MCP driver (`driver-mcp`, rmcp 0.8.5, с progress
и cancellation через `CancellationToken`).

Следующие задачи: миграции Postgres, расширенные integration-тесты для
`driver-mcp` против реального MCP stdio-сервера, HTTP-транспорт для MCP
(сейчас поддержан только stdio child-process).
