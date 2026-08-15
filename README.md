# agent-connector

Universal Agent Adapter Runtime — transport-neutral middleware that exposes
local (stdio) and remote (HTTP/SSE) agents through a uniform task lifecycle
with durable storage, idempotency, retention and A2A/ACP protocol mappers.

## Layout

```text
agent-connector/
├── Cargo.toml                      # workspace root (resolver = "2")
├── config/adapter.example.yaml     # образец конфигурации adapterd
├── crates/
│   ├── adapter-model/              # DTO, identifiers, schema (no runtime)
│   ├── adapter-store-contract/     # TaskStore trait + retention
│   ├── adapter-core/               # task lifecycle, registry, policy
│   ├── protocol-a2a-mapper/        # A2A <-> Core semantic mapper
│   ├── protocol-acp-mapper/        # ACP <-> Core semantic mapper
│   ├── driver-stdio/               # UAIC/1 NDJSON subprocess driver
│   ├── driver-http-sse/            # UAIC/1 HTTP+SSE driver
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
и выполняет фоновую retention-cleanup. Логи пишутся через `tracing`.

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

Scaffold-коммит: workspace, crates перенесены из плоского прототипа
(`adapter_core_v2_fix.rs` → `adapter-core`, DTO → `adapter-model`),
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` проходят. A2A server, ACP runtime, миграции Postgres
и integration-тесты — следующие задачи.
