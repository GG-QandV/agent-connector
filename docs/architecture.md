# Architecture

Agent-connector — модульный рантайм поверх **Universal Agent Integration
Contract (UAIC)**. Он предоставляет единый жизненный цикл задач независимо от
транспорта агента (stdio-процесс или удалённый HTTP/SSE) и от протокола клиента
(A2A или ACP).

## Принципы

- `adapter-model` — только DTO/идентификаторы, без рантайма.
- `adapter-core` — жизненный цикл, registry, policy; не знает SQL, HTTP,
  A2A SDK и ACP transport.
- Storage-адаптеры реализуют один `TaskStore` (`adapter-store-contract`).
- Protocol mappers (`protocol-a2a-mapper`, `protocol-acp-mapper`) — semantic
  границы между wire-форматами и Core.
- Drivers (`driver-stdio`, `driver-http-sse`) реализуют `AgentDriver`.
- Только `adapterd` связывает config, concrete storage, drivers и протоколы.

## Граф зависимостей

```text
adapter-model
      ↑
adapter-store-contract ← adapter-core
      ↑                    ↑
   storage-*        protocol-* / driver-*
                          ↑
                       adapterd
```

## Полные документы

Детальная архитектура и спеки хранятся в `docs/design/`:

- [Рантайм: архитектура](design/rust-agent-adapter-architecture.md)
- [Поcrate-спеки](design/universal-agent-adapter-module-specifications.md)
- [Производственные NFR](design/universal-agent-adapter-production-nfr.md)
- [Мультиагентная секция](design/agent-adapter-multi-agent-section.md)
- [Пины A2A SDK](design/a2a-sdk-pinned-dependencies.toml)

## Что реализовано

Workspace собран, все crates компилируются, `cargo test --workspace` зелёный.
Реализовано: A2A HTTP server (`protocol-a2a-server`, подключён в `adapterd`,
включая `/healthz`/`/readyz`), ACP stdio runtime (`protocol-acp-runtime`,
lib; запуск отдельным процессом — отложен, см. `protocol-compatibility.md`),
Postgres-миграции (`migration_guard` в `postgres-task-store-adapter`),
MCP driver (включая hot-update skills при `tools/list_changed`).
Не реализовано: `tests/integration` (пустые каталоги, только README),
`tests/contract`/`tests/fixtures` — заготовки. Статусы — в
[`operations.md`](operations.md) и [`protocol-compatibility.md`](protocol-compatibility.md).
