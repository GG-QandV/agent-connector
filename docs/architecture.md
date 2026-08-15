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

## Что реализовано в scaffold

Workspace собран, все crates компилируются, `cargo test --workspace` зелёный.
Нереализованные части (A2A wire server, ACP stdio runtime, миграции Postgres,
integration-тесты) — следующие итерации; статусы зафиксированы в
[`operations.md`](operations.md) и TODO.
