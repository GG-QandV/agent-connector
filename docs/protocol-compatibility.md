# Protocol compatibility

Agent-connector — двусторонний конвертер между **ACP** (editor/CLI ↔ coding
agent, stdio JSON-RPC) и **A2A** (HTTP JSON-RPC + SSE) с единым внутренним
жизненным циклом задач (UAIC).

## Потоки

| Клиент | Направление | Транспорт |
|---|---|---|
| ACP | ACP-агент ↔ runtime | stdio JSON-RPC |
| A2A | runtime ↔ удалённый агент | HTTP JSON-RPC + SSE |
| UAIC | runtime ↔ агент (любой) | NDJSON (stdio) / HTTP+SSE |

## Semantic mapping

- `protocol-a2a-mapper` переводит A2A Task/Message/Artifact → `CoreCommand`,
  и `CoreEvent` → A2A Task/Stream events.
- `protocol-acp-mapper` переводит ACP session prompt/update → `CoreCommand`,
  и `CoreEvent` → ACP session updates.
- Drivers (`driver-stdio`, `driver-http-sse`) говорят на UAIC/1 и возвращают
  только normalized `DriverEvent`.

## UAIC/1

Единый контракт runtime ↔ агент: один JSON-объект на строку (stdio) или
`POST`/SSE-фреймы (HTTP). Пин SDK и детали — в
`design/universal-agent-adapter-module-specifications.md` (§2 UAIC).

## Status

Mappers реализованы (semantic DTO ↔ Core). **Wire-слои не реализованы**:
A2A HTTP JSON-RPC/SSE server и ACP stdio JSON-RPC loop — следующие задачи
(см. `operations.md` TODO). Mappers спроектированы так, чтобы SDK-обновление
меняло только тонкую boundary-прослойку, не Core/stores/drivers.
