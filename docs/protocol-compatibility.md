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

Mappers реализованы (semantic DTO ↔ Core). Wire-слои реализованы частично:

- **A2A HTTP JSON-RPC/SSE server** — реализован (`protocol-a2a-server`:
  `build_router`, executor, card, health/auth, task_store) и подключён в
  `adapterd` (`main.rs`, `build_router`), включая `/healthz`/`/readyz`.
- **ACP stdio JSON-RPC loop** — реализован как библиотека (`protocol-acp-runtime`:
  `AcpRuntime`, `codec`), но запуск отдельным процессом/профилем **отложен**
  (см. `operations.md`). ACP — унаследованная ниша без развития (стратегия §9.3),
  поэтому интеграция loop'а в `adapterd` не приоритетна.

Mappers спроектированы так, чтобы SDK-обновление меняло только тонкую
boundary-прослойку, не Core/stores/drivers.

## Стратегия диалектов A2A (2026)

Протокольная стратегия обоих продуктов (шлюз + адаптер) зафиксирована в
`docs/A2A-protocol-strategy-2026.md` (версии EN/UK/RU — рядом, `.summary.md` —
краткие резюме; единое ТЗ — `docs/TZ-a2a-dialects-gateway-adapter.md`):

1. **База — A2A SDK (v1.0, ProtoJSON):** `SendMessage`/`GetTask`/`CancelTask`.
2. **Fallback — A2A Spec (pre-1.0):** `message/send`/`tasks/get` — совместимость
   со старыми клиентами (Python `a2a-sdk` и др.).
3. **Deep fallback — ACP:** только унаследованные инсталляции, без развития.
4. **ANP (W3C DID)** — отдельная ниша, вне scope.

Влияние на wire-слои адаптера: `driver-a2a-client` получает `wire_format: auto`
(диалект-зонд + кэш на эндпоинт, приоритет SDK); `protocol-a2a-server` — приём
Spec на входе. Детали и DoD — в §9.2 документа стратегии.
