Готов ADR-0002. Ключевая идея, ради которой всё построено: **MCP-сервер повторяет ровно тот же структурный паттерн**, что уже доказан в двух существующих серверах — тонкий mapper, без собственной копии бизнес-логики.

## Что экспонируется, а что явно нет

`tools/list` собирается из `AgentRegistry::agents()` → каждый `skill` каждого `RegisteredAgent` становится MCP `Tool` с именем `{agent_id}.{skill}`. `tools/call` маппится на `AdapterCore::dispatch(Invoke)` + `subscribe()` — тот же паттерн, что уже использует `AdapterAgentExecutor` для A2A через `futures_util::stream::unfold`.executor.rs

Сознательно исключены из v1: `resources/*`/`prompts/*` (заблокировано нерешённой Проблемой 2 из ADR-0001), `sampling/createMessage` (agent-connector не хост модели), и полный `InputRequired` через MCP (заблокировано SEP-1686 feature-gate из ADR-0001 Решение 3) — вместо тихого зависания MCP-сервер явно возвращает `CallToolResult{isError: true}` с понятным сообщением.

## Как избежать тройного дублирования

Таблица RACI в документе фиксирует главное правило: **task lifecycle, idempotency и concurrency limits живут только в `AdapterCore`**, каждый protocol crate (`a2a-server`, `acp-runtime`, новый `mcp-server`) отвечает только за свой wire-формат и streaming-адаптацию. Единственный новый stateful компонент — `progress_bridge` для корреляции `progress_token` ↔ `TaskId`, который зеркалит `ExecutionManager` из A2A-слоя, а не изобретает свою логику хранения task state.sdk_a2a_server_handler.rs

Предложенный скелет crate — `crates/protocol-mcp-server/` с `tool_catalog.rs`, `call_mapper.rs`, `progress_bridge.rs` — сохраняет ту же трёхчастную структуру, что уже видна в `protocol-a2a-server` (`executor.rs` + `sdk_a2a_server_handler.rs` с `ExecutionManager`). Готов при необходимости написать первый черновик `tool_catalog.rs` (самая простая, наименее спорная часть — просто read-only маппер `AgentRegistry` → `Vec<Tool>`), если хотите начать реализацию с него.
