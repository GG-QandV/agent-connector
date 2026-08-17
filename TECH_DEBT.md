# TECH_DEBT

## Открытые

### 2026-08-17: ручной парсер SDK-wire вместо типизированного a2a::Task (пункт 2, ТЗ §2.4.3)
- **Что**: `crates/driver-a2a-client/src/wire/sdk.rs::parse_task` парсит ответ вручную через `serde_json::Value`, хотя SDK предоставляет типизированные `a2a::Task`/`TaskState`/`Part` (`a2a/src/types.rs`).
- **Почему осознанный отказ (не недоделка)**: крейт `a2a` (workspace dep, pinned `02ee560`) — **v0.3.0, pre-1.0**, без `#[non_exhaustive]` ни на одном типе, в `types.rs` всего 3 коммита. SDK может без предупреждения добавить вариант enum или обязательное поле (например у `Task.context_id` нет `#[serde(default)]`) — типизированный парсер тогда ломается целиком, ручной — нет.
- **Impact**: low (рабочий код, покрыт тестами); риск — расхождение с форматом при будущих версиях SDK.
- **Триггер закрытия**: новая версия SDK → перейти на `a2a::Task` в `sdk.rs` (проверить `context_id` и `#[non_exhaustive]` в новых версиях).

### 2026-08-17: detect_from_agent_card нефункционален (пункт 3, ТЗ §3.2 п.4 / DoD §3.5)
- **Что**: `crates/driver-a2a-client/src/dialect_probe.rs::detect_from_agent_card` всегда возвращает `None`. Канал отменён владельцем (решение от 2026-08-17): маппинг `protocolVersion` → wire-диалект семантически ошибочен (версия протокола ≠ выбор wire-реализации). Ключевое определение диалекта — зонд (`probe_wire_format`), он корректен. Порядок `card → probe` в `resolve_auto_wire()` сохранён как точка расширения.
- **Почему**: спека AgentCard не содержит поля, надёжно отличающего wire-реализацию.
- **Impact**: none (карточка не участвует в резолюции, зонд всё решает корректно).
- **Триггер закрытия**: новая версия спеки AgentCard с полем, различающим wire-реализацию → реализовать детект в `detect_from_agent_card`, порядок в `resolve_auto_wire()` уже готов.

## Закрыто

### 2026-08-17: живой E2E spec/auto через шлюз → hermes (ТЗ §2.6 п.4)
- **Закрыто**: `crates/driver-a2a-client/tests/e2e_live.rs` (ignored-по-умолчанию): spec-wire invoke → Completed («E2E_OK»), auto-wire зонд на реальном шлюзе → Completed («E2E_AUTO»), smoke с bounded timeout. Проверено живьём: 3 passed. Запуск: `cargo test -p driver-a2a-client --test e2e_live -- --ignored --nocapture`.
- **SDK-ветка закрыта** (коммит `9d057a5`): driver (sdk) → adapterd (SDK-сервер) → A2aClient-агент (spec) → шлюз → hermes, Completed «SDK_OK». Попутно исправлен sdk-wire парсер (конкатенация всех status.message.parts, регрессия-тест). 4/4 live passed.
- **Инфраструктура E2E** (не в репо): шлюз `/tmp/gateway-e2e/config.yaml` (hermes-main = `hermes acp`, токен `t-e2e-001`, порт 8348), adapterd `/tmp/adapter-e2e-sdk.yaml` (агент a2a-client spec на шлюз, порт 8349, `E2E_GW_TOKEN=t-e2e-001`).

### 2026-08-17: коды ошибок DriverEvent не соответствовали ТЗ §2.5
- **Закрыто**: `a2a_task_failed` → `a2a_remote_error` (task Failed/Rejected); `a2a_call_failed` разделён через `send_error_to_a2a_code` на `a2a_no_task` (нет result/task) и `a2a_remote_error` (прочее). 4 теста фиксируют маппинг. Коммит `9166c52`.

### 2026-08-17: дефекты распознавания диалекта D1–D4
- **Закрыто** (коммит `9ba5bb9`): D1 — зонд распознаёт `-32601` (стандарт JSON-RPC) и нормализованный текст «method not found» (три варианта формулировки); D2 — честный `None` для AgentCard (см. открытый пункт выше); D3 — one-shot retry при MethodNotFound на реальном вызове (полная инвалидация кэша OnceCell — TODO); D4 — интеграционный тест восстановления.
