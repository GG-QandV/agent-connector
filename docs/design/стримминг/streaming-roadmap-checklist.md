# Streaming — Roadmap и Чеклист задач

Основа: `streaming-architecture.md`. Репозитории: `GG-QandV/agent-connector`, `GG-QandV/ACP-A2A_gateway`.

## Roadmap (фазы)

### Фаза 0 — Стабилизация тестового фундамента
Цель: зафиксировать текущие инварианты тестами до рефакторинга, чтобы регрессии были видны сразу.

### Фаза 1 — `ReliableTaskStream` в `adapter-core`
Цель: единая точка recovery для `Lagged`/gap/dup, вместо дублирования логики в каждом mapper'е.

### Фаза 2 — Исправление `driver-http-sse`
Цель: устранить дефекты SSE-клиента (manifest cache, frame-limit, backoff, error classification).

### Фаза 3 — A2A SSE транспорт до production-контракта
Цель: `id:`/`event:`/heartbeat/terminal-close на HTTP-уровне.

### Фаза 4 — Решение по ACP streaming
Цель: выбрать pull или push модель, синхронизировать с заявленными `capabilities`.

### Фаза 5 — Семантические исправления mapper'ов
Цель: устранить `session_id="unknown"`, нестабильный `request_id`, пустые `artifacts`.

### Фаза 6 — End-to-end тесты и нагрузочная проверка
Цель: reconnect/lag/gap/dup/terminal сценарии под нагрузкой, подтверждение backpressure-политики.

### Фаза 7 — Документация и ревью
Цель: обновить `docs/driver-mcp-spec.md`, `docs/architecture.md`, `docs/protocol-compatibility.md`, отразить итоговый контракт.

---

## Чеклист задач

### Фаза 0 — Тесты-фундамент
- [ ] Тест: `subscribe_does_not_miss_event_during_history_read` — событие публикуется между чтением history и открытием receiver.
- [ ] Тест: `lagged_receiver_recovers_from_durable_history` — subscriber намеренно отстаёт, получает `Lagged`, получает все события без потерь.
- [ ] Тест: `resume_is_idempotent` — повторный subscribe с тем же `after_seq` не возвращает уже подтверждённые события.
- [ ] Тест: `duplicate_sse_events_are_ignored` — дублирующийся `seq` от драйвера не доставляется дважды.
- [ ] Тест: `sequence_gap_triggers_recovery` — `seq=10` после `seq=7` инициирует durable catch-up.
- [ ] Зафиксировать текущее поведение `broadcast_overflow.rs` как baseline (уже есть, не трогать до Фазы 1). [file:9]

### Фаза 1 — `ReliableTaskStream`
- [ ] Спроектировать структуру `ReliableTaskStream { task_id, after_seq, pending: VecDeque<CoreEvent>, receiver }` в `adapter-core`.
- [ ] Реализовать `next()` по алгоритму: pending → recv → gap-detect → Lagged-catch-up → Closed → None.
- [ ] Гарантировать порядок операций в `AdapterCore::subscribe`: открыть `broadcast::Receiver` **до** чтения durable history.
- [ ] Реализовать дедупликацию между history и уже полученными live-событиями (по `seq`).
- [ ] Добавить пометку `terminal_reached: bool`, после которой `next()` всегда возвращает `None`.
- [ ] Заменить прямой `receiver.recv()` в `protocol-a2a-mapper::A2aTaskEventStream::next()` на вызов `ReliableTaskStream`. [file:54]
- [ ] Заменить прямой `receiver.recv()` в `protocol-acp-mapper::AcpUpdateStream::next()` на вызов `ReliableTaskStream`. [file:53]
- [ ] Покрыть модуль unit-тестами из Фазы 0 (перенести/адаптировать под новый API).
- [ ] Code review + PR в `agent-connector`.

### Фаза 2 — `driver-http-sse`
- [ ] Изменить `manifest` на `Arc<RwLock<Option<UaicManifest>>>`, убрать пересоздание в `clone_for_task()`. [file:51]
- [ ] Переписать учёт `max_event_bytes`: считать размер отдельного извлечённого frame, а не суммарный buffer.
- [ ] Ввести классификацию ошибок: `ConsumerClosed` / `TransportFailure` / `ProtocolFailure` / `Terminal`.
- [ ] Не выполнять reconnect при `ConsumerClosed` и после `Terminal`.
- [ ] Сбрасывать `backoff` и счётчик `attempts` после первого успешно обработанного события в сессии.
- [ ] Заменить самодельный `find_sse_boundary`/`parse_sse_frame` на устойчивый SSE decoder (поддержка `event:`, `retry:`, multi-line `data:`, `:` comment/keepalive строк).
- [ ] Добавить обработку HTTP `Retry-After` при `429`/`503`.
- [ ] Добавить валидацию монотонности `seq` на уровне driver (защита от некорректного сервера).
- [ ] Обновить/добавить unit-тесты драйвера под новую error-классификацию и frame-limit.

### Фаза 3 — A2A SSE транспорт
- [ ] Добавить `id: <seq>` в каждый исходящий SSE-фрейм.
- [ ] Добавить `event: task.status` / `event: task.artifact` типизацию фреймов.
- [ ] Реализовать heartbeat/comment (`: keepalive`) каждые 15–30 секунд.
- [ ] Гарантировать закрытие потока только после `final: true` (terminal-событие).
- [ ] Поддержать `Last-Event-ID` на входе HTTP-эндпоинта, преобразуя его в `after_seq`.
- [ ] Добавить корректные HTTP-статусы для: неавторизован, task не найдена, task уже завершена.
- [ ] Интеграционный тест: клиент обрывает соединение на середине потока и переподключается с `Last-Event-ID`.

### Фаза 4 — ACP: выбор модели
- [ ] Принять решение: Pull (MVP) или Push (полноценный streaming) — зафиксировать в ADR/доке.
- [ ] Если **Pull**: выставить `capabilities.streaming = false` в `AcpRuntimeConfig::default()`. [file:52]
- [ ] Если **Pull**: `session/update` должен принимать `afterSeq` и использовать `ReliableTaskStream` для скрытия `Lagged` от клиента.
- [ ] Если **Push**: спроектировать `SessionStreamManager` (`DashMap<TaskId, CancellationToken>` + единый `mpsc::Sender<JsonRpcNotification>`).
- [ ] Если **Push**: реализовать единственный stdout writer task, мультиплексирующий notifications и responses.
- [ ] Если **Push**: обновить `AcpRuntime::run`/`run_with_shutdown` для интеграции с фоновыми subscription-задачами и корректным drain при shutdown.
- [ ] Тест: `acp_updates_are_serialized_by_single_writer` — параллельные task-streams не перемешивают JSON-RPC строки.

### Фаза 5 — Семантические исправления
- [ ] Пробросить реальный `session_id` из `TaskSnapshot`/task-index в `map_event` вместо `"unknown"`. [file:53]
- [ ] Перенести `request_id` для `InputRequired` в `CoreEvent`/`InputRequest`, чтобы ID был стабилен между повторными доставками (Lagged/reconnect). [file:53]
- [ ] Реализовать чтение artifact-истории в `A2aMapper::send_task`/`get_task`, чтобы `artifacts` не были всегда пустыми. [file:54]
- [ ] Проверить согласованность `TaskState → A2A state` маппинга (`status_from_snapshot`) с фактическими terminal-событиями стрима.

### Фаза 6 — E2E и нагрузочные тесты
- [ ] `terminal_event_stops_reconnect` — после `completed`/`failed`/`cancelled` новое SSE-соединение не создаётся.
- [ ] `slow_sse_client_does_not_block_core` — медленный сетевой consumer не блокирует публикацию событий в `AdapterCore`.
- [ ] Нагрузочный тест: N параллельных задач × M медленных подписчиков — подтвердить отсутствие потерь terminal/artifact событий.
- [ ] Тест полного reconnect-цикла HTTP/SSE driver: обрыв соединения на разных этапах stream (до первого события / в середине / на terminal).
- [ ] Регрессионный прогон `broadcast_overflow.rs` и `idempotency_race.rs` после всех изменений. [file:9][file:10]

### Фаза 7 — Документация
- [ ] Обновить `docs/driver-mcp-spec.md` разделом про `ReliableTaskStream` и `after_seq` контракт.
- [ ] Обновить `docs/architecture.md` диаграммой из `streaming-architecture.md`.
- [ ] Обновить `docs/protocol-compatibility.md`: зафиксировать финальное состояние `capabilities.streaming` для ACP.
- [ ] Обновить `AGENTS.md`/`CLAUDE.md`, если контракт stream затрагивает поведение агентов-исполнителей.
- [ ] Финальное ревью PR, обновление `TECH_DEBT.md` — закрыть пункты, относящиеся к streaming reliability.

---

## Приоритет выполнения (сжато)

1. Фаза 0 → 1 (без этого любой рефакторинг рискован).
2. Фаза 2 (быстрые точечные фиксы, независимы от Фазы 1).
3. Фаза 3 (зависит от Фазы 1).
4. Фаза 4 (архитектурное решение, блокирует часть Фазы 6).
5. Фаза 5 (можно параллельно с Фазой 3–4).
6. Фаза 6 (после 1–5).
7. Фаза 7 (непрерывно, финализация в конце).
