# Senior-задание: SSE, ACP stdio runtime, concurrency, lifecycle

## Контекст

Это senior-часть общего задания по wire-серверам `agent-connector`. Junior/middle части (health/readiness, config fixtures, `AgentCardProducer`, non-streaming JSON-RPC methods, contract tests) реализуются отдельно и не блокируют начало этой работы, но эта часть блокирует production-readiness всего adapter.

Подтверждённая база: `github.com/a2aproject/a2a-rs`, pinned commit `02ee56024a485a5f184cbc55d1706918ee1ff809`, crate `a2a-server` (axum-based), traits `RequestHandler`/`AgentExecutor`, error type `A2AError`. Discovery path — `/.well-known/agent-card.json`.

Перед началом обязательно прочитать целиком (не выборочно):

```text
a2a-server/src/jsonrpc.rs
a2a-server/src/handler.rs
a2a/src/error.rs (или где определён A2AError)
examples/tests/transports_e2e.rs   # там уже есть streaming executor пример
```

Не реализовывать streaming/SSE до подтверждения, как именно `a2a-server` ожидает подписку на события (streaming response type, метод в `RequestHandler`, или отдельный `AgentExecutor::execute` с callback/stream).

## Блок 1: SSE task streaming

### Требования к поведению

- SSE endpoint отдаёт события task в строгом порядке `sequence`, без пропусков и дублей при нормальной работе.
- Поддержка resume: клиент передаёт последний полученный курсор (`Last-Event-ID` или SDK-defined механизм); server отдаёт только события после этого курсора, не пересылая уже подтверждённые.
- Disconnect клиента **не** отменяет task. Task продолжает исполняться и его события продолжают писаться в store.
- Cancellation task — отдельная явная операция (`tasks/cancel`), не побочный эффект закрытия stream.
- Slow consumer (клиент читает медленнее, чем генерируются события) не должен блокировать исполнение task и не должен приводить к unbounded memory growth.
- Terminal event (`completed`/`failed`/`canceled`) закрывает stream корректно согласно SDK-контракту, без "зависшего" открытого соединения.
- Авторизация и existence-check task проверяются **до** открытия stream, не после первого чтения из store.

### Технические ограничения реализации

- Канал между "writer" (кто пишет события в store/notify) и "SSE sender" должен быть bounded (`tokio::sync::mpsc` с фиксированной capacity или broadcast с ограничением backlog). Unbounded channel запрещён — это прямой путь к OOM при slow consumer.
- При переполнении канала: либо lagging consumer получает explicit gap/error event и должен сделать resume с курсора из store, либо реализовать backpressure до writer с явным timeout, чтобы не заблокировать navигацию другими tasks. Выбранную стратегию описать в docstring над функцией.
- Нельзя держать открытую SQLite/Postgres transaction или connection checked out из pool на всё время жизни SSE stream. Каждое чтение новых событий — отдельный short-lived query/poll или подписка на in-process notify (`tokio::sync::Notify`/`watch`), не долгоживущая transaction.
- Cancel-safety: если SSE future отменяется (client отключился, `axum` дропнул response future), это не должно оставить task/driver в неконсистентном состоянии и не должно "потерять" writer task (не должно возникать orphaned background task, который никто не await-ит и не отменяет).
- Reconnect/resume должен быть протестирован конкретно на пропущенное окно: сгенерировать события 1..10, отключить consumer после события 5, снова подключиться с курсором 5, получить события 6..10 без дублирования и без потери.

### Обязательные тесты

- Ordered delivery без потерь при нормальном consumer.
- Resume после disconnect с корректным курсором отдаёт ровно недостающие события.
- Slow consumer: искусственно задержанный reader не блокирует завершение task и не роняет процесс по памяти (тест с ограниченным числом событий и явной проверкой bounded channel behavior, не "живой" замер памяти).
- Отмена клиентского subscribe future не отменяет task и не паникует, не оставляет висящих tasks (использовать `tokio::task` tracking или `JoinSet`/`JoinHandle` presence-check в тесте).
- Unauthorized/unknown task не открывает stream (проверка происходит до первого event).

## Блок 2: ACP stdio JSON-RPC runtime

### Контекст протокола

ACP (`agentclientprotocol.com`, protocol v1) — JSON-RPC 2.0 over stdio, newline-delimited messages, без embedded newline внутри одной строки, stdout зарезервирован исключительно под protocol messages. В workspace нет подтверждённого готового Rust SDK под этот протокол на момент задания — перед реализацией подтвердить отсутствие/наличие подходящего crate; если crate не найден, реализовать типизированный слой самостоятельно по официальной JSON schema протокола, не изобретая собственный формат.

### Требования к реализации

- Async read loop построчно с buffered reader (`tokio::io::BufReader` + `AsyncBufReadExt::lines` либо аналог), каждая строка — один JSON-RPC message (request, response или notification).
- Разбор должен различать: valid request (есть `id`), valid notification (нет `id`), malformed JSON, valid JSON но invalid JSON-RPC envelope (нет `method`/неверная `jsonrpc` версия).
- На malformed JSON — вернуть JSON-RPC parse error **как protocol message**, loop не паникует и не завершается, продолжает читать следующую строку.
- На invalid request — вернуть invalid-request error с тем же `id`, если `id` удалось извлечь, иначе `null` согласно JSON-RPC spec.
- На notification — никогда не писать ответ в stdout, независимо от исхода обработки.
- Response и async events должны сохранять порядок относительно своего request/subscription, но не должны блокировать обработку параллельных независимых requests, если протокол это допускает (уточнить по ACP transport doc; если ACP v1 stdio строго sequential — реализовать sequential, не эмулировать параллелизм искусственно).
- Максимальный размер строки ограничен конфигом; превышение — explicit error до передачи в `protocol-acp-mapper`/`AdapterCore`, не попытка распарсить partial buffer.
- Все внутренние логи, panics-hooks и diagnostics — только в stderr через `tracing`; stdout не должен получить ни одного байта, не являющегося валидным ACP JSON-RPC message plus newline.
- Flush stdout после каждой written line — нельзя буферизовать ответы так, чтобы клиент их не увидел вовремя.

### Shutdown

- SIGTERM/Ctrl+C: runtime переходит в draining — прекращает принимать новые top-level requests, ожидает завершения уже начатых операций в пределах `shutdown_grace_seconds`, затем закрывает stdout, не оставив недописанную/оборванную JSON строку.
- EOF на stdin завершает loop без panic и без зависания; pending operations должны либо завершиться, либо быть явно отменены с логированием причины в stderr.

### Обязательные тесты (in-memory harness, без реального subprocess)

- Valid request → ровно одна valid response line с тем же `id`.
- Malformed JSON → parse-error line, затем следующий valid request всё равно успешно обрабатывается тем же runtime instance.
- Invalid envelope (валидный JSON, невалидный JSON-RPC) → invalid-request error.
- Notification → нулевые записи в stdout.
- Oversized line → explicit error до вызова mapper/core, без попытки парсинга.
- EOF → clean shutdown, никаких panics, все ожидающиеся до EOF операции корректно завершены/отменены.
- Смоделированный SIGTERM (через cancellation token, не реальный сигнал) → readiness/draining переключается раньше, чем закрывается stdout, и никакая частично записанная строка не появляется в выводе.

## Блок 3: concurrency limits и timeout cancellation в `adapter-core`

### Требования

- Глобальный лимит `max_concurrent_tasks` и per-agent лимит `AgentLimits::max_concurrent_tasks` должны быть реальными semaphore-based ограничениями, а не "лучшими намерениями" через optimistic counter без атомарности.
- Превышение per-agent `max_queued_tasks` — task должен быть явно отклонён (typed error), а не бесконечно ждать освобождения слота.
- `default_timeout_seconds` — по истечении таймаута driver call должен быть **отменён** (через `tokio::time::timeout` вокруг driver invocation, а не просто "перестать ждать" оставив driver работать в фоне без отслеживания), task переводится в terminal timeout/failed state.
- Явно продумать и задокументировать: что происходит, если driver не поддерживает graceful cancellation (например, stdio child process) — нужен явный kill/drop пути, а не "надежда", что drop trait сам всё почистит корректно.
- Race between: клиент вызывает cancel одновременно с natural completion driver call. Реализация должна детерминированно прийти к одному consistent terminal state, не допускать двойного перехода статуса и не терять result, если completion произошёл на миллисекунды раньше cancel.
- Все проверки лимитов выполняются до вызова driver, не после частичного запуска работы.

### Обязательные тесты

- N параллельных submit при лимите K: активных driver calls одновременно ≤ K, доказано через test driver, считающий текущие concurrent invocations (atomic counter с assert на пике).
- Превышение `max_queued_tasks` даёт typed reject, а не hang теста по таймауту.
- Timeout: driver, который "висит" дольше `default_timeout_seconds` в тесте, гарантированно отменяется, task получает terminal timeout status, тест не использует реальный wall-clock sleep дольше необходимого (использовать управляемый test clock/`tokio::time::pause` + `advance`).
- Race test: одновременный cancel и completion (через управляемые barriers/channels в test driver, не через угадывание тайминга) — результат детерминирован и не паникует под `loom`-style или повторным прогоном (минимум: тест стабильно проходит при запуске с `--test-threads=1` и не флапает при повторных запусках, лучше — явная синхронизация через channel rendezvous, не sleep-based timing guess).

## Блок 4: интеграция с `a2a-server` internals

- Сопоставить SSE/streaming требование этого задания с фактическим streaming API `a2a-server` (нужно подтвердить: JSON-RPC streaming response, отдельный SSE route, или `AgentExecutor` стриминговый callback — это должно быть явно найдено при чтении `handler.rs`/`jsonrpc.rs`/`transports_e2e.rs`, не предположено).
- Если SDK стриминг реализован через `AgentExecutor`, у которого `execute` принимает какой-то emitter/sink, наш `AdapterRequestHandler`/`AgentExecutor` impl должен транслировать этот sink в события из `AdapterCore` без блокировки executor thread и без потери backpressure semantics, описанных в Блоке 1.

## Definition of done (senior-часть)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Плюс в PR:

- Явное описание choice стратегии backpressure для SSE (drop-with-gap-event vs backpressure-with-timeout) и почему.
- Явное подтверждение, из какого файла `a2a-server` взят streaming API, с точным именем trait/метода.
- Явное подтверждение, что ACP реализован через сторонний crate (имя, версия) или самостоятельно (со ссылкой на прочитанную JSON schema).
- Результаты race/concurrency тестов с описанием, как воспроизводилась гонка детерминированно (не через угадывание таймингов).
- Список edge cases, которые оставлены как known limitation (если есть), с обоснованием почему они вне текущего scope.
