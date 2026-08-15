# Задание локальному агенту: тесты для agent-connector

## Цель

Построить воспроизводимый тестовый контур после scaffold-коммита. Тесты должны проверять контракты и поведение, а не повторять реализацию. Не добавлять реальный вызов внешней сети, Docker или production agent в unit/contract suite.

## Границы задачи

- Работать в `tests/contract`, `tests/integration`, `tests/fixtures` и в `#[cfg(test)]` модулях соответствующих crates.
- Не менять публичные API без отдельной необходимости; если тест выявил дефект, оформить отдельный commit с исправлением.
- Не тестировать A2A/ACP wire API до получения подтверждённых pinned SDK types и server implementation.
- Все тесты должны быть deterministic: без `sleep`, случайных портов без bind, внешних credentials, сети и реального Docker.

## Стек

Добавить workspace dev-dependencies только при необходимости:

```toml
[workspace.dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time", "sync"] }
serde_json = "1"
tempfile = "3"
assert_matches = "1"
```

Для mock driver использовать собственный `TestDriver` в `tests/support` или crate-local test module. Не использовать HTTP mock server для current scope.

## Структура

```text
tests/
├── contract/
│   ├── task_store_contract.rs
│   ├── idempotency_contract.rs
│   └── driver_contract.rs
├── integration/
│   ├── sqlite_task_lifecycle.rs
│   ├── adapter_core_lifecycle.rs
│   └── retention_cleanup.rs
├── fixtures/
│   ├── adapter.local.yaml
│   ├── adapter.invalid-duplicate-agent.yaml
│   ├── adapter.invalid-http.yaml
│   └── task-events.json
└── support/
    ├── mod.rs
    ├── test_driver.rs
    └── test_clock.rs
```

`tests/support` не является test suite и не должен содержать самостоятельных `#[test]`.

## Contract tests: TaskStore

Один общий набор contract tests должен выполняться для `MemoryTaskStore`, SQLite и, при наличии test DSN, Postgres. Оформить как shared async test functions, вызываемые каждой реализацией.

Обязательные сценарии:

1. Создание task сохраняет заданные ID, agent ID, input и исходный status.
2. Получение отсутствующей task возвращает typed `NotFound`, а не `Option`/строку ошибки.
3. Допустимый transition меняет status и записывает событие.
4. Недопустимый transition отклоняется; состояние и версия task не меняются.
5. Optimistic concurrency: два update с одной expected version — успешен ровно один.
6. События возвращаются в порядке sequence и корректно фильтруются after-sequence cursor.
7. Idempotency key с тем же scope и payload возвращает исходный task; с отличающимся payload — typed conflict.
8. Cancellation terminal task не меняет terminal status.
9. Cleanup удаляет только объекты старше TTL и возвращает корректный report.
10. Многоарендный scope/owner (если он есть в модели) не допускает cross-scope read.

## Unit tests: model и core

### adapter-model

- Round-trip JSON serialization для `Task`, `TaskEvent`, `Artifact`, `TaskStatus`.
- Terminal statuses не имеют исходящих transitions.
- Проверить все разрешённые и запрещённые transitions таблицей test cases.
- Валидация IDs, limit values и timeout: zero/empty/oversized cases.

### adapter-core

С `MemoryTaskStore` и `TestDriver` проверить:

1. `submit` создаёт task и запускает driver один раз.
2. Duplicate idempotency request не запускает driver второй раз.
3. Driver progress event сохраняется и отдаётся subscriber/read API в правильном порядке.
4. Success переводит task в completed и сохраняет artifacts/result.
5. Driver error переводит task в failed с безопасным public error; секрет/error internals не утекать в output.
6. Timeout отменяет driver и переводит task в terminal timeout/failed status согласно модели.
7. Client cancellation проксируется в driver и является идемпотентной.
8. Global и per-agent concurrency limits: лишняя работа queue/reject согласно контракту, активных driver calls не больше лимита.
9. Oversized input/event отклоняется до вызова driver.
10. Unknown agent и unavailable agent дают typed domain error.

## Integration tests: SQLite

Использовать `tempfile::TempDir`; каждый test получает собственный DB file. Никаких общих `./data` и зависимости от порядка тестов.

Проверить:

- миграция из пустого SQLite файла;
- создание/чтение/transition/events после re-open DB;
- WAL-compatible concurrent read во время append event;
- idempotency survives re-open;
- retention cleanup;
- config profile `adapter.local.yaml` создаёт SQLite parent directory и проходит validation.

## Config fixture tests

- `adapter.local.yaml` — валидный SQLite + один stdio test agent.
- `adapter.invalid-duplicate-agent.yaml` — duplicate ID; ожидается validation error.
- `adapter.invalid-http.yaml` — `http://` endpoint при выключенном development mode; ожидается validation error.
- Отсутствующий Postgres `dsn_env` — typed startup error до сетевого подключения.
- Не логировать bearer token, DSN password или env secret в error/debug output.

## Postgres test policy

Postgres integration tests пометить `#[ignore]` и запускать только при заданной переменной `TEST_POSTGRES_DSN`:

```bash
TEST_POSTGRES_DSN=postgres://... cargo test -p postgres-task-store-adapter -- --ignored
```

Каждый запуск обязан создавать уникальную test schema/database namespace и удалять её cleanup guard-ом. Никогда не использовать production DSN.

## Команды и acceptance criteria

Добавить `scripts/check.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Добавить `scripts/run-local.sh`, который копирует/читает local fixture, запускает `adapterd` с SQLite profile и явно сообщает требуемые env variables для remote profile.

Готово, когда выполняется:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/check.sh
```

В PR приложить краткую таблицу: test file → проверяемый контракт → storage/core implementation. Отдельно указать, что A2A/ACP wire compatibility и Postgres suite ещё не входят в обязательный локальный CI, если pinned SDK server и `TEST_POSTGRES_DSN` не доступны.
