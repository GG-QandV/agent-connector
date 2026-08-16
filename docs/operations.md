# Operations

Эксплуатация `adapterd` в локальном и remote-профиле.

## Запуск SQLite profile

```bash
cp config/adapter.example.yaml adapter.yaml   # storage.type: sqlite
./scripts/check.sh
cargo run -p adapterd -- adapter.yaml
```

- SQLite-база создаётся автоматически по `storage.path` (дефолт `./data/adapter.db`),
  родительская директория создаётся при старте.
- WAL + `synchronous=NORMAL` + процесс-локальный write mutex — включены по умолчанию.
- Проверка запуска: в логах `adapterd started`, в `./data/` появился `adapter.db`.

## Запуск PostgreSQL profile

```bash
# 1) поднять Postgres (опционально)
docker compose -f deploy/docker-compose.postgres.yaml up -d

# 2) конфиг
storage:
  type: postgres
  dsn_env: ADAPTER_PG_DSN
  schema: agent_adapter

# 3) задать DSN в окружении и запустить
export ADAPTER_PG_DSN='postgres://adapter:pass@127.0.0.1:5432/adapter'
cargo run -p adapterd -- adapter.yaml
```

- adapterd **не управляет** Postgres: только подключается. Схема
  `agent_adapter` создаётся при старте (если её нет), таблицы — `IF NOT EXISTS`.
- Каждый инстанс использует выделенную схему, `public` по умолчанию запрещён.

## Переменные окружения

| Переменная | Назначение |
|---|---|
| `ADAPTER_PG_DSN` | DSN для Postgres (`storage.type: postgres`, `dsn_env`) |
| `<token_env>` | токен Bearer для `driver: http-sse` (поле `token_env` у агента) |
| `RUST_LOG` | уровень логов (tracing), напр. `RUST_LOG=info` |

Секреты — только через env или локальный `.env` (в `.gitignore`). Никогда
не коммитить.

## Миграции

- SQLite: применяются автоматически при `SqliteTaskStore::open` (idempotent,
  `CREATE TABLE IF NOT EXISTS`).
- Postgres: применяются автоматически в `PostgresTaskStore::connect` внутри
  защищённой `migration_guard` секции. Отдельного CLI миграций пока нет —
  это запланировано.

## Health / readiness

В scaffold wire-серверов ещё нет (см. TODO), поэтому внешних health-эндпоинтов
нет. Локально состояние проверяется:

- процесс жив: `systemctl status adapterd` (если systemd) или `pgrep adapterd`;
- retention-cleanup работает: в логах строки `retention cleanup complete`.

Добавление HTTP health/readiness — в скоупе A2A server.

## Логи

- tracing stdout; настройка через `RUST_LOG`.
- Не логировать secrets, промпты и содержимое артефактов.

## Backup / restore

- **SQLite**: остановить daemon, скопировать `adapter.db*` (`-wal`, `-shm`),
  восстановить тем же набором файлов.
- **Postgres**: штатный `pg_dump`/`pg_restore` для схемы `agent_adapter`.

## Graceful shutdown

`adapterd` ждёт SIGINT (Ctrl+C) / SIGTERM, останавливает retention-cleanup
и завершает daemon. Планируется `TaskSupervisor`: прекращение приёма задач →
readiness false → ожидание/отмена активных задач в `shutdown_grace_seconds` →
закрытие protocol streams. Поле `runtime.shutdown_grace_seconds` заложено в
конфиг.

## Bearer token parsing edge cases (resolve_caller)

Открытые code-review todo для `resolve_caller()` в
`crates/protocol-a2a-server/src/executor.rs`. Архитектура подтверждена
(`ServiceParams` → `ExecutorContext.service_params`, см.
`docs/design/auth-architecture.md`), остались осознанные решения по edge cases:

1. **Множественные `Authorization` headers.** `extract_service_params`
   сохраняет `Vec<String>` "в порядке вставки" — если клиент присылает два
   `Authorization` header, `resolve_caller()` сейчас молча берёт `.first()`.
   Решение должно быть осознанным: брать первый / отклонять как invalid /
   брать последний.
2. **Точный префикс `"Bearer "`.** `strip_prefix("Bearer ")` case-sensitive с
   ровно одним пробелом. HTTP spec case-sensitive для scheme name, но решить
   явно, разрешать ли lowercase `"bearer"`.
3. **Пустой токен после `"Bearer "`.** `"Bearer "` (пробел без токена) —
   валидный по формату header; сейчас `strip_prefix` даёт `Some("")` →
   `resolve("")` → `None` → 401, но добавить явную проверку для ясности.

## Incident checklist

1. Сервис не стартует → проверь `RUST_LOG=debug`, валидность YAML,
   наличие `storage.path` директории.
2. `resource exhausted` → `runtime.max_concurrent_tasks` / per-agent limits.
3. Postgres `Unavailable` → DSN, сеть, схема, права.
4. SQLite `Corrupt` → битые данные/время; проверить `*.db-wal` и файловую
   систему; восстановить из backup.
5. Утечка диска → retention TTL (по умолчанию 7 дней) и cleanup batch.
