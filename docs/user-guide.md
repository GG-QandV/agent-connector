# User Guide — agent-connector

Руководство для оператора/пользователя adapterd: установка, конфигурация,
управление службой и ежедневная эксплуатация.

## 1. Что это

`agent-connector` — слой, который приводит разных агентов к единому жизненному
циклу задач: локальные (stdio), удалённые (HTTP/SSE) и MCP-серверы. Снаружи
daemon `adapterd` говорит на A2A (HTTP/JSON-RPC) и ACP (stdio), внутри единый
`adapter-core` хранит задачи, идемпотентность и события.

Архитектура в двух словах:

```
клиент (A2A/ACP)
   │
   ▼
adapterd ── adapter-core ── driver-stdio / driver-http-sse / driver-mcp ── агент
   │
   └── TaskStore (memory / sqlite / postgres)
```

## 2. Установка

### Вариант A: через adapterctl (рекомендуется)

```bash
# из корня репозитория (там же выполняется cargo build --release)
sudo adapterctl install --storage sqlite --start
```

`adapterctl install` сделает:
1. соберёт `adapterd` в release;
2. создаст служебного пользователя (`_adapterd` / `adapterd`);
3. скопирует бинарь и запишет `adapter.yaml` + `.env` в install-prefix
   (по умолчанию `/opt/agent-connector`; сменить — `--prefix`);
4. зарегистрирует службу: systemd unit (Linux), LaunchDaemon (macOS),
   Windows service (sc.exe);
5. при `--start` — сразу запустит.

Storage-профили (`--storage`):
| Значение | Что происходит |
|---|---|
| `sqlite` | файл БД в `data/` — ничего больше |
| `existing-postgres` | вы указываете DSN (`--postgres-dsn`), installer проверяет `SELECT 1` |
| `managed-docker-postgres` | installer сам поднимает изолированный Postgres-контейнер (требует `--confirm-docker`) |
| `external-managed` | RDS/Neon/Supabase и т.п. (как existing, другое название) |

Пример установки с managed Docker Postgres:

```bash
sudo adapterctl install --storage managed-docker-postgres --confirm-docker --start
```

Пример с собственным Postgres:

```bash
sudo adapterctl install --storage existing-postgres \
  --postgres-dsn "postgres://user:pass@127.0.0.1:5432/agent" \
  --config ./adapter.yaml --start
```

### Вариант B: вручную (dev / без root)

```bash
cp config/adapter.example.yaml adapter.yaml
cargo build --release -p adapterd
./target/release/adapterd adapter.yaml
```

Секреты (Postgres DSN, bearer-токены) — в `.env` рядом с конфигом:
`adapterd` сам читает `.env` из той же директории при ручном запуске, а
при службе это делает systemd (`EnvironmentFile=`) / launchd (через
`ADAPTERD_ENV_FILE`).

## 3. Управление службой

| Команда | Действие |
|---|---|
| `adapterctl start` | запустить службу |
| `adapterctl stop` | остановить (регистрация службы сохраняется) |
| `adapterctl restart` | перезапустить |
| `adapterctl uninstall` | удалить службу (конфиг/`.env` тоже удаляются) |
| `adapterctl uninstall --purge-data` | дополнительно удалить `data/` и managed Docker volume |
| `adapterctl backup-postgres backup.sql` | pg_dump managed-Docker Postgres |
| `adapterctl upgrade-postgres postgres:17.0-alpine` | сменить образ Postgres (обязательный backup перед) |

Требуются root/sudo (или административные права на Windows).

## 4. Конфигурация

Базовый `adapter.yaml`:

```yaml
mode: local              # local | remote

storage:
  type: sqlite           # memory | sqlite | postgres
  path: ./data/adapter.db

runtime:
  max_concurrent_tasks: 32
  cleanup_interval_seconds: 3600

auth:
  bearer_tokens:
    - token_env: ADAPTER_BEARER_TOKEN   # имя env-переменной, не сам токен!
      caller_id: primary-client
      allowed_scopes: []

retention:
  task_ttl_days: 7
  event_ttl_days: 7
  idempotency_ttl_hours: 24
  cleanup_batch_size: 1000

agents:
  - id: stdio-agent
    skills: [code-review]
    driver: stdio
    command: ./agent
```

### Агенты

**stdio** — локальный subprocess, общается по UAIC/1 (по одной JSON-строке
на stdin/stdout):

```yaml
- id: local-agent
  driver: stdio
  command: /path/to/agent
  args: ["--flag"]
  working_dir: /path/to/work
  env:
    SOME_VAR: value
```

**http-sse** — удалённый агент по HTTP+SSE:

```yaml
- id: remote-agent
  driver: http-sse
  endpoint: https://agent.example.com
  token_env: AGENT_TOKEN      # optional
  allow_http_development: false   # true только для http:// в dev
```

**mcp** — подключение к MCP-серверу (stdio или HTTP):

```yaml
- id: mcp-search
  driver: mcp
  mcp_transport: stdio        # stdio | http
  command: ./mcp-servers/search-server
  allowed_tools: [web_search, fetch_page]   # allowlist: какие tools доступны
  discovery_timeout_seconds: 10
```

Важно про MCP:
- `allowed_tools` — **обязателен в remote mode**. Пустой список разрешает
  все tools сервера (допустимо только в local/dev).
- Входные аргументы валидируются против `inputSchema` сервера до вызова.
- Поддерживается версия MCP-протокола: 2024-11-05, 2025-03-26, 2025-06-18.

### Аутентификация

Пустая секция `auth:` — auth выключена (`AllowAllPolicy`). Для включения
укажите имена env-переменных с bearer-токенами. Значение токена — только в
`.env`, никогда в `adapter.yaml`:

```yaml
auth:
  bearer_tokens:
    - token_env: ADAPTER_BEARER_TOKEN
      caller_id: primary-client
```

```bash
# .env
ADAPTER_BEARER_TOKEN=secret-token-value
```

## 5. Запуск daemon

Локально (dev):

```bash
export RUST_LOG=info
cargo run -p adapterd -- adapter.yaml
```

Как служба — `sudo systemctl start adapterd` / `launchctl kickstart -k
system/com.agent-connector.adapterd` / `sc start adapterd`.

Здоровье и готовность (A2A HTTP):
- `GET /healthz` — процесс жив
- `GET /readyz` — storage и registry готовы

## 6. Работа с задачами (A2A)

adapterd слушает HTTP (по умолчанию `0.0.0.0:8348`, сменить — env
`ADAPTERD_LISTEN`). JSON-RPC методы: `tasks/send`, `tasks/get`,
`tasks/cancel`, `session/update`, `message/send` — по A2A-спецификации.

Идемпотентность: один и тот же `idempotencyKey` не создаёт вторую задачу —
возвращается существующая.

## 7. Postgres lifecycle (managed Docker)

- Пароль генерируется при установке и живёт только в `.env`.
- Контейнер/volume помечены ownership-лейблом — installer не тронет чужой
  ресурс с таким же именем.
- `upgrade-postgres` всегда делает backup ДО смены образа.
- Volume **не** удаляется при `uninstall` без `--purge-data`.

## 8. Troubleshooting

| Симптом | Что проверить |
|---|---|
| служба не стартует | `journalctl -u adapterd -e` (Linux), `sudo launchctl print system/com.agent-connector.adapterd` (macOS), Event Viewer (Windows) |
| "environment variable is missing: X" | переменная не задана в `.env`/окружении |
| "MCP endpoint must use https" | remote-mode требует https; для dev включите `allow_http_development` |
| "unknown or disallowed MCP tool" | tool нет в `allowed_tools` или не объявлен сервером |
| Postgres validation failed | DSN неверен; `SELECT 1` не проходит — проверьте доступность/пароль |
| секреты не подхватились | `.env` рядом с конфигом; при службе — путь через `ADAPTERD_ENV_FILE` (macOS) |
| no TTY available | non-interactive среда: передайте `--storage` явно |

## 9. Известные ограничения

- MCP hot-update skills при `tools/list_changed` — реализовано (ADR-0001 R1,
  commit `625545b`), работает без restart агента.
- MCP multi-turn (`input_required`) — не поддерживается (`provide_input`
  возвращает ошибку).
- MCP `prompts`/`resources` не маппятся в skills/context.
- macOS-слой не имеет sandbox-exec изоляции (только Unix-права).
