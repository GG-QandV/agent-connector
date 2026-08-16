# Инструкция агенту: заменить macOS-заглушку на рабочий launchd

## Контекст

Приоритет платформ пересмотрен: Windows 65-70%, macOS 25-30%, Linux — остаток.
`src/platform/macos.rs` сейчас — заглушка `Err(Unsupported)`. Это критично,
потому что покрывает почти треть пользователей без рабочей установки.

## Шаг 1 — заменить src/platform/macos.rs целиком

Взять содержимое из черновика `adapterctl_macos_launchd.rs` (последний в чате).
Полностью заменяет старую заглушку. Ключевые отличия от заглушки:

- `ensure_service_user()` — реальный `dscl` (создаёт `_adapterd` в UID
  диапазоне 200-400, hidden system account)
- `register_service()` — генерирует `.plist` в `/Library/LaunchDaemons/`,
  `launchctl bootstrap system <plist>` (современный синтаксис, не load/unload)
- `unregister_service()` — `launchctl bootout` + удаление plist
- `start_service()` — `launchctl kickstart -k`
- `restrict_file_permissions()` — `chmod 0600` + `chown user:daemon`

Проверить, что `platform/mod.rs` уже даёт доступ к `PlatformError`,
`PlatformServiceManager`, `ServiceContext` — этот файл их только `use super::{...}`,
не переопределяет.

## Шаг 2 — обязательная связанная правка в adapterd (НЕ adapterctl)

launchd, в отличие от systemd `EnvironmentFile=-...`, не читает `.env` файл
сам. Plist передаёт путь через `EnvironmentVariables` → `ADAPTERD_ENV_FILE`.
Без этой правки DSN/секреты не попадут в процесс на macOS.

В `crates/adapterd/src/main.rs`, в самом начале `fn main()` (до `Config::load()`):

```rust
// Если ADAPTERD_ENV_FILE задан (macOS launchd путь — systemd делает это
// сам через EnvironmentFile=, launchd не умеет) — прочитать .env файл и
// проставить переменные в process env ДО чтения конфига.
if let Ok(env_file_path) = std::env::var("ADAPTERD_ENV_FILE") {
    if let Ok(contents) = std::fs::read_to_string(&env_file_path) {
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') { continue; }
            if let Some((key, value)) = line.split_once('=') {
                std::env::set_var(key.trim(), value.trim());
            }
        }
    } else {
        tracing::warn!(path = %env_file_path, "ADAPTERD_ENV_FILE set but file unreadable");
    }
}
```

Не тащить внешний `dotenv`/`dotenvy` crate ради 10 строк — эта логика проще
и достаточна для формата `.env`, который сам install_flow.rs генерирует
(простой `KEY=value`, без экспорта bash-синтаксиса).

## Шаг 3 — проверка

```bash
cargo build -p adapterd -p adapterctl
cargo test -p adapterctl   # включает 3 новых теста из macos.rs
```

На реальной macOS-машине (руками, не в CI без sudo):
```bash
sudo adapterctl install --storage sqlite --start
sudo launchctl print system/com.agent-connector.adapterd   # проверить статус
tail -f /var/log/agent-connector/adapterd.log
sudo adapterctl uninstall
```

## Что осталось несделанным даже после этой правки

- `sandbox-exec` профиль для файловой изоляции (аналог `ProtectSystem=full`) —
  сознательно не реализован, launchd не имеет прямого эквивалента, полагаемся
  на Unix-права. Не блокер для MVP.
- Интеграционный тест реального launchd-цикла (install → start → uninstall)
  под macOS в CI — GitHub Actions macOS runners поддерживают `sudo`, но тест
  не написан, только unit-тесты рендеринга plist.
