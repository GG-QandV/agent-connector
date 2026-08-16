# Инструкция агенту: куда положить файлы adapterctl и что сначала исправить

## Блокер №0 — исправить ДО расстановки файлов

`crates/adapterd/src/` содержит только `config.rs` + `main.rs` — это **бинарный крейт**, без `lib.rs`. `Config`/`AgentConfig`/`StorageConfig` сейчас живут как `mod config;` внутри бинарника и **не экспортируются** наружу. Мой `Cargo.toml` для `adapterctl` ссылался на `adapterd-config = { path = "../adapterd/src/config" }` — такого пути в Cargo не существует, зависимость нерабочая.

**Нужно сделать одно из двух, прежде чем собирать `adapterctl`:**
- (а) Вынести `config.rs` в новый крейт `crates/adapterd-config/` (переименовать `pub struct Config` и friends туда, `adapterd` и `adapterctl` оба зависят от него), либо
- (б) Добавить `crates/adapterd/src/lib.rs`, ре-экспортирующий `pub mod config;`, и в `adapterctl/Cargo.toml` писать `adapterd = { path = "../adapterd" }`.

Вариант (а) чище (нет циклической путаницы "бинарник как библиотека"). Выбрать один, применить, зафиксировать в `Cargo.toml` `adapterctl` до следующего шага — иначе `config_template.rs` не скомпилируется.

## Расстановка файлов

Все файлы ниже — черновики из чата, не финальные diff'ы. Создать новый crate:

```
mkdir -p crates/adapterctl/src crates/adapterctl/tests
```

| Черновик из чата | Куда положить | Что сделать перед этим |
|---|---|---|
| `adapterctl_cargo_toml.toml` | `crates/adapterctl/Cargo.toml` | Исправить путь на `adapterd-config`/`adapterd` по итогу Блокера №0 |
| `adapterctl_main.rs` | `crates/adapterctl/src/main.rs` | Без изmenений |
| `adapterctl_i18n_stub.rs` | `crates/adapterctl/src/i18n.rs` | Без изменений (English-only решение принято) |
| `adapterctl_windows.rs` | `crates/adapterctl/src/platform/windows.rs` | Создать `platform/mod.rs` с `pub mod windows;`, `pub trait PlatformServiceManager`, `PlatformError`, `ServiceContext` (сейчас эти типы разбросаны по нескольким черновикам — собрать в `platform/mod.rs`) |
| `adapterctl_linux_register_service.rs` | `crates/adapterctl/src/platform/linux.rs` | `platform/mod.rs`: `pub mod linux;` |
| — (macOS) | `crates/adapterctl/src/platform/macos.rs` | **Не написан.** Использовать заглушку из `adapterctl_install.rs` (Command::Install skeleton) как основу — там были `Err(Unsupported)` версии всех 4 методов |
| `adapterctl_managed_docker_windows.rs` | `crates/adapterctl/src/managed_docker.rs` | Заменить старые `connect()`/`ensure_running()` на версии из `adapterctl_preflight_check.rs` (там `preflight_check()` + рефакторинг `connect_internal()`) |
| `adapterctl_preflight_check.rs` | Слить внутрь `crates/adapterctl/src/managed_docker.rs` | Это правка существующего файла, не отдельный файл — добавить `preflight_check()`, `PreflightError`, обновлённый `ensure_running()` |
| `adapterctl_install_flow.rs` (первая версия) + `adapterctl_install_flow_patch.rs` (config_template) + `adapterctl_install_flow_preflight_patch.rs` (preflight) | Слить в один `crates/adapterctl/src/install_flow.rs` | **Три черновика правят один файл** — собрать вручную: взять базу из первого, применить config_template-diff (заменить default_agents), применить preflight-diff (добавить проверку после --confirm-docker, до cargo build) |
| `adapterctl_config_template.rs` | `crates/adapterctl/src/config_template.rs` | Поправить `use adapterd_config::{...}` по итогу Блокера №0 |
| `adapterctl_postgres_lifecycle.rs` | `crates/adapterctl/src/postgres_lifecycle.rs` | Обновить `connect()` на переиспользование `managed_docker::preflight_check()`, не собственную копию |
| `adapterctl_integration_test_fixed.rs` | `crates/adapterctl/tests/managed_docker_integration.rs` | Использовать эту версию, НЕ `adapterctl_integration_test_placement.rs` (та ссылалась на несуществующую функцию, эта версия исправлена) |

## Не переносить в репозиторий как есть

- `adapterctl_install.rs` (самый первый черновик, до Windows/managed_docker) — устарел, заменён последующими версиями. Держать только как референс для macOS-заглушки (см. таблицу выше).
- `adapterctl_i18n.rs` (двухъязычная en/ru версия) — отклонена в пользу `adapterctl_i18n_stub.rs`. Не использовать.
- `install.sh` (bash-версия, самая первая) — заменена полностью на Rust `adapterctl`. Можно удалить или оставить в `scripts/` как deprecated-referenced, не как актуальный installer.

## После расстановки — обязательные шаги

```bash
cargo build -p adapterctl              # если не компилируется — сначала Блокер №0
cargo test -p adapterctl                # юнит-тесты (без Docker)
cargo test -p adapterctl -- --ignored   # integration-тесты (нужен Docker, Linux containers mode)
cargo clippy -p adapterctl --all-targets -- -D warnings
```

## Ещё не написано вообще (не путать с "написано, но не расставлено")

- macOS `register_service()`/`unregister_service()`/`start_service()` — реальный launchd-код
- Graceful Ctrl+C (atomic write `.tmp`+`rename` для backup, `tokio::signal::ctrl_c()` вокруг `docker pull`)
- `.github/workflows/adapterctl-ci.yml`
- Замена `atty`→`is-terminal`, `sc.exe`→`windows-service` crate, `serde_yaml`→преемник (все три — TODO в Cargo.toml, не блокируют MVP)
