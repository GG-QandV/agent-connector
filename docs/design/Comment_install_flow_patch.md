## Закрыты шаги 1, 2, 3 и попутно шаг 8 — все требовали правки одних и тех же двух файлов

**`main.rs`** — все три подкоманды (`Install`, `UpgradePostgres`, `BackupPostgres`) теперь реально вызывают написанные модули вместо `todo!()`. Добавил новый флаг `--config` в `InstallArgs`, который прокидывается в `run_install()`.

**`install_flow.rs`** — две правки в одном файле:

* `run_install()` получил параметры `config_template_path`/`repo_root`, вызывает `config_template::load()` **до** `cargo build` — тот же fail-fast принцип, что уже применялся к проверке `--confirm-docker`: не тратим время на компиляцию, если сам шаблон агентов невалиден.

* `uninstall_flow::run()` теперь реально вызывает `managed_docker::remove_all_resources(true)` при `--purge-data` — раньше это было закомментированной заглушкой (пункт 8 из списка). Обработка трёх исходов: успех, `NotOurResource` (защита сработала, ресурс не наш — не удаляем, только предупреждаем), и любая другая Docker-ошибка (например, daemon не запущен — не фейлим весь uninstall, файловая часть уже отработала).

## Что осталось после этой группировки

Из первоначальных 10 пунктов закрыто **4** (1, 2, 3, 8) одним связанным изменением. Осталось **6**:

* macOS `register_service()`/`unregister_service()`/`start_service()` — три `todo!()`, launchd plist не написан

* macOS `ensure_service_user()` — требует отдельного решения (dscl или явный `--user`)

* Linux `register_service()` — рендеринг unit-шаблона всё ещё не реализован

* `Cargo.toml` для `crates/adapterctl` — не создан

* Graceful Ctrl+C во время `docker pull`/`pg_dump`

* Интеграционный тест полного цикла на реальном/testcontainers Docker
