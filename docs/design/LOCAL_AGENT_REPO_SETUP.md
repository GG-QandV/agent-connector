# Инструкция локальному агенту: структура ACP-A2A adapter

## Цель

Развернуть gateway в существующем репозитории, сохранив `adapter/` самостоятельным Rust workspace. Пока **не выделять** его в отдельный Git-репозиторий: граница уже выражена папкой и Cargo workspace, а выделение имеет смысл после появления независимых релизов, CI и ownership.

## Исходная папка

```text
/home/gg/projects/AGENTS/ACP-A2A_gateway/adapter/
```

Работать только внутри этой директории. Не изменять родительский репозиторий без отдельной задачи.

## Требуемая структура

```text
adapter/
├── Cargo.toml                    # workspace root; только members/dependency versions
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── docs/
│   ├── architecture.md
│   ├── operations.md
│   └── protocol-compatibility.md
├── config/
│   └── adapter.example.yaml
├── crates/
│   ├── adapter-model/
│   ├── adapter-store-contract/
│   ├── adapter-core/
│   ├── protocol-a2a-mapper/
│   ├── protocol-acp-mapper/
│   ├── driver-stdio/
│   ├── driver-http-sse/
│   ├── memory-task-store/
│   ├── sqlite-task-store-adapter/
│   ├── postgres-task-store-adapter/
│   └── adapterd/
│       ├── src/
│       │   ├── main.rs
│       │   └── config.rs
│       └── migrations/
├── tests/
│   ├── contract/
│   ├── integration/
│   └── fixtures/
├── scripts/
│   ├── check.sh
│   └── run-local.sh
└── deploy/
    ├── docker-compose.postgres.yaml
    └── systemd/adapterd.service
```

## Правила зависимостей

- `adapter-model` не зависит ни от чего, кроме базовых сериализации/ID библиотек.
- `adapter-store-contract` зависит от `adapter-model`.
- `adapter-core` зависит только от `adapter-model` и `adapter-store-contract`; он не знает SQL, Docker, Axum, A2A SDK или ACP transport.
- Каждый storage adapter зависит от `adapter-store-contract` и своего DB driver.
- Protocol mappers зависят от `adapter-core` и соответствующего protocol SDK, но не от конкретных storage adapters.
- `driver-*` реализуют driver trait из `adapter-core`.
- Только `adapterd` связывает config, concrete storage, drivers, protocol servers и background cleanup.
- Запрещены циклические зависимости и импорт concrete storage/driver в `adapter-core`.

## Инициализация

1. Проверить, что установлен стабильный Rust (`rustup show active-toolchain`).
2. В `adapter/` создать Cargo workspace с `resolver = "2"` и перечислить crates выше в `members`.
3. Зафиксировать общие версии зависимостей через `[workspace.dependencies]`.
4. Добавить локальные path dependencies между crates; не публиковать внутренние crates в crates.io.
5. Создать `.gitignore` как минимум для `/target`, `/data`, `.env`, `*.db`, `*.db-wal`, `*.db-shm`.
6. Положить образец конфигурации в `config/adapter.example.yaml`; секреты хранить только в env variables или локальном `.env`, никогда не коммитить.
7. Запустить `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` и `cargo test --workspace`.

## Git-режим

- Если `ACP-A2A_gateway` уже Git-репозиторий, оставить `adapter/` обычной подпапкой и коммитить изменения в родительский repo.
- Не создавать Git submodule и не выполнять `git init` внутри `adapter/` без решения владельца проекта.
- Когда adapter будет иметь самостоятельные релизы, отдельный CI/CD, внешних пользователей и независимый owner, перенести папку с сохранением истории через `git filter-repo --path adapter/ --path-rename adapter/:` в новый repo.

## Definition of done

Локальный агент должен оставить workspace, в котором `cargo test --workspace` проходит, `adapterd` стартует с `config/adapter.example.yaml`, а SQLite data directory создаётся локально без Docker. Docker Compose предназначен только для опционального Postgres profile.
