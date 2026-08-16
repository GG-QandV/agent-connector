# Инструкция агенту: интеграция `driver-mcp` в `agent-connector`

Код в `driver_mcp_TRULY_FINAL.rs` — API-verified референс, не финальный production файл. Нужно перенести его в workspace, доработать одну ownership-проблему и подключить к существующей конфигурации.

## 1. Создать crate

```bash
cd crates
cargo new --lib driver-mcp
```

`Cargo.toml`:

```toml
[dependencies]
adapter-core = { path = "../adapter-core" }
adapter-model = { path = "../adapter-model" }
rmcp = { version = "0.8", features = ["client"] }
tokio = { workspace = true }
async-trait = { workspace = true }
dashmap = { workspace = true }
futures-util = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
```

Перепроверить точную версию `rmcp` в `Cargo.lock`, если она уже используется где-то в проекте — использовать ту же, не смешивать версии.

## 2. Перенести код

Скопировать содержимое `driver_mcp_TRULY_FINAL.rs` в `crates/driver-mcp/src/lib.rs` как есть.

## 3. Закрыть единственный оставшийся пробел — cancel() ownership

Сейчас `cancel()` не может вызвать `handle.cancel()`, потому что `RequestHandle` перемещён в `tokio::spawn`. Нужно применить вариант (б), уже описанный в комментарии внутри файла:

1. В `invoke()`, вместо прямого `tokio::time::timeout(timeout, handle.await_response())`, создать `tokio_util::sync::CancellationToken`, сохранить его в `active_handles` вместо голого `RequestId`:
   ```rust
   active_handles: Arc<DashMap<TaskId, CancellationToken>>
   ```
2. Внутри spawn'нутой задачи использовать `tokio::select!`:
   ```rust
   tokio::select! {
       result = handle.await_response() => { /* существующая обработка */ }
       _ = cancel_token.cancelled() => {
           let _ = handle.cancel(Some("cancelled by adapter-core".to_string())).await;
           let _ = tx.send(DriverEvent::Cancelled).await;
       }
   }
   ```
3. В `cancel()` снаружи — просто `cancel_token.cancel()` по найденному в `active_handles` токену, без прямого доступа к `handle`.

Это устраняет moved-value проблему: `handle` целиком остаётся внутри spawn'нутой задачи, снаружи передаётся только сигнал отмены.

## 4. Подключить в `adapterd-config`

В `crates/adapterd/src/config.rs` добавить вариант `AgentTransportConfig::Mcp` — точная структура описана в `driver-mcp-spec.md` раздел 4. Обязательно сохранить `allowed_tools` как **обязательное** поле вне local/dev profile — не делать его optional без явной проверки profile в `Config::validate()`.

## 5. Подключить в `adapterd/src/main.rs`

В `build_driver()` добавить ветку:

```rust
AgentTransportConfig::Mcp { transport, allowed_tools, discovery_timeout_seconds } => {
    let driver = match transport {
        McpTransportConfig::Stdio { command, args, env } => {
            McpDriver::connect_stdio(
                agent.id.clone(),
                McpStdioConfig { command, args, env },
                allowed_tools,
                Duration::from_secs(agent.limits.default_timeout_seconds),
            ).await.map_err(|e| StartupError::Driver(e.to_string()))?
        }
        McpTransportConfig::Http { .. } => {
            return Err(StartupError::Driver("MCP HTTP transport not yet implemented".into()));
        }
    };
    Ok(Arc::new(driver))
}
```

HTTP-вариант MCP-транспорта не реализован в этом коде — оставить явную ошибку, не молчаливый unimplemented.

## 6. Тесты

Минимум: integration test с реальным минимальным MCP stdio-сервером из `examples/servers/` репозитория `rust-sdk` (например `counter.rs` — самый простой). Полный цикл: `connect_stdio` → `discover_tools` находит tool → `invoke()` → получить `Completed` с ожидаемым content → `cancel()` во время выполнения долгого tool call (если есть подходящий тест-сервер с задержкой, например `progress_demo.rs`) → убедиться, что `handle.cancel()` реально отправляется и spawn'нутая задача завершается, не оставляя висящих tokio tasks.

## 7. Definition of done

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Плюс: живой integration test проходит против реального MCP stdio-сервера (не только mock), `cancel()` не оставляет orphaned tasks (проверить через `tokio::task::JoinHandle` presence-check в тесте, как это уже сделано в других connector-тестах проекта).
