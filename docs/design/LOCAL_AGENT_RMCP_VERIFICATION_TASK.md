# Задание локальному агенту: закрыть 3 последних пробела в `driver-mcp`

## Контекст

`driver-mcp` — новый driver-модуль для `agent-connector`, реализующий `AgentDriver` trait поверх MCP client SDK `rmcp` (`github.com/modelcontextprotocol/rust-sdk`, commit `f713ebd1a6feab492fb730a8bc13026be114d82f`). Через веб-поиск и `search_code` по GitHub API удалось подтвердить 9 из 12 ключевых частей API (см. `driver_mcp_FINAL_STATUS.md` и `driver_mcp_v2_progress_confirmed.rs`). Остались три узких, точно локализованных пробела — их нельзя закрыть текстовым grep-поиском по API, нужен реальный `cargo doc` или чтение полного файла целиком.

## Что уже подтверждено (не трогать, не перепроверять)

- `call_tool(&self, params: CallToolRequestParams) -> Result<CallToolResult, ServiceError>`
- `list_tools(&self, params: Option<PaginatedRequestParams>) -> Result<ListToolsResult, ServiceError>`
- `notify_cancelled` существует как `peer_not` метод, принимает `CancelledNotificationParam::new(Option<RequestId>, Option<String>)`
- `ContentBlock::text()`, `ContentBlock::Text(TextContent)`, `ContentBlock::Image(ImageContent)` варианты
- `().serve(transport)` работает потому что `()` — unit type с blanket no-op `ClientHandler`; чтобы получать notifications, нужен свой тип, и `.serve()` вызывается **на этом типе**: `my_handler.serve(transport).await?`
- `ClientHandler::on_progress(&self, params: ProgressNotificationParam, context: NotificationContext<RoleClient>)`
- `ProgressNotificationParam { pub progress_token: ProgressToken, pub progress: f64, ... }`
- Встроенный SDK helper `rmcp::handler::client::progress::ProgressDispatcher` — обёртка над `HashMap<ProgressToken, mpsc::Sender<ProgressNotificationParam>>`, с методом `handle_notification(params).await`
- `RequestMetaObject::with_progress_token(token)` кладётся в `PeerRequestOptions.meta`

## Что нужно найти — 3 задачи

### Задача 1: как подписаться на конкретный `ProgressToken` через `ProgressDispatcher`

**Файл:** `crates/rmcp/src/handler/client/progress.rs` (в клонированном репозитории `modelcontextprotocol/rust-sdk`, тег/commit `f713ebd1a6feab492fb730a8bc13026be114d82f` или актуальный релизный тег).

**Что искать:** методы `impl ProgressDispatcher`, кроме уже найденного `new()` и `handle_notification()`. Нужен метод типа `register`/`subscribe`/`watch`, который принимает `ProgressToken` и возвращает `mpsc::Receiver<ProgressNotificationParam>` (или похожий), чтобы наш driver мог подписаться на прогресс конкретного invoke-вызова ДО отправки `call_tool()`.

**Как искать:** открыть файл в IDE/`cat`, прочитать весь `impl ProgressDispatcher` блок целиком — не grep, потому что имя метода неизвестно заранее.

**Ожидаемый результат:** точная сигнатура метода, например:
```rust
impl ProgressDispatcher {
    pub fn subscribe(&self, token: ProgressToken) -> mpsc::Receiver<ProgressNotificationParam> { ... }
    // или
    pub async fn register(&self, token: ProgressToken) -> mpsc::Receiver<ProgressNotificationParam> { ... }
}
```

### Задача 2: как передать `PeerRequestOptions`/progress token в `call_tool()`

**Файлы:**
- `crates/rmcp/src/service/client.rs` — искать другие `pub async fn call_tool*` варианты рядом с уже найденным базовым `call_tool()`.
- `crates/rmcp/tests/test_request_timeout_progress.rs` — здесь есть тестовый хелпер `call_tool_with_options()`, посмотреть **его полное тело**, что конкретно он вызывает внутри (какой метод SDK, не тестовый).

**Что искать:** метод на `RunningService`/`Peer`, сигнатура которого принимает и `CallToolRequestParams`, и `PeerRequestOptions` вместе. Возможные кандидаты по конвенции именования в этом SDK (судя по macro `method!(peer_req ...)` паттерну, увиденному для других методов): `call_tool_with_options`, `send_request_with_options`, или у самого `call_tool()` может быть второй optional параметр, который просто не был виден в кратком фрагменте сигнатуры.

**Как искать:** прочитать полностью функцию `async fn call_tool_with_options(...)` в `test_request_timeout_progress.rs` — она **вызывает** реальный SDK API внутри своего тела, просто была написана как обёртка для теста. Внутри неё будет прямой вызов настоящего метода.

**Ожидаемый результат:** точный код вида:
```rust
async fn call_tool_with_options(
    client: &RunningService<RoleClient, ProgressCountingClient>,
    name: &str,
    options: PeerRequestOptions,
) -> Result<CallToolResult, ServiceError> {
    // <-- вот эта строка нужна целиком
    client.call_tool_with_XXX(CallToolRequestParams::new(name), options).await
}
```

### Задача 3: как клиент получает `RequestId` своего исходящего запроса для последующего `cancel`

**Файлы:**
- `crates/rmcp/src/service.rs` — искать `RequestId`, генерацию id для outgoing request.
- `crates/rmcp/src/service/client.rs` — искать, возвращает ли `call_tool()` или его low-level аналог что-то, содержащее `RequestId`, помимо самого `CallToolResult`.

**Что искать:** OДИН из двух вероятных паттернов:
- (а) есть low-level метод типа `send_request(...) -> (RequestId, impl Future<Output = Result<...>>)`, который даёт id **до** получения ответа — тогда `call_tool()` высокого уровня просто не даёт доступа к этому id, и нужно использовать low-level API вместо высокоуровневого `call_tool()`, если нужен cancel.
- (б) SDK ожидает, что cancellation работает иначе — не через явный `notify_cancelled` с request id от клиента, а через какой-то `CancellationToken`/`Drop` механизм на уровне самого `Future`, возвращаемого `call_tool()` (то есть просто `drop()` futures или явный `tokio::select!` с внешним cancellation token, без отправки MCP-level notification вообще).

**Как искать:** прочитать `crates/rmcp/src/service.rs` целиком в поиске слова `RequestId` и понять, публичен ли какой-либо способ его получить с клиентской стороны для конкретного отправленного запроса.

**Ожидаемый результат:** либо конкретная сигнатура low-level API, либо явный вывод "cancellation для клиента реализуется через X, не через notify_cancelled с явным request id".

## Как оформить результат

Создать файл `docs/design/rmcp-api-verification.md` в репозитории `agent-connector` с тремя разделами (по одному на задачу), каждый с:
- точной сигнатурой найденного метода/паттерна;
- путём к файлу и номером строки, где он определён;
- одним примером использования (skopировать из существующих examples/tests в `rust-sdk`, если есть).

После заполнения этого файла — обновить `driver_mcp_v2_progress_confirmed.rs` (или финальную версию `driver-mcp/src/lib.rs`), заменив оставшиеся `TODO-VERIFY`/предположения на подтверждённый код, и заменить самодельный `McpClientHandler` с `DashMap` на тонкую обёртку над `ProgressDispatcher`, как показано в `driver_mcp_FINAL_STATUS.md`.

## Definition of done

```bash
cargo doc --open -p rmcp   # локально, чтобы верифицировать входные данные перед правкой
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Три пункта выше закрыты в `docs/design/rmcp-api-verification.md`, `driver-mcp` компилируется без единого `TODO-VERIFY`/`unimplemented!()`, minimal integration test против реального MCP stdio test-server (можно взять любой минимальный сервер из `examples/servers/` в `rust-sdk`) проходит для полного цикла: discovery → invoke → progress event (если сервер их шлёт) → completed → cancel (best-effort, если применимо).
