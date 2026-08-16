# `rmcp` API verification — закрытие 3 пробелов для `driver-mcp`

Верификация выполнена чтением полного исходного кода `modelcontextprotocol/rust-sdk`
на commit `f713ebd1a6feab492fb730a8bc13026be114d82f` (клонирован в
`/home/gg/projects/AGENTS/rust-sdk-src`). Все сигнатуры ниже — точные, из исходников
на указанном коммите.

## Задача 1: подписка на `ProgressToken` через `ProgressDispatcher`

**Файл:** `crates/rmcp/src/handler/client/progress.rs`

Метод называется **`subscribe`** (async). Возвращает `ProgressSubscriber` — объект,
реализующий `futures::Stream<Item = ProgressNotificationParam>`. При `Drop` 
`ProgressSubscriber` автоматически отписывается от токена.

```rust
// progress.rs:37
impl ProgressDispatcher {
    /// Subscribe to progress notifications for a specific token.
    ///
    /// If you drop the returned `ProgressSubscriber`, it will automatically
    /// unsubscribe from notifications for that token.
    pub async fn subscribe(&self, progress_token: ProgressToken) -> ProgressSubscriber {
        let (sender, receiver) = tokio::sync::mpsc::channel(Self::CHANNEL_SIZE);
        self.dispatcher
            .write()
            .await
            .insert(progress_token.clone(), sender);
        let receiver = ReceiverStream::new(receiver);
        ProgressSubscriber {
            progress_token,
            receiver,
            dispatcher: self.dispatcher.clone(),
        }
    }

    // progress.rs:52
    pub async fn unsubscribe(&self, token: &ProgressToken) { ... }

    // progress.rs:57
    pub async fn clear(&self) { ... }
}
```

Внутренняя структура (`progress.rs:8-9`):

```rust
type Dispatcher = Arc<RwLock<HashMap<ProgressToken, tokio::sync::mpsc::Sender<ProgressNotificationParam>>>>;
```

`ProgressSubscriber` (`progress.rs:63-100`):

```rust
pub struct ProgressSubscriber {
    pub(crate) progress_token: ProgressToken,
    pub(crate) receiver: ReceiverStream<ProgressNotificationParam>,
    pub(crate) dispatcher: Dispatcher,
}
impl ProgressSubscriber {
    pub fn progress_token(&self) -> &ProgressToken { &self.progress_token }
}
impl Stream for ProgressSubscriber {
    type Item = ProgressNotificationParam;  // poll_next → receiver.poll_next_unpin
}
impl Drop for ProgressSubscriber { ... }  // removes token from dispatcher map
```

**Практический вывод:** подписка делается на `handle.progress_token` (см. Задачу 2),
который генерируется SDK автоматически при каждой отправке запроса. Подписка
обязательна **до** вызова `call_tool()`, но сгенерированный токен известен только
после вызова `send_request_with_option()` (который возвращает `RequestHandle` с
полем `progress_token`) — поэтому порядок: вызвать low-level `send_request_with_option`
→ получить `RequestHandle` → `subscribe(handle.progress_token)` → `handle.await_response()`.

## Задача 2: передача `PeerRequestOptions` в `call_tool`

Тестовый хелпер `call_tool_with_options` из
`crates/rmcp/tests/test_request_timeout_progress.rs:111-126` — обёртка над
**публичным** методом `Peer::send_request_with_option`:

```rust
async fn call_tool_with_options(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ProgressCountingClient>,
    name: &str,
    options: PeerRequestOptions,
) -> Result<rmcp::model::ServerResult, ServiceError> {
    client
        .send_request_with_option(
            ClientRequest::CallToolRequest(Request::new(CallToolRequestParams::new(
                name.to_owned(),
            ))),
            options,
        )
        .await?                    // → RequestHandle<RoleClient>
        .await_response()          // → Result<ServerResult, ServiceError>
        .await
}
```

Используемые здесь типы и сигнатуры (все публичные):

- `Peer::send_request_with_option` — `crates/rmcp/src/service.rs:850-857`:
  ```rust
  pub async fn send_request_with_option(
      &self,
      request: R::Req,
      options: PeerRequestOptions,
  ) -> Result<RequestHandle<R>, ServiceError>
  ```
- `RequestHandle::await_response` — `service.rs:544`:
  ```rust
  pub async fn await_response(mut self) -> Result<R::PeerResp, ServiceError>
  ```
- `ClientRequest::CallToolRequest(Request<CallToolRequestParams>)` — вариант enum
  `ClientRequest` (проверено в тестах). `Request::new(params)` строит запрос.
- Возврат — `ServerResult` (enum); вариант `CallToolResult` извлекается матчем
  `ServerResult::CallToolResult(result)`.

Важный факт про options: **`call_tool()` высокоуровневый НЕ принимает options**.
Его сигнатура (`crates/rmcp/src/service/client.rs:1502`, macro `method!`):

```rust
method!(peer_req call_tool CallToolRequest(CallToolRequestParams) => CallToolResult);
// → pub async fn call_tool(&self, params: CallToolRequestParams) -> Result<CallToolResult, ServiceError>
```

`list_tools` — аналогично через macro (`client.rs:1668`):
```rust
method!(peer_req list_tools ListToolsRequest(Option<PaginatedRequestParams>)? => ListToolsResult);
// → pub async fn list_tools(&self, params: Option<PaginatedRequestParams>) -> Result<ListToolsResult, ServiceError>
```

Для прогресса/cancel нужно использовать именно **low-level путь** через
`send_request_with_option`, потому что только `RequestHandle` даёт `progress_token`
и `RequestId`.

## Задача 3: `RequestId` исходящего запроса и cancel

**Ответ:** способ (а) — есть low-level API, возвращающий и `RequestId`, и
`progress_token`, через `RequestHandle`. Высокоуровневый `call_tool()` этот id
не отдаёт.

`RequestHandle` (`crates/rmcp/src/service.rs:524-538`) — публичные поля:

```rust
#[derive(Debug)]
#[non_exhaustive]
pub struct RequestHandle<R: ServiceRole> {
    pub rx: tokio::sync::oneshot::Receiver<Result<R::PeerResp, ServiceError>>,
    pub options: PeerRequestOptions,
    pub peer: Peer<R>,
    pub id: RequestId,              // ← id исходящего запроса
    pub progress_token: ProgressToken, // ← сгенерированный SDK токен
    progress_reset_rx: Option<mpsc::Receiver<()>>,
}
```

Публичные методы `RequestHandle`:

```rust
// service.rs:544
pub async fn await_response(mut self) -> Result<R::PeerResp, ServiceError>

// service.rs:655 — отмена запроса через MCP-level notifications/cancelled
pub async fn cancel(self, reason: Option<String>) -> Result<(), ServiceError>

// service.rs:541-542
pub const REQUEST_TIMEOUT_REASON: &str = "request timeout";
pub const REQUEST_MAX_TOTAL_TIMEOUT_REASON: &str = "maximum total timeout exceeded";
```

`RequestHandle::cancel(reason)` сам отправляет `notify_cancelled` с `request_id`
на сервер (`service.rs:655-673`) — т.е. **отдельно вызывать `notify_cancelled`
не нужно**, если запрос был отправлен через `send_request_with_option`. Также
`await_response()` при таймауте сам отменяет запрос (`service.rs:559`).

Сгенерированный `RequestId` берётся из `RequestIdProvider` (`service.rs:875`):
```rust
let id = self.request_id_provider.next_request_id();  // AtomicU32Provider → RequestId::Number
```

Доступ к `Peer` из `RunningService` (`crates/rmcp/src/service.rs:1047-1071`):

```rust
pub struct RunningService<R: ServiceRole, S: Service<R>> { ... }

impl<R: ServiceRole, S: Service<R>> Deref for RunningService<R, S> {
    type Target = Peer<R>;
    fn deref(&self) -> &Self::Target { &self.peer }
}
impl<R: ServiceRole, S: Service<R>> RunningService<R, S> {
    pub fn peer(&self) -> &Peer<R> { &self.peer }           // service.rs:1065
    pub fn service(&self) -> &S { self.service.as_ref() }   // service.rs:1069 — доступ к ClientHandler
    pub fn cancellation_token(&self) -> RunningServiceCancellationToken { ... }
    pub fn is_closed(&self) -> bool { ... }
    pub async fn waiting(mut self) -> Result<QuitReason, tokio::task::JoinError> { ... }
    pub async fn close(&mut self) -> Result<QuitReason, tokio::task::JoinError> { ... }
    pub async fn close_with_timeout(&mut self, timeout: Duration) -> Result<Option<QuitReason>, tokio::task::JoinError> { ... }
    pub async fn cancel(mut self) -> Result<QuitReason, tokio::task::JoinError> { ... }
}
```

Через `Deref` методы `Peer<RoleClient>` (в т.ч. `call_tool`, `list_tools`,
`send_request_with_option`, `notify_cancelled`) доступны прямо на
`RunningService`. А через `service()` — доступ к нашему `ClientHandler`
(для `ProgressDispatcher`).

## Полный рабочий пример (из `crates/rmcp/tests/test_progress_subscriber.rs`)

Клиент с `ProgressDispatcher`:

```rust
use futures::StreamExt;
use rmcp::{
    ClientHandler, Peer, RoleServer, ServiceExt,
    handler::client::progress::ProgressDispatcher,
    model::{CallToolRequestParams, ClientRequest, ProgressNotificationParam, Request, RequestMetaObject},
    service::PeerRequestOptions,
};

pub struct MyClient {
    progress_handler: ProgressDispatcher,
}
impl MyClient { pub fn new() -> Self { Self { progress_handler: ProgressDispatcher::new() } } }

impl ClientHandler for MyClient {
    async fn on_progress(&self, params: ProgressNotificationParam, _ctx: rmcp::service::NotificationContext<rmcp::RoleClient>) {
        self.progress_handler.handle_notification(params).await;
    }
}
```

Полный цикл discovery → invoke → progress → completed:

```rust
let client = MyClient::new();
// ... transport setup, spawn server ...
let client_service = client.serve(transport_client).await?;   // ServiceExt::serve

// 1) invоке через low-level (нужен RequestHandle для progress_token/id)
let handle = client_service
    .send_cancellable_request(
        ClientRequest::CallToolRequest(Request::new(CallToolRequestParams::new("some_progress"))),
        PeerRequestOptions::no_options(),
    )
    .await?;   // send_cancellable_request = send_request_with_option

// 2) подписка на прогресс ДО await_response
let mut progress_subscriber = client_service
    .service()                       // &MyClient
    .progress_handler
    .subscribe(handle.progress_token.clone())
    .await;

tokio::spawn(async move {
    while let Some(notification) = progress_subscriber.next().await {
        tracing::info!("Progress: {:?}", notification);
    }
});

// 3) ожидание ответа
let _response = handle.await_response().await?;

// 4) cancel (best-effort, отдельная ветка): handle.cancel(Some("user cancelled")).await?;
```

## Итог для `driver-mcp`

- Самодельный `McpClientHandler` с `DashMap<String, mpsc::Sender<DriverEvent>>`
  заменяется на тонкую обёртку над `ProgressDispatcher` (см.
  `driver_mcp_FINAL_STATUS.md` — схема верна).
- Для прогресса и cancel `driver-mcp` должен ходить через
  `send_request_with_option` (или его алиас `send_cancellable_request`) +
  `RequestHandle`, а не через высокоуровневый `call_tool()`.
- `RequestHandle.progress_token` — сгенерированный SDK токен; класть свой в
  `PeerRequestOptions.meta` можно, но SDK принудительно перезаписывает его своим
  (`service.rs:888`; тест `generated_progress_token_overrides_option_meta_token`
  подтверждает приоритет).
- `RequestHandle.cancel(reason)` сам шлёт `notifications/cancelled` с `request_id` —
  явный `notify_cancelled` не требуется.
- Фичи крейта: `client` (ProgressDispatcher/tokio-stream), `transport-child-process`
  (stdio), `macros` (tool_router — только для тестов/сервера).