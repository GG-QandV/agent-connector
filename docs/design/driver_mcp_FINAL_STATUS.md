# `driver-mcp` — итоговый статус верификации API (после 3 раундов `search_code`)

## Полностью подтверждено чтением исходников `modelcontextprotocol/rust-sdk` (commit `f713ebd1a6feab492fb730a8bc13026be114d82f`)

| API | Файл | Сигнатура/факт |
|---|---|---|
| `call_tool` | `service/client.rs` | `pub async fn call_tool(&self, params: CallToolRequestParams) -> Result<CallToolResult, ServiceError>` |
| `list_tools` | `service/client.rs` | `pub async fn list_tools(&self, params: Option<PaginatedRequestParams>) -> Result<ListToolsResult, ServiceError>` |
| `notify_cancelled` | `service/client.rs` (macro) + `test_subscriptions.rs` | `peer.notify_cancelled(CancelledNotificationParam::new(Some(request_id), Some(reason))).await` |
| `ContentBlock::text/Text/Image` | `model/content.rs`, `model.rs` | `ContentBlock::text(s) -> ContentBlock::Text(TextContent::new(s))`; `ContentBlock::Image(ImageContent)` подтверждён |
| `serve()` вызывается на handler, не на `()` | `progress_client.rs` | `client_handler.serve(transport).await?` — `()` в README это просто unit-type с blanket no-op `ClientHandler` |
| `ClientHandler::on_progress` | `handler/client.rs` | `async fn on_progress(&self, params: ProgressNotificationParam, context: NotificationContext<RoleClient>)` |
| `ProgressNotificationParam` структура | `model.rs` | `pub progress_token: ProgressToken, pub progress: f64, ...` — camelCase на wire, snake_case в Rust |
| **`ProgressDispatcher`** — встроенный SDK helper | `handler/client/progress.rs` | `Arc<RwLock<HashMap<ProgressToken, mpsc::Sender<ProgressNotificationParam>>>>` — заменяет весь мой самодельный `McpClientHandler` с `DashMap` |
| `progress_handler.handle_notification(params).await` | `test_progress_subscriber.rs` | Метод для передачи полученного notification во `ProgressDispatcher` изнутри `on_progress` callback |
| Передача progress token в запрос | `model/meta.rs`, `test_request_timeout_progress.rs` | `RequestMetaObject::with_progress_token(token)`, кладётся в `PeerRequestOptions.meta`, не в `CallToolRequestParams` напрямую |

## Ключевой архитектурный вывод — заменить самодельный код на SDK primitive

Мой `McpClientHandler` с ручным `DashMap<String, mpsc::Sender<DriverEvent>>` (в файле `driver_mcp_v2_progress_confirmed.rs`) — **избыточное переизобретение** того, что SDK уже предоставляет через `ProgressDispatcher`. Правильная финальная структура:

```rust
pub struct McpClientHandler {
    progress: rmcp::handler::client::progress::ProgressDispatcher,
}

impl ClientHandler for McpClientHandler {
    async fn on_progress(&self, params: ProgressNotificationParam, _ctx: NotificationContext<RoleClient>) {
        self.progress.handle_notification(params).await;
    }
}
```

Регистрация/подписка на конкретный token, судя по типу `Dispatcher = HashMap<ProgressToken, mpsc::Sender<ProgressNotificationParam>>`, происходит через какой-то `register`/`subscribe` метод на `ProgressDispatcher`, который я не нашёл текстовым поиском (вероятно private или требует чтения полного файла `progress.rs`, не фрагментами). **Это единственный оставшийся пробел для полной progress-интеграции.**

## Оставшийся открытый пробел — как передать `PeerRequestOptions` в `call_tool()`

Подтверждено, что `PeerRequestOptions.meta` — правильное место для progress token, и что низкоуровневый путь существует (`call_tool_with_options` — но это оказалась **тестовая хелпер-функция в тесте**, не метод самого SDK). Значит либо:

- есть отдельный публичный метод на `RunningService`/`Peer`, принимающий `params + options` (не найден текстовым поиском за 3 попытки — вероятно называется иначе, например `call_tool_with_meta` или через builder на самом `CallToolRequestParams`), либо
- нужно использовать более низкоуровневый `Peer::send_request`-подобный API напрямую, минуя высокоуровневый `call_tool()` хелпер.

## Оставшийся открытый пробел — `RequestId` для `notify_cancelled`

`notify_cancelled` принимает `Option<RequestId>` (подтверждено по вызову `Some(the_request_id)` в README и `Some(context.sink().id().clone())` в тесте — значит `RequestId` **доступен на стороне сервера** через `context.sink().id()`, но не подтверждён способ получить его **на стороне клиента** сразу после отправки `call_tool()` — то есть как клиент узнаёт id своего собственного исходящего запроса, чтобы потом его отменить.

## Рекомендация

Три оставшихся пункта (register API на `ProgressDispatcher`, передача `PeerRequestOptions` в `call_tool`, получение `RequestId` исходящего запроса на клиенте) — это уровень детализации, для которого `search_code` с точечными текстовыми запросами исчерпал эффективность за 3 раунда. Дальнейшая проверка требует либо:

1. Полного построчного чтения `crates/rmcp/src/handler/client/progress.rs` и `crates/rmcp/src/service/client.rs` целиком (не фрагментами через `search_code text_matches`), что нужно делать через `get_file_contents` — который в этой сессии не отдаёт полный текст файла, как выяснилось ранее.
2. Либо клонирования репозитория и `cargo doc --open` локально у разработчика, что даст полный browsable API за секунды вместо десятков точечных grep-запросов.

**Рекомендую разработчику, реализующему этот модуль, использовать вариант 2** — это единственный практичный способ закрыть оставшиеся три пункта. Всё остальное в спеке и коде (`driver_mcp_v2_progress_confirmed.rs`, с заменой `McpClientHandler` на wrapper над `ProgressDispatcher`, как показано выше) уже подтверждено и готово к использованию как основа.

## Итоговая полнота реализации

| Компонент | Статус |
|---|---|
| `AgentDriver` trait impl структура | ✅ Готово |
| `call_tool`/`list_tools` вызовы | ✅ Подтверждено полностью |
| `Part ↔ MCP arguments/content` маппинг | ✅ Готово |
| Progress notification receiving (`ClientHandler`) | ✅ Механизм подтверждён, требует замены на `ProgressDispatcher` |
| Progress token → outgoing request association | ⚠️ Частично — `RequestMetaObject::with_progress_token` подтверждён, точка вставки в `call_tool()` не найдена |
| Cancel via `notify_cancelled` | ⚠️ Метод подтверждён, получение `RequestId` на клиенте не подтверждено |
| Discovery/`tools/list` пагинация | ✅ Готово |
| Security (allowlist, input validation) | ✅ Архитектурно специфицировано в `driver-mcp-spec.md` |
