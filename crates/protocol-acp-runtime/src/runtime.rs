//! ACP stdio runtime: построчный JSON-RPC loop над AdapterCore через mapper.

use crate::codec::*;
use adapter_core::{
    AdapterCore, Caller, CallerId, CoreCommand, CoreError, CoreEvent, DispatchResult, TaskId,
    TaskSubscription,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Обработчик ACP-запросов: маппинг на AdapterCore.
#[async_trait]
pub trait AcpHandler: Send + Sync {
    async fn dispatch(
        &self,
        caller: Caller,
        command: CoreCommand,
    ) -> Result<DispatchResult, CoreError>;
    async fn subscribe(
        &self,
        task_id: TaskId,
        after_seq: u64,
    ) -> Result<TaskSubscription, CoreError>;
    async fn history(&self, task_id: TaskId, after_seq: u64) -> Result<Vec<CoreEvent>, CoreError>;
}

#[async_trait]
impl AcpHandler for AdapterCore {
    async fn dispatch(
        &self,
        caller: Caller,
        command: CoreCommand,
    ) -> Result<DispatchResult, CoreError> {
        self.dispatch(caller, command).await
    }
    async fn subscribe(
        &self,
        task_id: TaskId,
        after_seq: u64,
    ) -> Result<TaskSubscription, CoreError> {
        self.subscribe(task_id, after_seq).await
    }
    async fn history(&self, task_id: TaskId, after_seq: u64) -> Result<Vec<CoreEvent>, CoreError> {
        self.history(task_id, after_seq).await
    }
}

#[derive(Clone, Debug)]
pub struct AcpRuntimeConfig {
    pub max_line_bytes: usize,
    /// Окно ожидания завершения in-flight запросов после `shutdown`.
    /// Используется `run_with_shutdown`: после внешнего shutdown-сигнала
    /// runtime отвергает новые top-level requests и ждёт завершения текущей
    /// строки не дольше этого таймаута.
    pub shutdown_grace: std::time::Duration,
    pub agent_id: String,
    pub agent_name: String,
    pub agent_version: String,
    pub capabilities: serde_json::Value,
}

impl Default for AcpRuntimeConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: 1024 * 1024,
            shutdown_grace: std::time::Duration::from_secs(5),
            agent_id: "adapter".into(),
            agent_name: "agent-connector".into(),
            agent_version: "0.1.0".into(),
            capabilities: serde_json::json!({
                "filesystem": false,
                "terminal": false,
                "streaming": true,
                "cancellation": true,
                "sessionResume": true
            }),
        }
    }
}

/// Duplex для тестов и реального stdio.
#[derive(Debug)]
pub struct StdinOut<R: AsyncBufRead + Unpin + Send, W: AsyncWrite + Unpin + Send> {
    pub reader: BufReader<R>,
    pub writer: W,
}

/// Диспетчер ACP-методов; владеет core/config/caller/drain — не касается I/O.
struct Dispatcher {
    core: Arc<dyn AcpHandler>,
    config: AcpRuntimeConfig,
    caller: Caller,
    drain_token: CancellationToken,
}

impl Dispatcher {
    fn new(
        core: Arc<dyn AcpHandler>,
        config: AcpRuntimeConfig,
        caller_id: impl Into<String>,
    ) -> Self {
        Self {
            core,
            config,
            caller: Caller {
                id: CallerId(caller_id.into()),
                scopes: Vec::new(),
            },
            drain_token: CancellationToken::new(),
        }
    }

    fn drain_token(&self) -> CancellationToken {
        self.drain_token.clone()
    }

    async fn handle(&self, request: &JsonRpcRequest, id: Option<JsonRpcId>) -> JsonRpcResponse {
        let params = request.params.clone().unwrap_or(Value::Null);
        match request.method.as_str() {
            "initialize" => self.method_initialize(id, &params),
            "shutdown" => {
                self.drain_token.cancel();
                JsonRpcResponse::success(id, Value::Null)
            }
            "session/new" => self.method_session_new(id, &params).await,
            "session/prompt" => self.method_session_prompt(id, &params).await,
            "session/cancel" => self.method_session_cancel(id, &params).await,
            "session/input" => self.method_session_input(id, &params).await,
            "session/update" => self.method_session_update(id, &params).await,
            "session/get" => self.method_session_get(id, &params).await,
            method => JsonRpcResponse::failure(id, method_not_found(method)),
        }
    }

    fn method_initialize(&self, id: Option<JsonRpcId>, _params: &Value) -> JsonRpcResponse {
        JsonRpcResponse::success(
            id,
            serde_json::json!({
                "protocolVersion": "1",
                "agent": {
                    "name": self.config.agent_name,
                    "version": self.config.agent_version,
                },
                "capabilities": self.config.capabilities,
            }),
        )
    }

    async fn method_session_new(&self, id: Option<JsonRpcId>, _params: &Value) -> JsonRpcResponse {
        let session_id = uuid::Uuid::new_v4();
        JsonRpcResponse::success(
            id,
            serde_json::json!({ "sessionId": session_id.to_string() }),
        )
    }

    async fn method_session_prompt(
        &self,
        id: Option<JsonRpcId>,
        params: &Value,
    ) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let request_id = params
            .get("requestId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if request_id.is_empty() {
            return JsonRpcResponse::failure(id, invalid_request("requestId is required"));
        }
        let session_uuid = if session_id.is_empty() {
            None
        } else {
            session_id.parse::<uuid::Uuid>().ok()
        };
        let input = extract_parts(params);
        let command = CoreCommand::Invoke(adapter_model::InvokeRequest {
            task_id: None,
            agent_id: None,
            skill_id: None,
            idempotency_key: format!("acp:{}", request_id),
            session_id: session_uuid,
            input,
            context: params.get("metadata").cloned().unwrap_or(Value::Null),
            deadline: None,
        });
        match self.core.dispatch(self.caller.clone(), command).await {
            Ok(DispatchResult::Created(snapshot) | DispatchResult::Existing(snapshot)) => {
                JsonRpcResponse::success(
                    id,
                    serde_json::json!({
                        "sessionId": snapshot.session_id.map(|s| s.to_string()).unwrap_or_default(),
                        "taskId": snapshot.task_id.to_string(),
                    }),
                )
            }
            Ok(_) => JsonRpcResponse::failure(id, internal_error("unexpected core result")),
            Err(e) => JsonRpcResponse::failure(id, core_error(e)),
        }
    }

    async fn method_session_cancel(
        &self,
        id: Option<JsonRpcId>,
        params: &Value,
    ) -> JsonRpcResponse {
        let task_id = params
            .get("taskId")
            .and_then(Value::as_str)
            .and_then(|v| v.parse().ok());
        match task_id {
            Some(task_id) => {
                match self
                    .core
                    .dispatch(
                        self.caller.clone(),
                        CoreCommand::Cancel {
                            task_id,
                            reason: Some("ACP cancel".into()),
                        },
                    )
                    .await
                {
                    Ok(_) => JsonRpcResponse::success(id, Value::Null),
                    Err(e) => JsonRpcResponse::failure(id, core_error(e)),
                }
            }
            None => JsonRpcResponse::failure(id, invalid_request("taskId (UUID) is required")),
        }
    }

    async fn method_session_input(&self, id: Option<JsonRpcId>, params: &Value) -> JsonRpcResponse {
        let task_id = params
            .get("taskId")
            .and_then(Value::as_str)
            .and_then(|v| v.parse().ok());
        match task_id {
            Some(task_id) => {
                let input = extract_parts(params);
                match self
                    .core
                    .dispatch(
                        self.caller.clone(),
                        CoreCommand::ProvideInput { task_id, input },
                    )
                    .await
                {
                    Ok(_) => JsonRpcResponse::success(id, Value::Null),
                    Err(e) => JsonRpcResponse::failure(id, core_error(e)),
                }
            }
            None => JsonRpcResponse::failure(id, invalid_request("taskId (UUID) is required")),
        }
    }

    async fn method_session_get(&self, id: Option<JsonRpcId>, params: &Value) -> JsonRpcResponse {
        let task_id = params
            .get("taskId")
            .and_then(Value::as_str)
            .and_then(|v| v.parse().ok());
        match task_id {
            Some(task_id) => match self
                .core
                .dispatch(self.caller.clone(), CoreCommand::GetStatus { task_id })
                .await
            {
                Ok(DispatchResult::Status(snapshot)) => JsonRpcResponse::success(
                    id,
                    serde_json::json!({
                        "taskId": snapshot.task_id.to_string(),
                        // Стабильная wire-сериализация, а не Debug-вывод:
                        // формат не зависит от derive(Debug).
                        "state": serde_json::to_value(snapshot.state).unwrap_or_default(),
                    }),
                ),
                Ok(_) => JsonRpcResponse::failure(id, internal_error("unexpected core result")),
                Err(e) => JsonRpcResponse::failure(id, core_error(e)),
            },
            None => JsonRpcResponse::failure(id, invalid_request("taskId (UUID) is required")),
        }
    }

    async fn method_session_update(
        &self,
        id: Option<JsonRpcId>,
        params: &Value,
    ) -> JsonRpcResponse {
        let task_id = params
            .get("taskId")
            .and_then(Value::as_str)
            .and_then(|v| v.parse().ok());
        match task_id {
            Some(task_id) => {
                // session/update — pull-модель: отдаём снимок истории
                // (клиент пере-поллит при необходимости). Никакой live-подписки
                // не создаём — это snapshot-ответ по протоколу ACP.
                match self.core.history(task_id, 0).await {
                    Ok(events) => {
                        let events: Vec<Value> = events.iter().map(event_to_json).collect();
                        JsonRpcResponse::success(id, serde_json::json!({ "events": events }))
                    }
                    Err(e) => JsonRpcResponse::failure(id, core_error(e)),
                }
            }
            None => JsonRpcResponse::failure(id, invalid_request("taskId (UUID) is required")),
        }
    }
}

pub struct AcpRuntime<R: AsyncBufRead + Unpin + Send, W: AsyncWrite + Unpin + Send> {
    dispatcher: Dispatcher,
    io: StdinOut<R, W>,
}

impl<R: AsyncBufRead + Unpin + Send, W: AsyncWrite + Unpin + Send> AcpRuntime<R, W> {
    pub fn new(
        core: Arc<dyn AcpHandler>,
        config: AcpRuntimeConfig,
        caller_id: impl Into<String>,
        io: StdinOut<R, W>,
    ) -> Self {
        Self {
            dispatcher: Dispatcher::new(core, config, caller_id),
            io,
        }
    }

    pub fn drain_token(&self) -> CancellationToken {
        self.dispatcher.drain_token()
    }

    pub async fn run(&mut self) {
        let dispatcher = &self.dispatcher;
        let reader = &mut self.io.reader;
        let writer = &mut self.io.writer;
        let mut lines = reader.lines();
        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => break, // EOF
                Err(e) => {
                    error!(error = %e, "stdin read error");
                    break;
                }
            };
            if dispatcher.drain_token.is_cancelled() {
                // Draining: молча не дропаем — отвечаем ошибкой, чтобы клиент
                // знал, что сервер завершает работу и нужно переподключиться.
                debug!("draining: rejecting new top-level request");
                if let Ok(Some(request)) = JsonRpcRequest::parse(&line) {
                    if !request.is_notification() {
                        let id = request.id.clone();
                        let err = internal_error("server is shutting down");
                        write_line(writer, JsonRpcResponse::failure(id, err)).await;
                    }
                }
                continue;
            }
            if line.len() > dispatcher.config.max_line_bytes {
                let err = internal_error("line exceeds max_line_bytes");
                write_line(writer, JsonRpcResponse::failure(None, err)).await;
                continue;
            }
            match JsonRpcRequest::parse(&line) {
                Ok(Some(request)) => {
                    if request.is_notification() {
                        let _ = dispatcher.handle(&request, None).await;
                    } else {
                        let id = request.id.clone();
                        let response = dispatcher.handle(&request, id).await;
                        write_line(writer, response).await;
                    }
                }
                Ok(None) => {}
                Err((error, id)) => {
                    write_line(writer, JsonRpcResponse::failure(id, error)).await;
                }
            }
        }
        debug!("acp runtime loop exited");
    }

    /// Запустить runtime с внешним shutdown-сигналом (например, SIGINT).
    /// Как только `external_shutdown` срабатывает, runtime переходит в
    /// draining: перестаёт принимать новые top-level requests (отвечая
    /// клиенту явной ошибкой "server is shutting down"), ждёт завершения
    /// уже читаемой строки в пределах `shutdown_grace`, затем возвращается.
    pub async fn run_with_shutdown(&mut self, external_shutdown: CancellationToken) {
        let grace = self.dispatcher.config.shutdown_grace;
        tokio::select! {
            _ = self.run() => {}
            _ = external_shutdown.cancelled() => {
                self.dispatcher.drain_token.cancel();
                debug!("external shutdown signal received, draining ACP runtime");
                // Даём текущему in-flight чтению/обработке строки шанс
                // завершиться в пределах grace period, а не рвём соединение
                // немедленно. run() сам продолжит читать stdin, но будет
                // отвергать НОВЫЕ top-level requests из-за drain_token.
                let _ = tokio::time::timeout(grace, self.run()).await;
            }
        }
    }
}

async fn write_line<W: AsyncWrite + Unpin + Send>(writer: &mut W, response: JsonRpcResponse) {
    let line = response.to_line();
    if let Err(e) = writer.write_all(line.as_bytes()).await {
        error!(error = %e, "stdout write failed");
        return;
    }
    if let Err(e) = writer.flush().await {
        error!(error = %e, "stdout flush failed");
    }
}

fn extract_parts(params: &Value) -> Vec<adapter_model::Part> {
    params
        .get("prompt")
        .and_then(Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|block| match block.get("kind").and_then(Value::as_str) {
                    Some("text") => block.get("text").and_then(Value::as_str).map(|t| {
                        adapter_model::Part::Text {
                            text: t.to_string(),
                        }
                    }),
                    Some("resource") => Some(adapter_model::Part::FileRef {
                        uri: block
                            .get("uri")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .into(),
                        mime_type: block
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    }),
                    _ => block
                        .get("json")
                        .cloned()
                        .map(|v| adapter_model::Part::Json { value: v }),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn event_to_json(event: &CoreEvent) -> Value {
    serde_json::json!({
        "seq": event.seq,
        "taskId": event.task_id.to_string(),
        // Стабильная wire-сериализация вместо Debug-вывода.
        "kind": serde_json::to_value(&event.kind).unwrap_or_default(),
    })
}

fn core_error(error: CoreError) -> JsonRpcError {
    match error {
        CoreError::InvalidRequest(msg) => invalid_request(msg),
        CoreError::AgentNotFound(id) => internal_error(format!("agent not found: {id}")),
        CoreError::NoEligibleAgent => internal_error("no eligible agent"),
        CoreError::ResourceExhausted(msg) => internal_error(msg),
        CoreError::Driver(msg) => internal_error(msg),
        CoreError::Timeout => internal_error("task timed out"),
        CoreError::Store(e) => internal_error(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;
    use std::io::Cursor;

    struct FakeHandler {
        dispatch_count: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl AcpHandler for FakeHandler {
        async fn dispatch(
            &self,
            _caller: Caller,
            command: CoreCommand,
        ) -> Result<DispatchResult, CoreError> {
            self.dispatch_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match command {
                CoreCommand::Invoke(_) => {
                    let id = uuid::Uuid::new_v4();
                    Ok(DispatchResult::Created(adapter_model::TaskSnapshot {
                        task_id: id,
                        session_id: None,
                        agent_id: adapter_model::AgentId("fake".into()),
                        caller_id: adapter_model::CallerId("c".into()),
                        state: adapter_model::TaskState::Accepted,
                        revision: 1,
                        last_seq: 1,
                        created_at: chrono::Utc::now(),
                        updated_at: chrono::Utc::now(),
                        terminal_at: None,
                    }))
                }
                CoreCommand::GetStatus { .. } => Err(CoreError::AgentNotFound("fake".into())),
                _ => Err(CoreError::InvalidRequest("unsupported".into())),
            }
        }
        async fn subscribe(
            &self,
            _task_id: TaskId,
            _after_seq: u64,
        ) -> Result<TaskSubscription, CoreError> {
            Err(CoreError::AgentNotFound("fake".into()))
        }
        async fn history(
            &self,
            _task_id: TaskId,
            _after_seq: u64,
        ) -> Result<Vec<CoreEvent>, CoreError> {
            Err(CoreError::AgentNotFound("fake".into()))
        }
    }

    type TestRuntime = AcpRuntime<Cursor<Vec<u8>>, Vec<u8>>;

    fn make_runtime(input: &str) -> (TestRuntime, Arc<FakeHandler>) {
        let handler = Arc::new(FakeHandler {
            dispatch_count: std::sync::atomic::AtomicUsize::new(0),
        });
        let io = StdinOut {
            reader: BufReader::new(Cursor::new(input.as_bytes().to_vec())),
            writer: Vec::new(),
        };
        let runtime = AcpRuntime::new(handler.clone(), AcpRuntimeConfig::default(), "test", io);
        (runtime, handler)
    }

    fn lines(out: &[u8]) -> Vec<String> {
        String::from_utf8_lossy(out)
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[tokio::test]
    async fn valid_request_gets_one_response_with_same_id() {
        let (mut runtime, _) = make_runtime(
            "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"session/new\",\"params\":{}}\n",
        );
        runtime.run().await;
        let out = lines(&runtime.io.writer);
        assert_eq!(out.len(), 1);
        let resp: JsonRpcResponse = serde_json::from_str(&out[0]).unwrap();
        assert_eq!(resp.id, Some(JsonRpcId::Number(7)));
        assert!(resp.result.is_some());
    }

    #[tokio::test]
    async fn malformed_json_gets_parse_error_then_continues() {
        let (mut runtime, _) =
            make_runtime("not json\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"session/new\"}\n");
        runtime.run().await;
        let out = lines(&runtime.io.writer);
        assert_eq!(out.len(), 2);
        let err: JsonRpcResponse = serde_json::from_str(&out[0]).unwrap();
        assert_eq!(err.error.unwrap().code, codec::PARSE_ERROR);
        let ok: JsonRpcResponse = serde_json::from_str(&out[1]).unwrap();
        assert_eq!(ok.id, Some(JsonRpcId::Number(2)));
    }

    #[tokio::test]
    async fn notification_never_writes_stdout() {
        let (mut runtime, _) =
            make_runtime("{\"jsonrpc\":\"2.0\",\"method\":\"session/new\",\"params\":{}}\n");
        runtime.run().await;
        assert!(runtime.io.writer.is_empty());
    }

    #[tokio::test]
    async fn invalid_envelope_gets_invalid_request() {
        let (mut runtime, _) =
            make_runtime("{\"jsonrpc\":\"1.0\",\"id\":1,\"method\":\"session/new\"}\n");
        runtime.run().await;
        let out = lines(&runtime.io.writer);
        assert_eq!(out.len(), 1);
        let resp: JsonRpcResponse = serde_json::from_str(&out[0]).unwrap();
        assert_eq!(resp.error.unwrap().code, codec::INVALID_REQUEST);
    }

    #[tokio::test]
    async fn eof_is_clean_shutdown() {
        let (mut runtime, _) =
            make_runtime("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/new\"}\n");
        runtime.run().await;
        assert!(!runtime.io.writer.is_empty());
    }

    #[tokio::test]
    async fn draining_rejects_requests_with_shutdown_error() {
        let (mut runtime, _) = make_runtime(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/new\",\"params\":{}}\n",
        );
        runtime.drain_token().cancel();
        runtime.run().await;
        let out = lines(&runtime.io.writer);
        assert_eq!(out.len(), 1);
        let resp: JsonRpcResponse = serde_json::from_str(&out[0]).unwrap();
        assert_eq!(resp.id, Some(JsonRpcId::Number(1)));
        let error = resp.error.unwrap();
        assert_eq!(error.code, codec::INTERNAL_ERROR);
        assert!(error.message.contains("shutting down"));
    }

    #[tokio::test]
    async fn draining_ignores_notifications_without_response() {
        let (mut runtime, _) =
            make_runtime("{\"jsonrpc\":\"2.0\",\"method\":\"session/new\",\"params\":{}}\n");
        runtime.drain_token().cancel();
        runtime.run().await;
        // Notification во время draining не получает ответ (по контракту).
        assert!(runtime.io.writer.is_empty());
    }
}
