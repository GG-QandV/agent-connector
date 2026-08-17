//! crates/driver-a2a-client/src/lib.rs
//!
//! Точка входа драйвера. Диспетчеризация по wire_format происходит один
//! раз в new() — дальше вся логика invoke/get/cancel работает только с
//! NormalizedTask и не знает, какой диалект JSON-RPC используется.
//!
//! Этот файл = чистовой клиент (docs/design/lib_driver.rs) + обёртка
//! `AgentDriver` (адаптер к `adapter_core`): чистовой клиент оперирует
//! NormalizedTask, а `AgentDriver` транслирует его в DriverEvent.

pub mod dialect_probe;
pub mod error;
pub mod wire;

use adapter_core::{AgentDriver, CoreError, DriverCapabilities, DriverEvent};
use adapter_model::{InvokeRequest, Part, PublicError, TaskId};
use async_trait::async_trait;
use dashmap::DashMap;
use dialect_probe::{detect_from_agent_card, probe_wire_format};
use error::{from_jsonrpc_error, A2aClientError};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, OnceCell};
use tokio_util::sync::CancellationToken;
use wire::{
    sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire, NormalizedPart, NormalizedState,
    NormalizedTask,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum A2aWireFormat {
    #[default]
    Sdk,
    Spec,
    /// Диалект неизвестен на момент конфигурации — определяется зондом
    /// (dialect_probe::probe_wire_format) при первом вызове execute(),
    /// результат кэшируется в OnceCell на весь lifetime драйвера.
    /// Приоритет при неоднозначности — Sdk (§3.4 ТЗ).
    Auto,
}

#[derive(Clone, Debug)]
pub struct A2aClientConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub wire_format: A2aWireFormat,
    pub timeout_secs: u64,
    /// Опциональный URL карточки агента (обычно
    /// "<base>/.well-known/agent.json"). Если задан и wire_format == Auto,
    /// детект по protocolVersion пробуется ПЕРВЫМ, зонд — fallback, если
    /// карточка недоступна или не содержит protocolVersion (ТЗ §3.2 п.4:
    /// "предпочтительный канал определения (без probe)").
    pub agent_card_url: Option<String>,
}

impl Default for A2aClientConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            token: None,
            wire_format: A2aWireFormat::default(),
            timeout_secs: 30,
            agent_card_url: None,
        }
    }
}

/// Маппинг core TaskId -> удалённый (remote) task id, который сервер вернул
/// в ответ на первый SendMessage. Нужен cancel/provide_input, чтобы они
/// обращались к правильному remote-заданию, а не к core id драйвера.
type RemoteTaskIds = Arc<DashMap<TaskId, String>>;
/// Локальные сигналы отмены: core TaskId -> CancellationToken.
type CancellationTokens = Arc<DashMap<TaskId, CancellationToken>>;

/// Чистовой A2A-клиент: wire-формат-нейтрален, оперирует NormalizedTask.
pub struct A2aClientDriver {
    config: A2aClientConfig,
    client: reqwest::Client,
    /// Заполнено сразу в new() при wire_format != Auto. При Auto — None,
    /// резолвится лениво через auto_wire_cache при первом execute().
    wire: Option<Arc<dyn A2aWire>>,
    /// Кэш результата зонда — заполняется один раз при wire_format == Auto,
    /// повторный зонд не выполняется (OnceCell гарантирует однократность
    /// инициализации сам по себе).
    auto_wire_cache: OnceCell<Arc<dyn A2aWire>>,
    remote_task_ids: RemoteTaskIds,
    cancellation_tokens: CancellationTokens,
}

impl A2aClientDriver {
    pub fn new(config: A2aClientConfig) -> Result<Self, A2aClientError> {
        let wire: Option<Arc<dyn A2aWire>> = match config.wire_format {
            A2aWireFormat::Sdk => Some(Arc::new(A2aSdkWire)),
            A2aWireFormat::Spec => Some(Arc::new(A2aSpecWire)),
            A2aWireFormat::Auto => None,
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        Ok(Self {
            config,
            client,
            wire,
            auto_wire_cache: OnceCell::new(),
            remote_task_ids: Arc::new(DashMap::new()),
            cancellation_tokens: Arc::new(DashMap::new()),
        })
    }

    /// Возвращает актуальный wire — сразу для Sdk/Spec, лениво через зонд
    /// для Auto (кэшируется в auto_wire_cache). Единственная точка, через
    /// которую execute() получает wire.
    async fn resolved_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
        if let Some(w) = &self.wire {
            return Ok(w.clone());
        }
        self.auto_wire_cache
            .get_or_try_init(|| self.resolve_auto_wire())
            .await
            .cloned()
    }

    /// Порядок резолюции при Auto (ТЗ §3.2): сначала AgentCard.protocolVersion
    /// (если agent_card_url задан) — предпочтительный канал, без побочных
    /// эффектов и без сетевого зонда на сам endpoint. Если карточка
    /// недоступна, не содержит protocolVersion, или agent_card_url не
    /// сконфигурирован — fallback на probe_wire_format (зонд).
    async fn resolve_auto_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
        if let Some(card_url) = &self.config.agent_card_url {
            if let Some(wire) = detect_from_agent_card(&self.client, card_url).await {
                return Ok(wire);
            }
            // Карточка недоступна/неинформативна — не считаем это фатальной
            // ошибкой, просто падаем на зонд ниже (по духу ТЗ: "предпочтительнее",
            // не "обязательно").
        }

        probe_wire_format(
            &self.client,
            &self.config.endpoint,
            self.config.token.as_deref(),
        )
        .await
    }

    /// Отправляет новое сообщение (SendMessage / message/send в зависимости
    /// от wire_format). task_id передаётся, если это продолжение диалога.
    pub async fn invoke(
        &self,
        text: &str,
        context_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<NormalizedTask, A2aClientError> {
        let parts = vec![NormalizedPart::text(text)];
        self.send_parts(&parts, context_id, task_id).await
    }

    /// Отправляет части (включая Json/FileRef) как сообщение. Внутренний
    /// примитив, на котором держится invoke(text) и provide_input.
    async fn send_parts(
        &self,
        parts: &[NormalizedPart],
        context_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<NormalizedTask, A2aClientError> {
        let op = A2aOperation::SendMessage {
            parts,
            context_id,
            task_id,
        };
        self.execute(op).await
    }

    /// Опрашивает статус существующей задачи (GetTask / tasks/get) — нужен
    /// для continuation после InputRequired без повторной отправки message.
    pub async fn get_task(&self, task_id: &str) -> Result<NormalizedTask, A2aClientError> {
        self.execute(A2aOperation::GetTask { task_id }).await
    }

    /// Отменяет задачу (CancelTask / tasks/cancel).
    pub async fn cancel_task(&self, task_id: &str) -> Result<NormalizedTask, A2aClientError> {
        self.execute(A2aOperation::CancelTask { task_id }).await
    }

    async fn execute(&self, op: A2aOperation<'_>) -> Result<NormalizedTask, A2aClientError> {
        let wire = self.resolved_wire().await?;
        let method = wire.jsonrpc_method(&op);
        let params = wire.build_params(&op);

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let mut req = self.client.post(&self.config.endpoint).json(&payload);
        if let Some(token) = &self.config.token {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        let body: Value = resp
            .json()
            .await
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        if let Some(err) = body.get("error") {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(-32000);
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            let first_error = from_jsonrpc_error(code, message, method, wire.name());

            // D3: резолюция была Auto (self.wire.is_none()) и первая попытка
            // дала MethodNotFound — пробуем ОДИН РАЗ заново
            // resolve_auto_wire() (минуя кэш), вдруг зонд на этот раз
            // выберет другой диалект. Например, если первая попытка
            // ошибочно закэшировала неверный wire, или сервер временно
            // вернул нестандартный ответ зонду. Не более одной повторной
            // попытки — иначе риск бесконечного цикла на действительно
            // недоступном сервере.
            if self.wire.is_none() && matches!(first_error, A2aClientError::MethodNotFound { .. }) {
                let retried_wire = self.resolve_auto_wire().await?;
                let retried_method = retried_wire.jsonrpc_method(&op);
                let retried_params = retried_wire.build_params(&op);
                let retried_payload = json!({
                    "jsonrpc": "2.0", "id": 1,
                    "method": retried_method, "params": retried_params,
                });
                let mut retried_req = self
                    .client
                    .post(&self.config.endpoint)
                    .json(&retried_payload);
                if let Some(token) = &self.config.token {
                    retried_req = retried_req.bearer_auth(token);
                }
                let retried_resp = retried_req
                    .send()
                    .await
                    .map_err(|e| A2aClientError::Http(e.to_string()))?;
                let retried_body: Value = retried_resp
                    .json()
                    .await
                    .map_err(|e| A2aClientError::Http(e.to_string()))?;
                if let Some(retried_err) = retried_body.get("error") {
                    let rcode = retried_err
                        .get("code")
                        .and_then(Value::as_i64)
                        .unwrap_or(-32000);
                    let rmessage = retried_err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown error");
                    return Err(from_jsonrpc_error(
                        rcode,
                        rmessage,
                        retried_method,
                        retried_wire.name(),
                    ));
                }
                let retried_result = retried_body.get("result").ok_or_else(|| {
                    A2aClientError::ProtocolError(
                        "missing 'result' in JSON-RPC response (retry)".into(),
                    )
                })?;
                return retried_wire.parse_task(retried_result);
            }

            return Err(first_error);
        }

        let result = body.get("result").ok_or_else(|| {
            A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into())
        })?;

        wire.parse_task(result)
    }

    /// Core-части (Text/Json/FileRef) -> NormalizedPart. Json сериализуется
    /// в текст, FileRef сохраняет uri/mime_type — ничего не теряется.
    fn core_parts_to_normalized(parts: &[Part]) -> Vec<NormalizedPart> {
        parts
            .iter()
            .map(|p| match p {
                Part::Text { text } => NormalizedPart::text(text.clone()),
                Part::Json { value } => NormalizedPart::text(value.to_string()),
                Part::FileRef { uri, mime_type } => NormalizedPart {
                    text: None,
                    raw: None,
                    uri: Some(uri.clone()),
                    mime_type: mime_type.clone(),
                },
            })
            .collect()
    }

    /// Нормализованный Part -> adapter_model::Part (текст/uri; raw пропускаем).
    fn normalized_to_part(p: &NormalizedPart) -> Part {
        if let Some(text) = &p.text {
            Part::Text { text: text.clone() }
        } else if let Some(uri) = &p.uri {
            Part::FileRef {
                uri: uri.clone(),
                mime_type: p.mime_type.clone(),
            }
        } else {
            Part::Text {
                text: String::new(),
            }
        }
    }

    /// NormalizedTask -> DriverEvent (терминальное событие).
    fn task_to_terminal(task: &NormalizedTask) -> DriverEvent {
        match task.state {
            NormalizedState::Completed => {
                let parts = task
                    .output_parts
                    .iter()
                    .map(Self::normalized_to_part)
                    .collect();
                DriverEvent::Completed(parts)
            }
            NormalizedState::Failed | NormalizedState::Rejected => {
                DriverEvent::Failed(PublicError {
                    code: "a2a_task_failed".into(),
                    message: task
                        .status_message
                        .clone()
                        .unwrap_or_else(|| "A2A task failed".into()),
                    retryable: false,
                })
            }
            NormalizedState::Canceled => DriverEvent::Cancelled,
            NormalizedState::InputRequired | NormalizedState::AuthRequired => {
                DriverEvent::InputRequired(adapter_model::InputRequest {
                    question: task
                        .status_message
                        .clone()
                        .unwrap_or_else(|| "input required".into()),
                    schema: None,
                })
            }
            _ => DriverEvent::Progress {
                message: task.status_message.clone().unwrap_or_default(),
                percent: None,
            },
        }
    }

    fn is_terminal(state: &NormalizedState) -> bool {
        matches!(
            state,
            NormalizedState::Completed
                | NormalizedState::Failed
                | NormalizedState::Canceled
                | NormalizedState::Rejected
        )
    }
}

// ============================================================
// AgentDriver — обёртка над чистовым клиентом
// ============================================================

#[async_trait]
impl AgentDriver for A2aClientDriver {
    fn id(&self) -> &str {
        // Для A2A-клиента id не входит в чистовой клиент (там только config).
        // Используем endpoint как человекочитаемый id.
        &self.config.endpoint
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            provide_input: true,
        }
    }

    async fn health(&self) -> Result<(), CoreError> {
        // Пустое сообщение — сервер вернёт task (или понятную ошибку).
        self.invoke("", None, None)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("A2A health check failed: {e}")))
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let cancel_token = CancellationToken::new();
        self.cancellation_tokens
            .insert(task_id, cancel_token.clone());

        let (tx, rx) = mpsc::channel(32);
        // Клиент не Clone (reqwest Client и wire — Arc, но struct не Clone).
        // Строим локального вызовчика из полей.
        let driver = self.clone_state();
        let remote_task_ids = self.remote_task_ids.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();

        tokio::spawn(async move {
            let _ = tx.send(DriverEvent::Accepted).await;

            let normalized_parts = A2aClientDriver::core_parts_to_normalized(&request.input);
            let session = request.session_id.map(|s| s.to_string());

            // Первый SendMessage идёт без remote task_id: сервер создаёт
            // задание и возвращает его id в ответе.
            let outcome = tokio::select! {
                result = driver.send_parts(&normalized_parts, session.as_deref(), None) => {
                    match result {
                        Ok(task) => {
                            let remote_id = task.id.clone();
                            remote_task_ids.insert(task_id, remote_id);
                            let terminal = A2aClientDriver::is_terminal(&task.state);
                            let event = A2aClientDriver::task_to_terminal(&task);
                            let _ = tx.send(event).await;
                            if terminal {
                                remote_task_ids.remove(&task_id);
                                cancellation_tokens.remove(&task_id);
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(DriverEvent::Failed(PublicError {
                                code: "a2a_call_failed".into(),
                                message: e.to_string(),
                                retryable: false,
                            })).await;
                            cancellation_tokens.remove(&task_id);
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    let _ = tx.send(DriverEvent::Cancelled).await;
                    remote_task_ids.remove(&task_id);
                    cancellation_tokens.remove(&task_id);
                }
            };
            let _ = outcome;
        });

        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        // Локальная отмена приоритетна; HTTP-отмена — best-effort.
        let remote_id = self
            .remote_task_ids
            .get(&task_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                CoreError::Driver(format!("no active A2A task for core task_id={task_id}"))
            })?;

        if let Some(token_entry) = self.cancellation_tokens.get(&task_id) {
            token_entry.value().cancel();
        }

        let result = self
            .cancel_task(&remote_id)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(e.to_string()));
        self.remote_task_ids.remove(&task_id);
        self.cancellation_tokens.remove(&task_id);
        result
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError> {
        // A2A multi-turn: повторный message/send с тем же remote task_id.
        let remote_id = self
            .remote_task_ids
            .get(&task_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                CoreError::Driver(format!("no active A2A task for core task_id={task_id}"))
            })?;

        let normalized_parts = Self::core_parts_to_normalized(&input);
        self.send_parts(&normalized_parts, None, Some(&remote_id))
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("A2A provide_input failed: {e}")))
    }
}

impl A2aClientDriver {
    fn clone_state(&self) -> Self {
        Self {
            config: self.config.clone(),
            client: self.client.clone(),
            wire: self.wire.clone(),
            // OnceCell не Clone напрямую при непустом значении в общем
            // случае, но здесь нужно ПЕРЕДАТЬ УЖЕ РЕЗОЛВЛЕННЫЙ wire в
            // спавненную задачу invoke() — не начинать новый зонд в клоне.
            // Если auto_wire_cache уже инициализирован, копируем его
            // содержимое в новый OnceCell; если нет — оставляем пустым
            // (резолвится при первом execute() уже внутри клона).
            auto_wire_cache: {
                let cloned = OnceCell::new();
                if let Some(w) = self.auto_wire_cache.get() {
                    // set() не может провалиться на свежем OnceCell.
                    let _ = cloned.set(w.clone());
                }
                cloned
            },
            remote_task_ids: self.remote_task_ids.clone(),
            cancellation_tokens: self.cancellation_tokens.clone(),
        }
    }
}

#[cfg(test)]
mod auto_wire_lib_tests {
    // Регрессия на реальную интеграцию: A2aClientDriver с wire_format: Auto
    // должен успешно выполнить execute() через зонд, без явного wire в
    // конфиге. Тест не имеет доступа к приватным полям — проверяет только
    // наблюдаемое поведение через публичный invoke().

    use crate::{A2aClientConfig, A2aClientDriver, A2aWireFormat};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn auto_wire_format_resolves_and_completes_invoke() {
        let server = MockServer::start().await;

        // Зонд (GetTask) -> "task not found" -> SDK распознан.
        // Реальный invoke (SendMessage) -> Completed.
        Mock::given(method("POST"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let method_name = body
                    .get("method")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                if method_name == "GetTask" {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32001, "message": "task not found" }
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {
                            "task": {
                                "id": "task-auto-1",
                                "status": { "state": "TASK_STATE_COMPLETED" },
                                "artifacts": [{ "parts": [{ "text": "auto-detected ok" }] }]
                            }
                        }
                    }))
                }
            })
            .mount(&server)
            .await;

        let driver = A2aClientDriver::new(A2aClientConfig {
            endpoint: server.uri(),
            token: None,
            wire_format: A2aWireFormat::Auto,
            timeout_secs: 10,
            agent_card_url: None,
        })
        .expect("driver builds even with Auto and no wire resolved yet");

        let task = driver
            .invoke("hello", None, None)
            .await
            .expect("invoke must resolve wire via probe and then complete");
        assert_eq!(task.id, "task-auto-1");
    }
}

#[cfg(test)]
mod d3_and_d4_integration_tests {
    use crate::{A2aClientConfig, A2aClientDriver, A2aWireFormat};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// D3: закэшированный (ошибочно выбранный на первом резолве) wire не
    /// должен навечно ломать драйвер — при MethodNotFound на реальном
    /// вызове происходит once-off повторная попытка. Зонд сначала
    /// "обманут" (первый GetTask отвечает "task not found" -> SDK
    /// распознан), но реальный SendMessage на SDK-путь падает с
    /// MethodNotFound, а spec-путь работает. При повторном резолве зонд
    /// честно отвечает "method_not_found: GetTask" -> падает на spec ->
    /// message/send завершается успехом.
    #[tokio::test]
    async fn auto_wire_recovers_once_from_wrong_initial_guess() {
        let server = MockServer::start().await;
        let sdk_probe_calls = Arc::new(AtomicUsize::new(0));

        Mock::given(method("POST"))
            .respond_with({
                let calls = sdk_probe_calls.clone();
                move |req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let m = body
                    .get("method")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                match m {
                    "GetTask" => {
                        let n = calls.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            // Первый зонд-вызов: SDK "подходит" (метод понят,
                            // задачи нет — task not found). Это обман: реальный
                            // SendMessage ниже вернёт MethodNotFound.
                            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                                "jsonrpc": "2.0", "id": 1,
                                "error": { "code": -32001, "message": "task not found" }
                            }))
                        } else {
                            // Повторный зонд после D3-retry: теперь честно
                            // сообщаем, что GetTask не распознан -> fallback на spec.
                            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                                "jsonrpc": "2.0", "id": 1,
                                "error": { "code": -32000, "message": "method_not_found: GetTask" }
                            }))
                        }
                    }
                    "SendMessage" => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32601, "message": "Method not found" }
                    })),
                    "tasks/get" => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32001, "message": "task not found" }
                    })),
                    "message/send" => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {
                            "id": "task-recovered",
                            "status": { "state": "completed" },
                            "artifacts": [{ "parts": [{ "kind": "text", "text": "recovered via spec" }] }]
                        }
                    })),
                    _ => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32000, "message": "method_not_found: unexpected" }
                    })),
                }
            }
            })
            .mount(&server)
            .await;

        let driver = A2aClientDriver::new(A2aClientConfig {
            endpoint: server.uri(),
            token: None,
            wire_format: A2aWireFormat::Auto,
            timeout_secs: 10,
            agent_card_url: None,
        })
        .expect("driver builds");

        let task = driver
            .invoke("hello", None, None)
            .await
            .expect("must recover once and complete via spec after wrong initial guess");
        assert_eq!(task.id, "task-recovered");
        assert_eq!(sdk_probe_calls.load(Ordering::SeqCst), 2);
    }

    /// D2/D4: agent_card_url задан и указывает на недоступный сервер —
    /// resolve_auto_wire() должен не считать это фатальной ошибкой и
    /// успешно упасть на зонд. detect_from_agent_card (D2) всегда
    /// возвращает None, поэтому ошибка НЕ может прийти из карточки —
    /// проверяем, что ошибка резолюции не утекла из card_url.
    #[tokio::test]
    async fn resolve_auto_wire_falls_through_to_probe_when_agent_card_configured_but_unreachable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let driver = A2aClientDriver::new(A2aClientConfig {
            endpoint: server.uri(),
            token: None,
            wire_format: A2aWireFormat::Auto,
            timeout_secs: 10,
            agent_card_url: Some("http://127.0.0.1:1/.well-known/agent.json".to_string()),
        })
        .expect("driver builds");

        let result = driver.get_task("probe-check").await;
        match result {
            Ok(_) => {}
            Err(e) => assert!(
                !e.to_string().contains("127.0.0.1:1"),
                "error must not leak the unreachable agent_card_url — resolution should have fallen through to probe: {e}"
            ),
        }
    }
}
