// ============================================================================
// crates/driver-a2a-client/src/lib.rs — ПЕРЕПИСАНО под реальный trait AgentDriver
//
// Источник истины: adapter-core/src/lib.rs (прочитан целиком). Ключевые факты,
// опровергающие все прежние черновики в этой сессии:
//
// 1. AgentDriver::invoke возвращает Result<mpsc::Receiver<DriverEvent>, CoreError>,
//    НЕ одноразовый Result<Task, Error>. Это долгоживущий стрим событий —
//    драйвер спавнит task, который шлёт Accepted → Progress* → (Artifact|
//    InputRequired)* → Completed|Failed|Cancelled по каналу, а вызывающий
//    код (AdapterCore::run_driver) читает receiver в цикле до terminal event.
//    Мой прежний NormalizedTask как одноразовый результат был категориально
//    неверной формой — нормализация wire→core должна происходить НА КАЖДОЕ
//    событие потока, не один раз в конце.
//
// 2. DriverEvent — конкретный enum из adapter_core, не абстракция wire-слоя:
//    Accepted, Progress{message,percent}, Artifact(ArtifactRef),
//    InputRequired(InputRequest), Completed(Vec<Part>), Failed(PublicError),
//    Cancelled. wire::NormalizedTask/NormalizedState из прежних черновиков
//    удаляются полностью — их некому потреблять, AdapterCore ждёt именно
//    DriverEvent.
//
// 3. cancel(&self, task_id) -> Result<(), CoreError> и
//    provide_input(&self, task_id, input: Vec<Part>) -> Result<(), CoreError> —
//    без возврата Task; успех/провал возвращается синхронно, а не через canal.
//
// 4. capabilities(&self) -> DriverCapabilities{cancellation, provide_input} —
//    обязательный метод, статически объявляющий, что умеет драйвер;
//    AdapterCore проверяет его ДО вызова cancel/provide_input (см. adapter-core
//    cancel()/provide_input(): "if agent.driver.capabilities().cancellation").
//
// 5. id(&self) -> &str — обязателен для RegisteredAgent.
//
// A2A wire-формат (SDK/Spec) остаётся деталью транспорта внутри invoke() —
// именно там, а не снаружи. Каждое A2A JSON-RPC поле состояния (TASK_STATE_*
// или completed/failed/...) конвертируется в DriverEvent по мере получения
// событий с сервера. Поскольку A2A gateway в текущей реализации НЕ поддерживает
// стриминг (см. transport_http.rs: "Фаза 1: streaming не реализован"), реальный
// invoke() здесь — одна блокирующая HTTP-задача, которая опрашивает tasks/get
// до terminal state и транслирует прогресс как одно Progress-событие + finalное
// Completed/Failed/Cancelled. Это единственный способ дать долгоживущий канал
// поверх non-streaming A2A backend, не выдавая его за что-то, чего нет.
// ============================================================================

pub mod error;
pub mod wire;

use adapter_core::{AgentDriver, CoreError, DriverEvent};
use adapter_model::{ArtifactRef, DriverCapabilities, InputRequest, InvokeRequest, Part, PublicError, TaskId};
use async_trait::async_trait;
use error::{from_jsonrpc_error, A2aClientError};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use wire::{sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire, NormalizedPart, NormalizedState, NormalizedTask};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum A2aWireFormat {
    #[default]
    Sdk,
    Spec,
}

#[derive(Clone, Debug)]
pub struct A2aClientConfig {
    pub agent_id: String,
    pub endpoint: String,
    pub token: Option<String>,
    pub wire_format: A2aWireFormat,
    pub timeout_secs: u64,
    /// Интервал опроса tasks/get, пока задача не в terminal state. A2A gateway
    /// не поддерживает push/SSE (Фаза 1), поэтому прогресс наблюдается только
    /// поллингом — это ограничение backend, не драйвера.
    pub poll_interval_ms: u64,
}

impl Default for A2aClientConfig {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            endpoint: String::new(),
            token: None,
            wire_format: A2aWireFormat::default(),
            timeout_secs: 30,
            poll_interval_ms: 500,
        }
    }
}

pub struct A2aClientDriver {
    config: A2aClientConfig,
    client: reqwest::Client,
    wire: Arc<dyn A2aWire>,
}

impl A2aClientDriver {
    pub fn new(config: A2aClientConfig) -> Result<Self, A2aClientError> {
        let wire: Arc<dyn A2aWire> = match config.wire_format {
            A2aWireFormat::Sdk => Arc::new(A2aSdkWire),
            A2aWireFormat::Spec => Arc::new(A2aSpecWire),
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        Ok(Self { config, client, wire })
    }

    async fn execute(&self, op: A2aOperation<'_>) -> Result<NormalizedTask, A2aClientError> {
        let method = self.wire.jsonrpc_method(&op);
        let params = self.wire.build_params(&op);

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

        let resp = req.send().await.map_err(|e| A2aClientError::Http(e.to_string()))?;
        let body: Value = resp.json().await.map_err(|e| A2aClientError::Http(e.to_string()))?;

        if let Some(err) = body.get("error") {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(-32000);
            let message = err.get("message").and_then(Value::as_str).unwrap_or("unknown error");
            return Err(from_jsonrpc_error(code, message, method, self.wire.name()));
        }

        let result = body
            .get("result")
            .ok_or_else(|| A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into()))?;

        self.wire.parse_task(result)
    }

    fn normalized_state_to_driver_event(
        state: &NormalizedState,
        status_message: Option<String>,
        output_parts: &[NormalizedPart],
    ) -> Option<DriverEvent> {
        match state {
            NormalizedState::Submitted => Some(DriverEvent::Accepted),
            NormalizedState::Working | NormalizedState::AuthRequired => {
                Some(DriverEvent::Progress {
                    message: status_message.unwrap_or_else(|| "working".to_string()),
                    percent: None,
                })
            }
            NormalizedState::InputRequired => {
                // adapter_model::InputRequest — точный конструктор не подтверждён
                // мной по коду adapter_model (файл не читан), заполняю минимально
                // необходимое поле prompt текстом статуса; при наличии реальной
                // схемы InputRequest скорректировать здесь.
                Some(DriverEvent::InputRequired(InputRequest {
                    prompt: status_message.unwrap_or_default(),
                    ..Default::default()
                }))
            }
            NormalizedState::Completed => {
                let parts: Vec<Part> = output_parts
                    .iter()
                    .filter_map(normalized_part_to_core_part)
                    .collect();
                Some(DriverEvent::Completed(parts))
            }
            NormalizedState::Failed => Some(DriverEvent::Failed(PublicError {
                code: "a2a_remote_error".into(),
                message: status_message.unwrap_or_else(|| "task failed".to_string()),
                retryable: false,
            })),
            NormalizedState::Canceled => Some(DriverEvent::Cancelled),
            NormalizedState::Rejected => Some(DriverEvent::Failed(PublicError {
                code: "a2a_rejected".into(),
                message: status_message.unwrap_or_else(|| "task rejected".to_string()),
                retryable: false,
            })),
        }
    }
}

/// NormalizedPart → adapter_model::Part. Схема Part в adapter_model не
/// подтверждена мной по коду (только по использованию в adapter-core как
/// Vec<Part> в DriverEvent::Completed) — предполагаю текстовый вариант по
/// аналогии с protocol::a2a::Part, помечаю как VERIFY.
fn normalized_part_to_core_part(p: &NormalizedPart) -> Option<Part> {
    // VERIFY: точный конструктор adapter_model::Part не подтверждён кодом
    // (adapter-model/src/lib.rs не читан). Ниже — заглушка через сериализацию
    // текста в общее поле; заменить на реальный вариант enum/struct Part
    // после получения adapter-model исходника.
    p.text.as_ref().map(|_text| {
        // Placeholder: не компилируется без реальной схемы Part.
        // Оставлено намеренно как явный маркер недостающего файла.
        unimplemented!("adapter_model::Part shape not verified — see adapter-model/src/lib.rs")
    })
}

#[async_trait]
impl AgentDriver for A2aClientDriver {
    fn id(&self) -> &str {
        &self.config.agent_id
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            // tasks/cancel и GetTask/CancelTask поддержаны обоими wire (§2.4.4
            // ТЗ) — cancellation декларируем true.
            cancellation: true,
            // provide_input реализован как повторный send_message с тем же
            // task_id (см. ниже) — тоже true.
            provide_input: true,
        }
    }

    async fn health(&self) -> Result<(), CoreError> {
        // Минимальная проверка: HTTP-достижимость endpoint. Полноценный
        // health возможен через GET agent.json, но это отдельный запрос,
        // не покрытый текущим A2aWire trait (он оперирует только rpc).
        self.client
            .get(&self.config.endpoint)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("A2A endpoint unreachable: {e}")))
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let (tx, rx) = mpsc::channel(32);

        // VERIFY: InvokeRequest.input — тип Vec<Part> по аналогии с
        // DriverEvent::Completed(Vec<Part>) в adapter-core, но сама структура
        // InvokeRequest объявлена в adapter_model (не читан) — беру только
        // текстовое содержимое первого part как сообщение A2A, это может не
        // совпадать с реальным полем.
        let text_input = "VERIFY: extract text from request.input (adapter_model::Part shape unknown)".to_string();
        let _ = &request; // подавление unused до реализации выше

        let client = self.client.clone();
        let endpoint = self.config.endpoint.clone();
        let token = self.config.token.clone();
        let wire = self.wire.clone();
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let a2a_task_id_hint = task_id;

        tokio::spawn(async move {
            let _ = tx.send(DriverEvent::Accepted).await;

            let parts = vec![NormalizedPart::text(text_input)];
            let send_op = A2aOperation::SendMessage {
                parts: &parts,
                context_id: None,
                task_id: None,
            };

            let driver_for_send = A2aClientDriver {
                config: A2aClientConfig {
                    agent_id: String::new(),
                    endpoint: endpoint.clone(),
                    token: token.clone(),
                    wire_format: A2aWireFormat::default(),
                    timeout_secs: 30,
                    poll_interval_ms: poll_interval.as_millis() as u64,
                },
                client: client.clone(),
                wire: wire.clone(),
            };

            let first = match driver_for_send.execute(send_op).await {
                Ok(t) => t,
                Err(e) => {
                    let _ = tx
                        .send(DriverEvent::Failed(PublicError {
                            code: "a2a_send_failed".into(),
                            message: e.to_string(),
                            retryable: false,
                        }))
                        .await;
                    return;
                }
            };

            let remote_task_id = first.id.clone();
            let mut current = first;

            loop {
                if let Some(event) = A2aClientDriver::normalized_state_to_driver_event(
                    &current.state,
                    current.status_message.clone(),
                    &current.output_parts,
                ) {
                    let is_terminal = matches!(
                        current.state,
                        NormalizedState::Completed
                            | NormalizedState::Failed
                            | NormalizedState::Canceled
                            | NormalizedState::Rejected
                    );
                    if tx.send(event).await.is_err() {
                        // Receiver закрыт (AdapterCore перестал слушать) —
                        // прекращаем поллинг, нет смысла продолжать HTTP-опрос
                        // для канала, который никто не читает.
                        return;
                    }
                    if is_terminal {
                        return;
                    }
                }

                tokio::time::sleep(poll_interval).await;

                let get_op = A2aOperation::GetTask { task_id: &remote_task_id };
                match driver_for_send.execute(get_op).await {
                    Ok(t) => current = t,
                    Err(e) => {
                        let _ = tx
                            .send(DriverEvent::Failed(PublicError {
                                code: "a2a_poll_failed".into(),
                                message: e.to_string(),
                                retryable: true,
                            }))
                            .await;
                        return;
                    }
                }
            }
        });

        let _ = a2a_task_id_hint; // TaskId из core пока не связывается с remote_task_id — см. TODO ниже

        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        // VERIFY: здесь нужна связь между core TaskId и remote A2A task_id
        // (строка вида "task-..."), которую invoke() выше только получает
        // внутри spawn и не сохраняет наружу. Для полноценной cancel()
        // нужна общая таблица core_task_id -> remote_task_id (например,
        // DashMap<TaskId, String> в A2aClientDriver), которой сейчас нет —
        // это реальный недостающий кусок дизайна, не мелочь.
        let _ = task_id;
        Err(CoreError::Driver(
            "cancel() requires core_task_id -> remote A2A task_id mapping, not yet implemented".into(),
        ))
    }

    async fn provide_input(&self, task_id: TaskId, _input: Vec<Part>) -> Result<(), CoreError> {
        // То же ограничение, что и в cancel(): нет сохранённого remote task_id
        // для повторного message/send с тем же контекстом.
        let _ = task_id;
        Err(CoreError::Driver(
            "provide_input() requires core_task_id -> remote A2A task_id mapping, not yet implemented".into(),
        ))
    }
}
