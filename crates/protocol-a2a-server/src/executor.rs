//! `AdapterAgentExecutor` — bridge между `a2a-server::AgentExecutor`
//! и нашим `AdapterCore`.
//!
//! `a2a-server::DefaultRequestHandler` вызывает `execute(ctx)` и сам публикует
//! результат в bounded broadcast (capacity 32) для SSE. Disconnect клиента не
//! отменяет task: `DefaultRequestHandler` исполняет executor в отдельной
//! `tokio::spawn` (`drive_execution`), поэтому дроп SSE-response не отменяет
//! выполнение.
//!
//! Здесь мы создаём задачу в AdapterCore (task_id = A2A task_id,
//! idempotency_key = A2A task_id) и стримим события Core → `StreamResponse`.
//! Создание задачи (все проверки лимитов/existence) происходит до первого
//! события потока.

use a2a::*;
use a2a_server::{AgentExecutor, ExecutorContext};
use adapter_core::{AdapterCore, Caller, CallerId, CoreCommand, CoreError, InvokeRequest};
use adapter_model::{Part as AdapterPart, TaskId};
use futures_util::{stream::BoxStream, StreamExt};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Executor, прокидывающий A2A вызовы в AdapterCore.
pub struct AdapterAgentExecutor {
    core: Arc<AdapterCore>,
    caller: Caller,
}

impl AdapterAgentExecutor {
    pub fn new(core: Arc<AdapterCore>, caller_id: impl Into<String>) -> Self {
        Self {
            core,
            caller: Caller {
                id: CallerId(caller_id.into()),
                scopes: Vec::new(),
            },
        }
    }

    async fn create_and_subscribe(
        &self,
        ctx: &ExecutorContext,
    ) -> Result<(TaskId, adapter_core::TaskSubscription), A2AError> {
        let task_id = ctx
            .task_id
            .parse::<TaskId>()
            .map_err(|_| A2AError::invalid_request("task_id must be a UUID"))?;
        let session_id = ctx.context_id.parse::<uuid::Uuid>().ok();
        let request = InvokeRequest {
            task_id: Some(task_id),
            agent_id: None,
            skill_id: None,
            idempotency_key: ctx.task_id.clone(),
            session_id,
            input: a2a_message_to_parts(ctx.message.as_ref())?,
            context: ctx
                .metadata
                .clone()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|e| A2AError::internal(format!("invalid metadata: {e}")))?
                .unwrap_or(serde_json::Value::Null),
            deadline: None,
        };
        let result = self
            .core
            .dispatch(self.caller.clone(), CoreCommand::Invoke(request))
            .await
            .map_err(core_error_to_a2a)?;
        let task_id = match result {
            adapter_core::DispatchResult::Created(s)
            | adapter_core::DispatchResult::Existing(s) => s.task_id,
            _ => return Err(A2AError::internal("unexpected core result for invoke")),
        };
        let subscription = self
            .core
            .subscribe(task_id, 0)
            .await
            .map_err(core_error_to_a2a)?;
        Ok((task_id, subscription))
    }
}

#[async_trait::async_trait]
#[allow(clippy::needless_return)]
impl AgentExecutor for AdapterAgentExecutor {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        // Состояние потока: сначала инициализация (создание задачи + подписка),
        // затем история из subscription.history, затем live broadcast.
        enum State {
            Init,
            Ready(adapter_core::TaskSubscription),
            Done,
        }
        let this = self.clone();
        Box::pin(futures_util::stream::unfold(
            (this, State::Init, ctx),
            |(this, state, ctx)| async move {
                let mut state = match state {
                    State::Init => match this.create_and_subscribe(&ctx).await {
                        Ok((_task_id, sub)) => State::Ready(sub),
                        Err(e) => {
                            return Some((Err(e), (this, State::Done, ctx)));
                        }
                    },
                    other => other,
                };
                // Отдать следующий элемент: сначала history, потом live.
                match &mut state {
                    State::Ready(sub) => {
                        if !sub.history.is_empty() {
                            let event = sub.history.remove(0);
                            return Some((
                                Ok(event_to_stream_response(&event)),
                                (this, state, ctx),
                            ));
                        }
                        match sub.receiver.recv().await {
                            Ok(event) => {
                                let item = event_to_stream_response(&event);
                                let terminal = matches!(
                                    event.kind,
                                    adapter_model::CoreEventKind::Completed { .. }
                                        | adapter_model::CoreEventKind::Failed { .. }
                                        | adapter_model::CoreEventKind::Cancelled
                                );
                                if terminal {
                                    state = State::Done;
                                }
                                return Some((Ok(item), (this, state, ctx)));
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                state = State::Done;
                                return Some((
                                    Ok(terminal_stream_response(&ctx)),
                                    (this, state, ctx),
                                ));
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                // gap: клиент должен сделать resume из store.
                                let err = A2AError::internal(
                                    "subscription fell behind task history; resume with a cursor",
                                );
                                state = State::Done;
                                return Some((Err(err), (this, state, ctx)));
                            }
                        }
                    }
                    State::Done | State::Init => return None,
                }
            },
        ))
        .boxed()
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let task_id = match ctx.task_id.parse::<TaskId>() {
            Ok(id) => id,
            Err(_) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(A2AError::invalid_request("task_id must be a UUID"))
                }));
            }
        };
        let core = self.core.clone();
        let caller = self.caller.clone();
        Box::pin(futures_util::stream::once(async move {
            let result = core
                .dispatch(
                    caller,
                    CoreCommand::Cancel {
                        task_id,
                        reason: None,
                    },
                )
                .await
                .map_err(core_error_to_a2a)?;
            Ok(task_snapshot_to_stream(&result))
        }))
    }
}

impl Clone for AdapterAgentExecutor {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            caller: self.caller.clone(),
        }
    }
}

fn a2a_message_to_parts(message: Option<&Message>) -> Result<Vec<AdapterPart>, A2AError> {
    let Some(message) = message else {
        return Ok(Vec::new());
    };
    message
        .parts
        .iter()
        .map(|part| match &part.content {
            PartContent::Text(text) => Ok(AdapterPart::Text { text: text.clone() }),
            PartContent::Raw(bytes) => Ok(AdapterPart::Json {
                value: serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()),
            }),
            PartContent::Url(url) => Ok(AdapterPart::FileRef {
                uri: url.clone(),
                mime_type: part.media_type.clone(),
            }),
            PartContent::Data(value) => Ok(AdapterPart::Json {
                value: value.clone(),
            }),
        })
        .collect()
}

/// Преобразовать CoreEvent в A2A StreamResponse.
fn event_to_stream_response(event: &adapter_core::CoreEvent) -> StreamResponse {
    let task_id = event.task_id.to_string();
    let context_id = event.task_id.to_string();
    match &event.kind {
        adapter_model::CoreEventKind::Accepted { .. } => {
            status_update(task_id, context_id, TaskState::Working, None)
        }
        adapter_model::CoreEventKind::Progress { message, .. } => status_update(
            task_id,
            context_id,
            TaskState::Working,
            Some(message.clone()),
        ),
        adapter_model::CoreEventKind::InputRequired { request } => status_update(
            task_id,
            context_id,
            TaskState::InputRequired,
            Some(request.question.clone()),
        ),
        adapter_model::CoreEventKind::Artifact { artifact } => {
            StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
                task_id: task_id.clone(),
                context_id,
                artifact: Artifact {
                    artifact_id: artifact.id.clone(),
                    name: Some(artifact.name.clone()),
                    description: None,
                    parts: vec![Part {
                        content: artifact
                            .uri
                            .clone()
                            .map(PartContent::Url)
                            .unwrap_or(PartContent::Text(String::new())),
                        filename: None,
                        media_type: Some(artifact.mime_type.clone()),
                        metadata: None,
                    }],
                    metadata: None,
                    extensions: None,
                },
                append: None,
                last_chunk: None,
                metadata: None,
            })
        }
        adapter_model::CoreEventKind::Completed { output } => {
            let message = Message::new(
                Role::Agent,
                output.iter().map(adapter_part_to_a2a).collect(),
            );
            task_completed(task_id, context_id, Some(message))
        }
        adapter_model::CoreEventKind::Failed { error } => task_failed(
            task_id,
            context_id,
            Some(Message::new(
                Role::Agent,
                vec![Part::text(error.message.clone())],
            )),
        ),
        adapter_model::CoreEventKind::CancelRequested { .. } => status_update(
            task_id,
            context_id,
            TaskState::Canceled,
            Some("cancellation requested".into()),
        ),
        adapter_model::CoreEventKind::Cancelled => task_canceled(task_id, context_id),
    }
}

fn status_update(
    task_id: String,
    context_id: String,
    state: TaskState,
    message: Option<String>,
) -> StreamResponse {
    StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
        task_id,
        context_id,
        status: TaskStatus {
            state,
            message: message.map(|text| Message::new(Role::Agent, vec![Part::text(text)])),
            timestamp: Some(chrono::Utc::now()),
        },
        metadata: None,
    })
}

fn task_completed(task_id: String, context_id: String, message: Option<Message>) -> StreamResponse {
    StreamResponse::Task(Task {
        id: task_id,
        context_id,
        status: TaskStatus {
            state: TaskState::Completed,
            message,
            timestamp: Some(chrono::Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: None,
    })
}

fn task_failed(task_id: String, context_id: String, message: Option<Message>) -> StreamResponse {
    StreamResponse::Task(Task {
        id: task_id,
        context_id,
        status: TaskStatus {
            state: TaskState::Failed,
            message,
            timestamp: Some(chrono::Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: None,
    })
}

fn task_canceled(task_id: String, context_id: String) -> StreamResponse {
    StreamResponse::Task(Task {
        id: task_id,
        context_id,
        status: TaskStatus {
            state: TaskState::Canceled,
            message: None,
            timestamp: Some(chrono::Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: None,
    })
}

fn terminal_stream_response(ctx: &ExecutorContext) -> StreamResponse {
    StreamResponse::Task(Task {
        id: ctx.task_id.clone(),
        context_id: ctx.context_id.clone(),
        status: TaskStatus {
            state: TaskState::Canceled,
            message: None,
            timestamp: Some(chrono::Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: None,
    })
}

fn adapter_part_to_a2a(part: &AdapterPart) -> Part {
    match part {
        AdapterPart::Text { text } => Part::text(text.clone()),
        AdapterPart::Json { value } => Part::data(value.clone()),
        AdapterPart::FileRef { uri, mime_type } => {
            Part::url(uri.clone()).with_media_type(mime_type.clone().unwrap_or_default())
        }
    }
}

fn task_snapshot_to_stream(result: &adapter_core::DispatchResult) -> StreamResponse {
    let snapshot = match result {
        adapter_core::DispatchResult::Created(s)
        | adapter_core::DispatchResult::Existing(s)
        | adapter_core::DispatchResult::Status(s)
        | adapter_core::DispatchResult::CancelRequested(s)
        | adapter_core::DispatchResult::InputAccepted(s) => s,
    };
    StreamResponse::Task(Task {
        id: snapshot.task_id.to_string(),
        context_id: snapshot
            .session_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| snapshot.task_id.to_string()),
        status: TaskStatus {
            state: map_task_state(snapshot.state),
            message: None,
            timestamp: Some(snapshot.updated_at),
        },
        artifacts: None,
        history: None,
        metadata: None,
    })
}

fn map_task_state(state: adapter_model::TaskState) -> TaskState {
    use adapter_model::TaskState as S;
    match state {
        S::Created | S::Accepted | S::Running => TaskState::Working,
        S::WaitingForInput => TaskState::InputRequired,
        S::CancelRequested | S::Cancelled => TaskState::Canceled,
        S::Completed => TaskState::Completed,
        S::Failed => TaskState::Failed,
    }
}

fn core_error_to_a2a(error: CoreError) -> A2AError {
    match error {
        CoreError::InvalidRequest(msg) => A2AError::invalid_request(msg),
        CoreError::AgentNotFound(id) => A2AError::invalid_request(format!("agent not found: {id}")),
        CoreError::NoEligibleAgent => A2AError::invalid_request("no eligible agent"),
        CoreError::ResourceExhausted(msg) => A2AError::internal(msg),
        CoreError::Driver(msg) => A2AError::internal(msg),
        CoreError::Timeout => A2AError::internal("task timed out"),
        CoreError::Store(e) => A2AError::internal(e.to_string()),
    }
}
