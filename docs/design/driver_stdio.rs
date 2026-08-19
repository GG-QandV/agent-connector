//! `driver-stdio` — generic UAIC/1 NDJSON driver for local agents.
//!
//! This driver is framework- and language-neutral. It starts any executable,
//! writes UAIC commands as one UTF-8 JSON object per stdin line, and reads UAIC
//! events as one UTF-8 JSON object per stdout line.
//!
//! Expected workspace dependencies:
//! async-trait = "0.1"
//! chrono = { version = "0.4", features = ["serde"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! thiserror = "2"
//! tokio = { version = "1", features = ["io-util", "process", "sync", "time"] }
//! tokio-util = "0.7"
//! uuid = { version = "1", features = ["serde"] }
//!
//! This file expects the public types from the generated `adapter_core` crate:
//! AgentDriver, CoreError, DriverCapabilities, DriverEvent, InvokeRequest,
//! Part, ArtifactRef, InputRequest, PublicError, TaskId.

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use adapter_core::{
    AgentDriver, ArtifactRef, CoreError, DriverCapabilities, DriverEvent,
    InputRequest, InvokeRequest, Part, PublicError, TaskId,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{mpsc, oneshot, Mutex},
    time,
};
use uuid::Uuid;

const UAIC_PROTOCOL: &str = "uaic/1";

#[derive(Clone, Debug)]
pub struct StdioDriverConfig {
    pub id: String,
    pub command: PathBuf,
    pub args: Vec<String>,
    pub working_dir: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub startup_timeout: Duration,
    pub command_timeout: Duration,
    pub max_line_bytes: usize,
    pub restart_on_crash: bool,
}

impl Default for StdioDriverConfig {
    fn default() -> Self {
        Self {
            id: "stdio-agent".into(),
            command: PathBuf::from("./agent"),
            args: Vec::new(),
            working_dir: None,
            env: HashMap::new(),
            startup_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_secs(15),
            max_line_bytes: 1024 * 1024,
            restart_on_crash: true,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UaicCapabilities {
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub cancellation: bool,
    #[serde(default)]
    pub provide_input: bool,
    #[serde(default)]
    pub status: bool,
    #[serde(default)]
    pub resume: bool,
    #[serde(default)]
    pub artifacts: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UaicManifest {
    pub protocol: String,
    pub agent: UaicAgentIdentity,
    #[serde(default)]
    pub capabilities: UaicCapabilities,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UaicAgentIdentity {
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Serialize)]
struct Envelope<T: Serialize> {
    protocol: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    message_id: Uuid,
    #[serde(flatten)]
    payload: T,
}

#[derive(Debug, Serialize)]
struct InitializePayload {}

#[derive(Debug, Serialize)]
struct HealthPayload {}

#[derive(Debug, Serialize)]
struct InvokePayload {
    task_id: TaskId,
    idempotency_key: String,
    session_id: Option<Uuid>,
    input: Vec<UaicPart>,
    context: serde_json::Value,
    deadline_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct CancelPayload {
    task_id: TaskId,
}

#[derive(Debug, Serialize)]
struct ProvideInputPayload {
    task_id: TaskId,
    input: Vec<UaicPart>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UaicPart {
    Text { text: String },
    Json { value: serde_json::Value },
    FileRef { uri: String, mime_type: Option<String> },
}

impl From<Part> for UaicPart {
    fn from(value: Part) -> Self {
        match value {
            Part::Text { text } => Self::Text { text },
            Part::Json { value } => Self::Json { value },
            Part::FileRef { uri, mime_type } => Self::FileRef { uri, mime_type },
        }
    }
}

#[derive(Debug, Deserialize)]
struct UaicInbound {
    protocol: String,
    #[serde(rename = "type")]
    message_type: String,
    #[serde(default)]
    task_id: Option<TaskId>,
    #[serde(default)]
    message_id: Option<Uuid>,
    #[serde(default)]
    capabilities: Option<UaicCapabilities>,
    #[serde(default)]
    agent: Option<UaicAgentIdentity>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    percent: Option<u8>,
    #[serde(default)]
    artifact: Option<ArtifactRef>,
    #[serde(default)]
    request: Option<InputRequest>,
    #[serde(default)]
    output: Option<Vec<Part>>,
    #[serde(default)]
    error: Option<PublicError>,
}

struct PendingTask {
    events: mpsc::Sender<DriverEvent>,
}

struct ProcessState {
    child: Child,
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Arc<Mutex<HashMap<TaskId, PendingTask>>>,
    manifest: UaicManifest,
}

pub struct StdioDriver {
    config: StdioDriverConfig,
    process: Mutex<Option<ProcessState>>,
}

impl StdioDriver {
    pub fn new(config: StdioDriverConfig) -> Self {
        Self { config, process: Mutex::new(None) }
    }

    async fn ensure_started(&self) -> Result<(), CoreError> {
        let mut guard = self.process.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let state = self.start_process().await?;
        *guard = Some(state);
        Ok(())
    }

    async fn start_process(&self) -> Result<ProcessState, CoreError> {
        let mut command = Command::new(&self.config.command);
        command
            .args(&self.config.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);

        if let Some(dir) = &self.config.working_dir {
            command.current_dir(dir);
        }
        for (key, value) in &self.config.env {
            command.env(key, value);
        }

        let mut child = command.spawn().map_err(|error| {
            CoreError::Driver(format!("cannot start {}: {error}", self.config.command.display()))
        })?;
        let stdin = child.stdin.take().ok_or_else(|| CoreError::Driver("agent stdin unavailable".into()))?;
        let stdout = child.stdout.take().ok_or_else(|| CoreError::Driver("agent stdout unavailable".into()))?;
        let stdin = Arc::new(Mutex::new(stdin));
        let pending = Arc::new(Mutex::new(HashMap::<TaskId, PendingTask>::new()));

        let (initialize_tx, initialize_rx) = oneshot::channel::<UaicManifest>();
        self.spawn_stdout_reader(stdout, pending.clone(), Some(initialize_tx));

        write_command(
            stdin.clone(),
            Envelope {
                protocol: UAIC_PROTOCOL,
                message_type: "initialize",
                message_id: Uuid::new_v4(),
                payload: InitializePayload {},
            },
            self.config.max_line_bytes,
        ).await?;

        let manifest = time::timeout(self.config.startup_timeout, initialize_rx)
            .await
            .map_err(|_| CoreError::Driver("agent initialize timeout".into()))?
            .map_err(|_| CoreError::Driver("agent closed before initialize".into()))?;

        if manifest.protocol != UAIC_PROTOCOL {
            return Err(CoreError::Driver(format!("unsupported UAIC protocol: {}", manifest.protocol)));
        }
        Ok(ProcessState { child, stdin, pending, manifest })
    }

    fn spawn_stdout_reader(
        &self,
        stdout: tokio::process::ChildStdout,
        pending: Arc<Mutex<HashMap<TaskId, PendingTask>>>,
        initialize_tx: Option<oneshot::Sender<UaicManifest>>,
    ) {
        let max_line_bytes = self.config.max_line_bytes;
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            let mut initialize_tx = initialize_tx;
            while let Ok(Some(line)) = reader.next_line().await {
                if line.len() > max_line_bytes {
                    continue;
                }
                let Ok(message) = serde_json::from_str::<UaicInbound>(&line) else {
                    continue;
                };
                if message.protocol != UAIC_PROTOCOL {
                    continue;
                }
                if message.message_type == "initialized" {
                    if let Some(tx) = initialize_tx.take() {
                        let manifest = UaicManifest {
                            protocol: message.protocol,
                            agent: message.agent.unwrap_or(UaicAgentIdentity {
                                name: "unnamed-agent".into(), version: String::new(), description: String::new(),
                            }),
                            capabilities: message.capabilities.unwrap_or_default(),
                        };
                        let _ = tx.send(manifest);
                    }
                    continue;
                }
                let Some(task_id) = message.task_id else { continue; };
                let event = match message.message_type.as_str() {
                    "accepted" => DriverEvent::Accepted,
                    "progress" => DriverEvent::Progress {
                        message: message.message.unwrap_or_default(),
                        percent: message.percent,
                    },
                    "artifact" => match message.artifact {
                        Some(artifact) => DriverEvent::Artifact(artifact),
                        None => continue,
                    },
                    "input_required" => match message.request {
                        Some(request) => DriverEvent::InputRequired(request),
                        None => continue,
                    },
                    "completed" => DriverEvent::Completed(message.output.unwrap_or_default()),
                    "failed" => DriverEvent::Failed(message.error.unwrap_or(PublicError {
                        code: "agent_failed".into(),
                        message: message.message.unwrap_or_else(|| "agent failed".into()),
                        retryable: false,
                    })),
                    "cancelled" => DriverEvent::Cancelled,
                    _ => continue,
                };
                let terminal = matches!(event, DriverEvent::Completed(_) | DriverEvent::Failed(_) | DriverEvent::Cancelled);
                let sender = {
                    let mut tasks = pending.lock().await;
                    if terminal {
                        tasks.remove(&task_id).map(|task| task.events)
                    } else {
                        tasks.get(&task_id).map(|task| task.events.clone())
                    }
                };
                if let Some(sender) = sender {
                    let _ = sender.send(event).await;
                }
            }
            let mut tasks = pending.lock().await;
            for (_, pending_task) in tasks.drain() {
                let _ = pending_task.events.try_send(DriverEvent::Failed(PublicError {
                    code: "agent_process_closed".into(),
                    message: "agent stdout closed".into(),
                    retryable: true,
                }));
            }
        });
    }

    async fn process(&self) -> Result<tokio::sync::MutexGuard<'_, Option<ProcessState>>, CoreError> {
        self.ensure_started().await?;
        Ok(self.process.lock().await)
    }
}

#[async_trait]
impl AgentDriver for StdioDriver {
    fn id(&self) -> &str { &self.config.id }

    fn capabilities(&self) -> DriverCapabilities {
        // The precise values are validated once the subprocess has completed initialize.
        // Conservative defaults are safe before startup.
        DriverCapabilities { cancellation: true, provide_input: true }
    }

    async fn health(&self) -> Result<(), CoreError> {
        let guard = self.process().await?;
        let state = guard.as_ref().ok_or_else(|| CoreError::Driver("agent process unavailable".into()))?;
        if state.child.id().is_none() {
            return Err(CoreError::Driver("agent process exited".into()));
        }
        Ok(())
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let guard = self.process().await?;
        let state = guard.as_ref().ok_or_else(|| CoreError::Driver("agent process unavailable".into()))?;
        let (tx, rx) = mpsc::channel(128);
        state.pending.lock().await.insert(task_id, PendingTask { events: tx });

        let deadline_ms = request.deadline.map(|deadline| deadline.as_millis() as u64);
        let command = Envelope {
            protocol: UAIC_PROTOCOL,
            message_type: "invoke",
            message_id: Uuid::new_v4(),
            payload: InvokePayload {
                task_id,
                idempotency_key: request.idempotency_key,
                session_id: request.session_id,
                input: request.input.into_iter().map(Into::into).collect(),
                context: request.context,
                deadline_ms,
            },
        };
        if let Err(error) = write_command(state.stdin.clone(), command, self.config.max_line_bytes).await {
            state.pending.lock().await.remove(&task_id);
            return Err(error);
        }
        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        let guard = self.process().await?;
        let state = guard.as_ref().ok_or_else(|| CoreError::Driver("agent process unavailable".into()))?;
        if !state.manifest.capabilities.cancellation {
            return Err(CoreError::InvalidRequest("agent does not support cancellation".into()));
        }
        write_command(
            state.stdin.clone(),
            Envelope {
                protocol: UAIC_PROTOCOL,
                message_type: "cancel",
                message_id: Uuid::new_v4(),
                payload: CancelPayload { task_id },
            },
            self.config.max_line_bytes,
        ).await
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError> {
        let guard = self.process().await?;
        let state = guard.as_ref().ok_or_else(|| CoreError::Driver("agent process unavailable".into()))?;
        if !state.manifest.capabilities.provide_input {
            return Err(CoreError::InvalidRequest("agent does not support provide_input".into()));
        }
        write_command(
            state.stdin.clone(),
            Envelope {
                protocol: UAIC_PROTOCOL,
                message_type: "provide_input",
                message_id: Uuid::new_v4(),
                payload: ProvideInputPayload {
                    task_id,
                    input: input.into_iter().map(Into::into).collect(),
                },
            },
            self.config.max_line_bytes,
        ).await
    }
}

async fn write_command<T: Serialize>(
    stdin: Arc<Mutex<ChildStdin>>,
    command: Envelope<T>,
    max_line_bytes: usize,
) -> Result<(), CoreError> {
    let mut encoded = serde_json::to_vec(&command)
        .map_err(|error| CoreError::Driver(format!("cannot encode UAIC command: {error}")))?;
    if encoded.len() > max_line_bytes {
        return Err(CoreError::InvalidRequest("UAIC command exceeds max_line_bytes".into()));
    }
    encoded.push(b'\n');
    let mut stdin = stdin.lock().await;
    stdin.write_all(&encoded).await
        .map_err(|error| CoreError::Driver(format!("cannot write to agent stdin: {error}")))?;
    stdin.flush().await
        .map_err(|error| CoreError::Driver(format!("cannot flush agent stdin: {error}")))
}
