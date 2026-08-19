//! `driver-http-sse` — generic UAIC/1 driver for remote agents.
//!
//! HTTP/SSE is functionally bidirectional:
//!   Adapter -> POST commands (invoke/cancel/provide_input)
//!   Agent   -> SSE task events
//!
//! This driver is transport/framework/language-neutral. The remote service only
//! needs to implement UAIC JSON endpoints and SSE event frames.
//!
//! Cargo.toml dependencies:
//! async-trait = "0.1"
//! chrono = { version = "0.4", features = ["serde"] }
//! futures-util = "0.3"
//! reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! tokio = { version = "1", features = ["sync", "time"] }
//! tokio-util = "0.7"
//! url = "2"
//! uuid = { version = "1", features = ["v4", "serde"] }
//!
//! Expected public types from `adapter_core`:
//! AgentDriver, ArtifactRef, CoreError, DriverCapabilities, DriverEvent,
//! InputRequest, InvokeRequest, Part, PublicError, TaskId.

use std::{sync::Arc, time::Duration};

use adapter_core::{
    AgentDriver, ArtifactRef, CoreError, DriverCapabilities, DriverEvent,
    InputRequest, InvokeRequest, Part, PublicError, TaskId,
};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, time};

const UAIC_PROTOCOL: &str = "uaic/1";

#[derive(Clone, Debug)]
pub enum Credential {
    None,
    Bearer(String),
}

#[derive(Clone, Debug)]
pub struct HttpSseDriverConfig {
    pub id: String,
    pub base_url: Url,
    pub credential: Credential,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub first_event_timeout: Duration,
    pub idle_stream_timeout: Duration,
    pub max_event_bytes: usize,
    pub reconnect_initial_backoff: Duration,
    pub reconnect_max_backoff: Duration,
    pub reconnect_attempts: usize,
    pub require_https: bool,
}

impl HttpSseDriverConfig {
    pub fn new(base_url: Url) -> Self {
        Self {
            id: "http-sse-agent".into(),
            base_url,
            credential: Credential::None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(15),
            first_event_timeout: Duration::from_secs(30),
            idle_stream_timeout: Duration::from_secs(60),
            max_event_bytes: 256 * 1024,
            reconnect_initial_backoff: Duration::from_millis(250),
            reconnect_max_backoff: Duration::from_secs(30),
            reconnect_attempts: 8,
            require_https: true,
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
struct InvokeBody {
    protocol: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    task_id: TaskId,
    idempotency_key: String,
    session_id: Option<uuid::Uuid>,
    input: Vec<UaicPart>,
    context: serde_json::Value,
    deadline_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct InputBody {
    protocol: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    task_id: TaskId,
    input: Vec<UaicPart>,
}

#[derive(Debug, Serialize)]
struct CancelBody {
    protocol: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    task_id: TaskId,
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
struct UaicEvent {
    protocol: String,
    #[serde(rename = "type")]
    message_type: String,
    task_id: TaskId,
    #[serde(default)]
    seq: Option<u64>,
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

pub struct HttpSseDriver {
    config: HttpSseDriverConfig,
    client: Client,
    manifest: tokio::sync::RwLock<Option<UaicManifest>>,
}

impl HttpSseDriver {
    pub fn new(config: HttpSseDriverConfig) -> Result<Self, CoreError> {
        if config.require_https && config.base_url.scheme() != "https" {
            return Err(CoreError::InvalidRequest("remote HTTP/SSE driver requires https".into()));
        }
        let client = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| CoreError::Driver(format!("cannot build HTTP client: {error}")))?;
        Ok(Self { config, client, manifest: tokio::sync::RwLock::new(None) })
    }

    fn endpoint(&self, suffix: &str) -> Result<Url, CoreError> {
        self.config.base_url.join(suffix)
            .map_err(|error| CoreError::Driver(format!("invalid endpoint path: {error}")))
    }

    fn authenticated(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.config.credential {
            Credential::None => request,
            Credential::Bearer(token) => request.bearer_auth(token),
        }
    }

    async fn load_manifest(&self) -> Result<UaicManifest, CoreError> {
        if let Some(manifest) = self.manifest.read().await.clone() {
            return Ok(manifest);
        }
        let endpoint = self.endpoint("v1/uaic/manifest")?;
        let response = self.authenticated(self.client.get(endpoint))
            .header("X-UAIC-Version", "1")
            .send().await
            .map_err(http_error)?;
        ensure_success(response.status(), "manifest")?;
        let manifest = response.json::<UaicManifest>().await
            .map_err(|error| CoreError::Driver(format!("invalid UAIC manifest: {error}")))?;
        if manifest.protocol != UAIC_PROTOCOL {
            return Err(CoreError::Driver(format!("unsupported UAIC protocol: {}", manifest.protocol)));
        }
        *self.manifest.write().await = Some(manifest.clone());
        Ok(manifest)
    }

    async fn stream_events(
        &self,
        task_id: TaskId,
        tx: mpsc::Sender<DriverEvent>,
    ) {
        let mut after_seq = 0u64;
        let mut attempts = 0usize;
        let mut backoff = self.config.reconnect_initial_backoff;
        loop {
            let result = self.read_sse(task_id, after_seq, &tx).await;
            match result {
                Ok(StreamOutcome::Terminal) => return,
                Ok(StreamOutcome::Disconnected(last_seq)) => {
                    after_seq = last_seq;
                    attempts += 1;
                }
                Err(error) => {
                    attempts += 1;
                    if attempts > self.config.reconnect_attempts {
                        let _ = tx.send(DriverEvent::Failed(PublicError {
                            code: "stream_unavailable".into(),
                            message: error.to_string(),
                            retryable: true,
                        })).await;
                        return;
                    }
                }
            }
            time::sleep(backoff).await;
            backoff = std::cmp::min(backoff.saturating_mul(2), self.config.reconnect_max_backoff);
        }
    }

    async fn read_sse(
        &self,
        task_id: TaskId,
        after_seq: u64,
        tx: &mpsc::Sender<DriverEvent>,
    ) -> Result<StreamOutcome, CoreError> {
        let mut endpoint = self.endpoint(&format!("v1/uaic/tasks/{task_id}/events"))?;
        endpoint.query_pairs_mut().append_pair("after_seq", &after_seq.to_string());
        let mut request = self.authenticated(self.client.get(endpoint))
            .header(header::ACCEPT, "text/event-stream")
            .header("X-UAIC-Version", "1");
        if after_seq > 0 {
            request = request.header("Last-Event-ID", after_seq.to_string());
        }
        let response = request.send().await.map_err(http_error)?;
        ensure_success(response.status(), "SSE stream")?;
        let mut bytes = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut latest_seq = after_seq;
        let mut first = true;

        loop {
            let next = if first {
                first = false;
                time::timeout(self.config.first_event_timeout, bytes.next()).await
                    .map_err(|_| CoreError::Driver("first SSE event timeout".into()))?
            } else {
                time::timeout(self.config.idle_stream_timeout, bytes.next()).await
                    .map_err(|_| CoreError::Driver("idle SSE stream timeout".into()))?
            };
            let Some(chunk) = next else { return Ok(StreamOutcome::Disconnected(latest_seq)); };
            let chunk = chunk.map_err(|error| CoreError::Driver(format!("SSE read failure: {error}")))?;
            buffer.extend_from_slice(&chunk);
            if buffer.len() > self.config.max_event_bytes {
                return Err(CoreError::Driver("SSE event exceeds max_event_bytes".into()));
            }
            while let Some(boundary) = find_sse_boundary(&buffer) {
                let frame = buffer.drain(..boundary).collect::<Vec<_>>();
                drain_boundary(&mut buffer);
                let Some((id, data)) = parse_sse_frame(&frame) else { continue; };
                let event: UaicEvent = serde_json::from_slice(&data)
                    .map_err(|error| CoreError::Driver(format!("invalid UAIC SSE event: {error}")))?;
                if event.protocol != UAIC_PROTOCOL || event.task_id != task_id {
                    continue;
                }
                if let Some(seq) = event.seq.or(id.and_then(|value| value.parse().ok())) {
                    if seq <= latest_seq { continue; }
                    latest_seq = seq;
                }
                let terminal = map_uaic_event(event, tx).await?;
                if terminal { return Ok(StreamOutcome::Terminal); }
            }
        }
    }
}

#[async_trait]
impl AgentDriver for HttpSseDriver {
    fn id(&self) -> &str { &self.config.id }

    fn capabilities(&self) -> DriverCapabilities {
        // Exact manifest capabilities are checked by cancel/provide_input.
        DriverCapabilities { cancellation: true, provide_input: true }
    }

    async fn health(&self) -> Result<(), CoreError> {
        let endpoint = self.endpoint("v1/uaic/health")?;
        let response = self.authenticated(self.client.get(endpoint)).send().await.map_err(http_error)?;
        ensure_success(response.status(), "health")?;
        self.load_manifest().await?;
        Ok(())
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        self.load_manifest().await?;
        let endpoint = self.endpoint("v1/uaic/tasks")?;
        let body = InvokeBody {
            protocol: UAIC_PROTOCOL,
            message_type: "invoke",
            task_id,
            idempotency_key: request.idempotency_key.clone(),
            session_id: request.session_id,
            input: request.input.into_iter().map(Into::into).collect(),
            context: request.context,
            deadline_ms: request.deadline.map(|duration| duration.as_millis() as u64),
        };
        let response = self.authenticated(self.client.post(endpoint))
            .header("X-UAIC-Version", "1")
            .header("Idempotency-Key", request.idempotency_key)
            .json(&body)
            .send().await.map_err(http_error)?;
        if response.status() != StatusCode::ACCEPTED && response.status() != StatusCode::OK {
            ensure_success(response.status(), "invoke")?;
        }

        let (tx, rx) = mpsc::channel(128);
        let driver = self.clone_for_task();
        tokio::spawn(async move { driver.stream_events(task_id, tx).await; });
        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        let manifest = self.load_manifest().await?;
        if !manifest.capabilities.cancellation {
            return Err(CoreError::InvalidRequest("agent does not support cancellation".into()));
        }
        let endpoint = self.endpoint(&format!("v1/uaic/tasks/{task_id}/cancel"))?;
        let response = self.authenticated(self.client.post(endpoint))
            .header("X-UAIC-Version", "1")
            .json(&CancelBody { protocol: UAIC_PROTOCOL, message_type: "cancel", task_id })
            .send().await.map_err(http_error)?;
        ensure_success(response.status(), "cancel")
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError> {
        let manifest = self.load_manifest().await?;
        if !manifest.capabilities.provide_input {
            return Err(CoreError::InvalidRequest("agent does not support provide_input".into()));
        }
        let endpoint = self.endpoint(&format!("v1/uaic/tasks/{task_id}/input"))?;
        let response = self.authenticated(self.client.post(endpoint))
            .header("X-UAIC-Version", "1")
            .json(&InputBody {
                protocol: UAIC_PROTOCOL,
                message_type: "provide_input",
                task_id,
                input: input.into_iter().map(Into::into).collect(),
            })
            .send().await.map_err(http_error)?;
        ensure_success(response.status(), "provide_input")
    }
}

impl HttpSseDriver {
    fn clone_for_task(&self) -> Self {
        Self {
            config: self.config.clone(),
            client: self.client.clone(),
            manifest: tokio::sync::RwLock::new(None),
        }
    }
}

enum StreamOutcome { Terminal, Disconnected(u64) }

async fn map_uaic_event(event: UaicEvent, tx: &mpsc::Sender<DriverEvent>) -> Result<bool, CoreError> {
    let mapped = match event.message_type.as_str() {
        "accepted" => DriverEvent::Accepted,
        "progress" => DriverEvent::Progress { message: event.message.unwrap_or_default(), percent: event.percent },
        "artifact" => DriverEvent::Artifact(event.artifact.ok_or_else(|| CoreError::Driver("artifact event without artifact".into()))?),
        "input_required" => DriverEvent::InputRequired(event.request.ok_or_else(|| CoreError::Driver("input_required without request".into()))?),
        "completed" => DriverEvent::Completed(event.output.unwrap_or_default()),
        "failed" => DriverEvent::Failed(event.error.unwrap_or(PublicError {
            code: "agent_failed".into(),
            message: event.message.unwrap_or_else(|| "agent failed".into()),
            retryable: false,
        })),
        "cancelled" => DriverEvent::Cancelled,
        _ => return Ok(false),
    };
    let terminal = matches!(mapped, DriverEvent::Completed(_) | DriverEvent::Failed(_) | DriverEvent::Cancelled);
    tx.send(mapped).await.map_err(|_| CoreError::Driver("task consumer closed".into()))?;
    Ok(terminal)
}

fn http_error(error: reqwest::Error) -> CoreError {
    CoreError::Driver(format!("HTTP transport failure: {error}"))
}

fn ensure_success(status: StatusCode, operation: &str) -> Result<(), CoreError> {
    if status.is_success() { return Ok(()); }
    let code = if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        "authorization"
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        "rate_limited"
    } else if status.is_server_error() {
        "remote_server_error"
    } else {
        "remote_request_error"
    };
    Err(CoreError::Driver(format!("{operation}: {code} ({status})")))
}

fn find_sse_boundary(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|pair| pair == b"\n\n")
        .or_else(|| buffer.windows(4).position(|quad| quad == b"\r\n\r\n"))
}

fn drain_boundary(buffer: &mut Vec<u8>) {
    if buffer.starts_with(b"\n\n") { buffer.drain(..2); }
    else if buffer.starts_with(b"\r\n\r\n") { buffer.drain(..4); }
}

fn parse_sse_frame(frame: &[u8]) -> Option<(Option<String>, Vec<u8>)> {
    let text = std::str::from_utf8(frame).ok()?;
    let mut id = None;
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            id = Some(value.trim().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() { data.push(b'\n'); }
            data.extend_from_slice(value.trim_start().as_bytes());
        }
    }
    (!data.is_empty()).then_some((id, data))
}
