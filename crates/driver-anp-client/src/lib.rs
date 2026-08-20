//! ANP client driver (profile-aware transport boundary).
//!
//! Implements the handoff §5 rules: the driver is only a full
//! [`adapter_core::AgentDriver`] once the `agent-connector.anp-task.v1`
//! profile is negotiated. In `MessagingOnly` every task command returns an
//! explicit `UnsupportedCapability` error.
//!
//! The driver is generic over the transport so tests use
//! [`anp_transport::FakeAnpTransport`]; the production adapter (pinned
//! upstream `anp` SDK) is added separately.

use std::collections::HashMap;
use std::sync::Arc;

use adapter_core::{AgentDriver, CoreError, DriverCapabilities, DriverEvent, InvokeRequest, Part};
use adapter_model::{PublicError, TaskId};
use anp_transport::{
    AnpCapabilities, AnpError, AnpMessage, AnpMessageBody, AnpMessageId, AnpTransport,
    NegotiationStatus, PeerRef, ProfileOffer, VerifiedAnpPeer,
};
use async_trait::async_trait;
use protocol_anp_profile::{
    MessageId, OperationId, ProfileError, TaskCancel, TaskEvent, TaskId as AnpTaskId, TaskInvoke,
    PROFILE_ID,
};
use tokio::sync::{mpsc, RwLock};
use tracing::warn;

use crate::state::{transition, AnpClientState};

mod state;

/// Driver errors.
#[derive(Debug, thiserror::Error)]
pub enum AnpClientError {
    #[error("ANP transport error: {0}")]
    Transport(#[from] AnpError),
    #[error("profile error: {0}")]
    Profile(#[from] ProfileError),
    #[error("task command requires `{PROFILE_ID}` profile; current state is {state}")]
    UnsupportedCapability { state: AnpClientState },
    #[error("illegal state transition {from} -> {to}")]
    IllegalState {
        from: AnpClientState,
        to: AnpClientState,
    },
    #[error("core error: {0}")]
    Core(#[from] CoreError),
    #[error("peer is not connected")]
    NotConnected,
}

/// Configuration for the ANP client driver.
#[derive(Debug, Clone)]
pub struct AnpClientConfig {
    /// Peer endpoint (localhost fixture or real ANP endpoint).
    pub endpoint: String,
    /// Optional pinned DID for production trust policy.
    pub expected_did: Option<String>,
    /// Optional pinned key fingerprint.
    pub expected_key_fingerprint: Option<String>,
    /// Local agent name presented to the peer in `TaskInvoke`.
    pub local_agent: String,
}

/// Profile-aware ANP client driver.
pub struct AnpClientDriver<T: AnpTransport> {
    transport: T,
    config: AnpClientConfig,
    state: RwLock<AnpClientState>,
    peer: RwLock<Option<VerifiedAnpPeer>>,
    capabilities: RwLock<Option<AnpCapabilities>>,
    remote_task_ids: Arc<RwLock<HashMap<TaskId, AnpTaskId>>>,
    id: String,
}

impl<T: AnpTransport> AnpClientDriver<T> {
    pub fn new(transport: T, config: AnpClientConfig) -> Self {
        let id = format!("anp:{}", config.endpoint);
        Self {
            transport,
            config,
            state: RwLock::new(AnpClientState::Disconnected),
            peer: RwLock::new(None),
            capabilities: RwLock::new(None),
            remote_task_ids: Arc::new(RwLock::new(HashMap::new())),
            id,
        }
    }

    pub async fn state(&self) -> AnpClientState {
        *self.state.read().await
    }

    /// Connects, verifies identity and negotiates the task profile.
    ///
    /// Follows the handoff state machine. On no-common-profile the driver
    /// ends in `MessagingOnly` and task commands fail explicitly.
    pub async fn connect(&self) -> Result<AnpClientState, AnpClientError> {
        self.apply_transition(AnpClientState::Connecting).await?;

        let peer = match self
            .transport
            .connect(PeerRef {
                endpoint: self.config.endpoint.clone(),
                expected_did: self.config.expected_did.clone(),
                expected_key_fingerprint: self.config.expected_key_fingerprint.clone(),
            })
            .await
        {
            Ok(peer) => peer,
            Err(e) => {
                self.apply_transition(AnpClientState::Failed).await?;
                return Err(AnpClientError::Transport(e));
            }
        };
        self.apply_transition(AnpClientState::IdentityVerified)
            .await?;
        *self.peer.write().await = Some(peer.clone());

        let caps = self.transport.capabilities(&peer).await?;
        *self.capabilities.write().await = Some(caps.clone());

        self.apply_transition(AnpClientState::Negotiating).await?;
        let offer = ProfileOffer {
            profiles: vec![PROFILE_ID.to_string()],
        };
        match self.transport.negotiate(&peer, offer).await {
            Ok(n) if n.status == NegotiationStatus::Accepted => {
                self.apply_transition(AnpClientState::TaskProfileReady)
                    .await?;
                Ok(AnpClientState::TaskProfileReady)
            }
            Ok(_) => {
                self.apply_transition(AnpClientState::MessagingOnly).await?;
                Ok(AnpClientState::MessagingOnly)
            }
            Err(AnpError::NoCommonProfile { .. }) => {
                self.apply_transition(AnpClientState::MessagingOnly).await?;
                Ok(AnpClientState::MessagingOnly)
            }
            Err(e) => {
                self.apply_transition(AnpClientState::Failed).await?;
                Err(AnpClientError::Transport(e))
            }
        }
    }

    async fn apply_transition(&self, to: AnpClientState) -> Result<(), AnpClientError> {
        let mut guard = self.state.write().await;
        let from = *guard;
        let next = transition(from, to).map_err(|_| AnpClientError::IllegalState { from, to })?;
        *guard = next;
        Ok(())
    }

    async fn require_task_profile(&self) -> Result<(), AnpClientError> {
        let state = *self.state.read().await;
        if !state.allows_task_commands() {
            return Err(AnpClientError::UnsupportedCapability { state });
        }
        Ok(())
    }

    async fn peer_checked(&self) -> Result<VerifiedAnpPeer, AnpClientError> {
        self.peer
            .read()
            .await
            .clone()
            .ok_or(AnpClientError::NotConnected)
    }

    /// Sends a `TaskInvoke`. Returns the wire message id used.
    pub async fn send_invoke(
        &self,
        anp_task_id: AnpTaskId,
        operation_id: OperationId,
        payload: serde_json::Value,
    ) -> Result<MessageId, AnpClientError> {
        self.require_task_profile().await?;
        let peer = self.peer_checked().await?;
        let message_id = MessageId::new();
        let msg = AnpMessage {
            message_id: AnpMessageId(message_id.0.clone()),
            kind: "task.invoke".into(),
            body: AnpMessageBody::Json(serde_json::to_string(&TaskInvoke {
                task_id: anp_task_id.clone(),
                operation_id,
                message_id: message_id.clone(),
                agent: self.config.local_agent.clone(),
                payload,
            })?),
        };
        self.transport.send(&peer, msg).await?;
        Ok(message_id)
    }

    /// Sends a `TaskCancel`.
    pub async fn send_cancel(
        &self,
        anp_task_id: AnpTaskId,
        operation_id: OperationId,
    ) -> Result<MessageId, AnpClientError> {
        self.require_task_profile().await?;
        let peer = self.peer_checked().await?;
        let message_id = MessageId::new();
        let msg = AnpMessage {
            message_id: AnpMessageId(message_id.0.clone()),
            kind: "task.cancel".into(),
            body: AnpMessageBody::Json(serde_json::to_string(&TaskCancel {
                task_id: anp_task_id,
                operation_id,
                message_id: message_id.clone(),
            })?),
        };
        self.transport.send(&peer, msg).await?;
        Ok(message_id)
    }

    /// Records the remote ANP task id for a local core task.
    pub async fn record_remote_task_id(&self, local: TaskId, remote: AnpTaskId) {
        self.remote_task_ids.write().await.insert(local, remote);
    }

    /// Translates an inbound `TaskEvent` into a core `DriverEvent`.
    ///
    /// `None` for `Accepted` (no consumer-facing progress).
    pub fn translate_event(ev: &TaskEvent) -> Option<DriverEvent> {
        use protocol_anp_profile::TaskState;
        match ev {
            TaskEvent::Accepted(_) => None,
            TaskEvent::Status(e) => match e.state {
                TaskState::Completed => Some(DriverEvent::Completed(vec![])),
                TaskState::Failed => Some(DriverEvent::Failed(PublicError {
                    code: "anp.task_failed".into(),
                    message: "remote task failed".into(),
                    retryable: false,
                })),
                TaskState::Cancelled => Some(DriverEvent::Cancelled),
            },
            TaskEvent::InputRequired(e) => {
                Some(DriverEvent::InputRequired(adapter_model::InputRequest {
                    question: e.prompt.clone(),
                    schema: None,
                }))
            }
            TaskEvent::Progress(e) => Some(DriverEvent::Progress {
                message: e
                    .payload
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("progress")
                    .to_string(),
                percent: None,
            }),
            TaskEvent::Artifact(e) => Some(DriverEvent::Artifact(adapter_model::ArtifactRef {
                id: e.artifact.uri.clone(),
                name: e
                    .artifact
                    .uri
                    .rsplit('/')
                    .next()
                    .unwrap_or("artifact")
                    .to_string(),
                mime_type: e
                    .artifact
                    .mime_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".into()),
                size_bytes: 0,
                uri: Some(e.artifact.uri.clone()),
            })),
            TaskEvent::Completed(e) => Some(DriverEvent::Completed(
                e.artifacts
                    .iter()
                    .map(|a| Part::FileRef {
                        uri: a.uri.clone(),
                        mime_type: a.mime_type.clone(),
                    })
                    .collect(),
            )),
            TaskEvent::Failed(e) => Some(DriverEvent::Failed(PublicError {
                code: "anp.task_failed".into(),
                message: e.error.clone(),
                retryable: false,
            })),
            TaskEvent::Cancelled(_) => Some(DriverEvent::Cancelled),
        }
    }

    /// Runs the driver against a live event stream by translating each
    /// event and forwarding it to the consumer channel.
    pub async fn forward_events(
        &self,
        mut events: impl StreamEvents,
        tx: mpsc::Sender<DriverEvent>,
    ) -> Result<(), AnpClientError> {
        while let Some(ev) = events.next_event().await? {
            protocol_anp_profile::validation::validate_event(&ev)?;
            if let Some(driver_event) = Self::translate_event(&ev) {
                if tx.send(driver_event).await.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Minimal event source abstraction to keep the driver free of a specific
/// inbound channel type.
#[async_trait]
pub trait StreamEvents: Send {
    async fn next_event(&mut self) -> Result<Option<TaskEvent>, AnpClientError>;
}

/// Turns an `mpsc::Receiver<TaskEvent>` into a [`StreamEvents`].
pub struct EventChannel(pub mpsc::Receiver<TaskEvent>);

#[async_trait]
impl StreamEvents for EventChannel {
    async fn next_event(&mut self) -> Result<Option<TaskEvent>, AnpClientError> {
        Ok(self.0.recv().await)
    }
}

#[async_trait]
impl<T: AnpTransport> AgentDriver for AnpClientDriver<T> {
    fn id(&self) -> &str {
        &self.id
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            provide_input: true,
        }
    }

    async fn health(&self) -> Result<(), CoreError> {
        match self.state().await {
            AnpClientState::TaskProfileReady => Ok(()),
            s => Err(CoreError::Driver(format!(
                "ANP driver not ready (state {s})"
            ))),
        }
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        _request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let state = self.state().await;
        if !state.allows_task_commands() {
            return Err(CoreError::Driver(format!(
                "ANP task invoke requires `{PROFILE_ID}`; state is {state}"
            )));
        }
        let (tx, rx) = mpsc::channel(32);
        let _ = tx.send(DriverEvent::Accepted).await;
        warn!(task_id = %task_id, "ANP driver invoke: outbound event streaming is not yet wired to a real peer transport; use send_invoke for now");
        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        let state = self.state().await;
        if !state.allows_task_commands() {
            return Err(CoreError::Driver(format!(
                "ANP task cancel requires `{PROFILE_ID}`; state is {state}"
            )));
        }
        // Correlation metadata lookup + wire cancel would go here once the
        // inbound/outbound event plumbing lands. Explicitly NOT a no-op that
        // pretends success.
        let _ = self.remote_task_ids.read().await;
        Err(CoreError::Driver(format!(
            "ANP cancel not yet wired for local task {task_id}"
        )))
    }

    async fn provide_input(&self, task_id: TaskId, _input: Vec<Part>) -> Result<(), CoreError> {
        let state = self.state().await;
        if !state.allows_task_commands() {
            return Err(CoreError::Driver(format!(
                "ANP provide_input requires `{PROFILE_ID}`; state is {state}"
            )));
        }
        let _ = self.remote_task_ids.read().await;
        Err(CoreError::Driver(format!(
            "ANP provide_input not yet wired for local task {task_id}"
        )))
    }
}

impl From<serde_json::Error> for AnpClientError {
    fn from(e: serde_json::Error) -> Self {
        AnpClientError::Profile(ProfileError::Serde(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anp_transport::{FakeAnpTransport, FakePeerSpec};

    fn task_peer() -> FakePeerSpec {
        FakePeerSpec::new("http://127.0.0.1:9910", "did:anp:local:drv-a")
            .with_capabilities(["agent-connector.anp-task.v1"])
            .with_accepts_offer(true)
    }

    fn messaging_peer() -> FakePeerSpec {
        FakePeerSpec::new("http://127.0.0.1:9911", "did:anp:local:drv-b")
            .with_capabilities(["direct.send"])
            .with_accepts_offer(true)
    }

    fn config(endpoint: &str) -> AnpClientConfig {
        AnpClientConfig {
            endpoint: endpoint.into(),
            expected_did: None,
            expected_key_fingerprint: None,
            local_agent: "local-agent".into(),
        }
    }

    #[tokio::test]
    async fn profile_selected_reaches_task_profile_ready() {
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([task_peer()]),
            config("http://127.0.0.1:9910"),
        );
        let state = driver.connect().await.unwrap();
        assert_eq!(state, AnpClientState::TaskProfileReady);
        assert_eq!(driver.state().await, AnpClientState::TaskProfileReady);
    }

    #[tokio::test]
    async fn no_common_profile_falls_back_messaging_only() {
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([messaging_peer()]),
            config("http://127.0.0.1:9911"),
        );
        let state = driver.connect().await.unwrap();
        assert_eq!(state, AnpClientState::MessagingOnly);
        // Task commands are explicitly unsupported in MessagingOnly.
        assert!(!driver.state().await.allows_task_commands());
    }

    #[tokio::test]
    async fn identity_failure_does_not_fall_back_to_insecure() {
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([task_peer()]),
            AnpClientConfig {
                endpoint: "http://127.0.0.1:9910".into(),
                expected_did: Some("did:anp:production".into()),
                expected_key_fingerprint: None,
                local_agent: "local-agent".into(),
            },
        );
        let r = driver.connect().await;
        assert!(matches!(
            r,
            Err(AnpClientError::Transport(
                AnpError::IdentityVerificationFailed(_)
            ))
        ));
        assert_eq!(driver.state().await, AnpClientState::Failed);
    }

    #[tokio::test]
    async fn generic_peer_gives_no_false_task_capabilities() {
        // A peer that advertises only direct.send must NOT report task
        // capability via the driver's task-command path.
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([messaging_peer()]),
            config("http://127.0.0.1:9911"),
        );
        let _ = driver.connect().await;
        let r = driver
            .invoke(
                adapter_model::TaskId::new_v4(),
                InvokeRequest {
                    task_id: None,
                    agent_id: None,
                    skill_id: None,
                    idempotency_key: "k".into(),
                    session_id: None,
                    input: vec![],
                    context: serde_json::json!({}),
                    deadline: None,
                },
            )
            .await;
        assert!(matches!(r, Err(CoreError::Driver(_))));
    }

    #[tokio::test]
    async fn task_ready_driver_reports_health_ok() {
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([task_peer()]),
            config("http://127.0.0.1:9910"),
        );
        let _ = driver.connect().await;
        assert!(driver.health().await.is_ok());
    }

    #[tokio::test]
    async fn messaging_only_driver_reports_unhealthy() {
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([messaging_peer()]),
            config("http://127.0.0.1:9911"),
        );
        let _ = driver.connect().await;
        assert!(driver.health().await.is_err());
    }

    #[tokio::test]
    async fn send_invoke_requires_task_profile() {
        let driver = AnpClientDriver::new(
            FakeAnpTransport::new([messaging_peer()]),
            config("http://127.0.0.1:9911"),
        );
        let _ = driver.connect().await;
        let r = driver
            .send_invoke(
                AnpTaskId("t-1".into()),
                OperationId("op-1".into()),
                serde_json::json!({}),
            )
            .await;
        assert!(matches!(
            r,
            Err(AnpClientError::UnsupportedCapability { .. })
        ));
    }

    #[tokio::test]
    async fn translate_terminal_events() {
        let ev = TaskEvent::Completed(protocol_anp_profile::TaskCompleted {
            task_id: AnpTaskId("t".into()),
            seq: 2,
            message_id: MessageId("m".into()),
            artifacts: vec![],
        });
        assert!(matches!(
            AnpClientDriver::<FakeAnpTransport>::translate_event(&ev),
            Some(DriverEvent::Completed(_))
        ));
    }
}
