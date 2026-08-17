//! crates/driver-a2a-client/src/lib.rs
//!
//! Точка входа драйвера. Диспетчеризация по wire_format происходит один
//! раз в new() — дальше вся логика invoke/get/cancel работает только с
//! NormalizedTask и не знает, какой диалект JSON-RPC используется.

pub mod error;
pub mod wire;

use error::{from_jsonrpc_error, A2aClientError};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use wire::{sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire, NormalizedPart, NormalizedTask};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum A2aWireFormat {
    #[default]
    Sdk,
    Spec,
}

#[derive(Clone, Debug)]
pub struct A2aClientConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub wire_format: A2aWireFormat,
    pub timeout_secs: u64,
}

impl Default for A2aClientConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            token: None,
            wire_format: A2aWireFormat::default(),
            timeout_secs: 30,
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

    /// Отправляет новое сообщение (SendMessage / message/send в зависимости
    /// от wire_format). task_id передаётся, если это продолжение диалога.
    pub async fn invoke(
        &self,
        text: &str,
        context_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<NormalizedTask, A2aClientError> {
        let parts = vec![NormalizedPart::text(text)];
        let op = A2aOperation::SendMessage {
            parts: &parts,
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
            let message = err.get("message").and_then(Value::as_str).unwrap_or("unknown error");
            return Err(from_jsonrpc_error(code, message, method, self.wire.name()));
        }

        let result = body
            .get("result")
            .ok_or_else(|| A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into()))?;

        self.wire.parse_task(result)
    }
}
