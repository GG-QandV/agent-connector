//! crates/driver-a2a-client/src/wire/mod.rs
//!
//! Wire-формат — деталь транспорта. AgentDriver-логика (invoke/cancel/
//! provide_input) работает только с NormalizedTask и никогда не видит
//! JSON-RPC имена методов или регистр полей конкретного диалекта.

pub mod sdk;
pub mod spec;

use crate::error::A2aClientError;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NormalizedState {
    Submitted,
    Working,
    InputRequired,
    AuthRequired,
    Completed,
    Failed,
    Canceled,
    Rejected,
}

#[derive(Clone, Debug, Default)]
pub struct NormalizedPart {
    pub text: Option<String>,
    pub raw: Option<Vec<u8>>,
    pub uri: Option<String>,
    pub mime_type: Option<String>,
}

impl NormalizedPart {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            text: Some(s.into()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct NormalizedTask {
    pub id: String,
    pub context_id: Option<String>,
    pub state: NormalizedState,
    pub status_message: Option<String>,
    pub output_parts: Vec<NormalizedPart>,
}

/// Какой RPC-метод использовать для конкретной операции. Явно, а не по
/// unified enum — каждый wire сам решает, как называется метод и куда
/// класть task_id (taskId camelCase vs id в params плоского tasks/get).
pub enum A2aOperation<'a> {
    SendMessage {
        parts: &'a [NormalizedPart],
        context_id: Option<&'a str>,
        task_id: Option<&'a str>,
    },
    GetTask {
        task_id: &'a str,
    },
    CancelTask {
        task_id: &'a str,
    },
}

pub trait A2aWire: Send + Sync {
    /// Человекочитаемое имя wire-формата — используется только в сообщениях
    /// об ошибках (см. error::from_jsonrpc_error), не влияет на протокол.
    fn name(&self) -> &'static str;

    /// JSON-RPC method для данной операции в этом wire-формате.
    fn jsonrpc_method(&self, op: &A2aOperation<'_>) -> &'static str;

    /// Строит `params` JSON-RPC запроса для данной операции.
    fn build_params(&self, op: &A2aOperation<'_>) -> Value;

    /// Разбирает `result` ответа в нормализованный Task, независимо от
    /// того, была ли это операция SendMessage, GetTask или CancelTask —
    /// все три в обоих wire-форматах возвращают Task-подобную структуру.
    fn parse_task(&self, result: &Value) -> Result<NormalizedTask, A2aClientError>;
}
