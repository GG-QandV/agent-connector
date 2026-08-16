//! crates/driver-a2a-client/src/error.rs
//!
//! Единый тип ошибок driver-a2a-client. Покрывает транспорт, протокол JSON-RPC
//! и специфичные для A2A-домена ситуации (context lost, method mismatch).

use std::fmt;

#[derive(Debug)]
pub enum A2aClientError {
    /// Ошибка транспорта (сеть, таймаут, невалидный HTTP-ответ).
    Http(String),

    /// Тело ответа не соответствует ожидаемой JSON-RPC структуре
    /// (нет `result`, нет `task`, отсутствуют обязательные поля).
    ProtocolError(String),

    /// Сервер вернул `-32601 METHOD_NOT_FOUND`. Почти всегда означает,
    /// что `wire_format` в конфиге эндпоинта выбран неверно.
    MethodNotFound {
        method: String,
        wire_format: &'static str,
        server_message: String,
    },

    /// Сервер вернул `-32010` (потеря контекста, например после перезапуска
    /// агента за шлюзом). Retryable: клиент должен начать новый context_id.
    ContextLost { server_message: String },

    /// Любая прочая ошибка уровня приложения (`-32000` и остальные коды).
    RemoteError { code: i64, message: String },
}

impl fmt::Display for A2aClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            A2aClientError::Http(msg) => write!(f, "A2A transport error: {msg}"),
            A2aClientError::ProtocolError(msg) => write!(f, "A2A protocol error: {msg}"),
            A2aClientError::MethodNotFound {
                method,
                wire_format,
                server_message,
            } => write!(
                f,
                "Method '{method}' not found (wire_format={wire_format}). \
                 Hint: check that the endpoint's wire_format matches the server's \
                 expected JSON-RPC dialect (sdk ↔ SendMessage, spec ↔ message/send). \
                 Server said: {server_message}"
            ),
            A2aClientError::ContextLost { server_message } => write!(
                f,
                "A2A context lost (-32010), likely due to agent restart behind the \
                 gateway. Retry with a fresh context_id (do not reuse task_id). \
                 Server said: {server_message}"
            ),
            A2aClientError::RemoteError { code, message } => {
                write!(f, "A2A remote error [{code}]: {message}")
            }
        }
    }
}

impl std::error::Error for A2aClientError {}

/// Строит ошибку из тела JSON-RPC `error` объекта, зная метод и wire_format
/// текущего запроса — чтобы MethodNotFound сразу содержал подсказку.
pub fn from_jsonrpc_error(
    code: i64,
    message: &str,
    method: &str,
    wire_format: &'static str,
) -> A2aClientError {
    match code {
        -32601 => A2aClientError::MethodNotFound {
            method: method.to_string(),
            wire_format,
            server_message: message.to_string(),
        },
        -32010 => A2aClientError::ContextLost {
            server_message: message.to_string(),
        },
        other => A2aClientError::RemoteError {
            code: other,
            message: message.to_string(),
        },
    }
}
