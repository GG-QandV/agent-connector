//! JSON-RPC 2.0 wire codec для ACP stdio.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const JSONRPC_VERSION: &str = "2.0";
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcId {
    Number(i64),
    String(String),
    Null,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<JsonRpcId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<JsonRpcId>,
}

impl JsonRpcRequest {
    /// Разобрать строку. Возвращает Ok(Some(request)) для валидного JSON-RPC
    /// request/notification, Ok(None) для malformed/invalid, либо Err с
    /// конкретным JSON-RPC error (для oversized line).
    pub fn parse(line: &str) -> Result<Option<Self>, (JsonRpcError, Option<JsonRpcId>)> {
        let value: Value = serde_json::from_str(line).map_err(|_| (parse_error(), None))?;
        // Если это response — не наш случай (мы — сервер). Требуем request.
        let Some(object) = value.as_object() else {
            return Err((invalid_request("request must be a JSON object"), None));
        };
        let jsonrpc = object.get("jsonrpc").and_then(Value::as_str);
        if jsonrpc != Some(JSONRPC_VERSION) {
            let id = extract_id(&value);
            return Err((invalid_request("jsonrpc must be \"2.0\""), id));
        }
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            let id = extract_id(&value);
            return Err((invalid_request("method is required"), id));
        };
        let id = object
            .get("id")
            .cloned()
            .map(|v| serde_json::from_value(v).unwrap_or(JsonRpcId::Null));
        Ok(Some(JsonRpcRequest {
            jsonrpc: JSONRPC_VERSION.into(),
            method: method.into(),
            id,
            params: object.get("params").cloned(),
        }))
    }

    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

fn extract_id(value: &Value) -> Option<JsonRpcId> {
    value
        .get("id")
        .cloned()
        .map(|v| serde_json::from_value(v).unwrap_or(JsonRpcId::Null))
}

pub fn parse_error() -> JsonRpcError {
    JsonRpcError {
        code: PARSE_ERROR,
        message: "parse error".into(),
        data: None,
    }
}

pub fn invalid_request(message: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: INVALID_REQUEST,
        message: message.into(),
        data: None,
    }
}

pub fn method_not_found(method: &str) -> JsonRpcError {
    JsonRpcError {
        code: METHOD_NOT_FOUND,
        message: format!("method not found: {method}"),
        data: None,
    }
}

pub fn internal_error(message: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: INTERNAL_ERROR,
        message: message.into(),
        data: None,
    }
}

impl JsonRpcResponse {
    pub fn success(id: Option<JsonRpcId>, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn failure(id: Option<JsonRpcId>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            result: None,
            error: Some(error),
            id,
        }
    }

    pub fn to_line(&self) -> String {
        let mut line = serde_json::to_string(self).unwrap_or_else(|_| {
            serde_json::to_string(&Self::failure(None, internal_error("serialization failed")))
                .unwrap_or_else(|_| {
                    r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"serialization failed"}}"#
                        .into()
                })
        });
        line.push('\n');
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_request_parses() {
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}"#;
        let request = JsonRpcRequest::parse(line).unwrap().unwrap();
        assert_eq!(request.method, "session/new");
        assert_eq!(request.id, Some(JsonRpcId::Number(1)));
    }

    #[test]
    fn top_level_array_is_invalid_request_not_parse_error() {
        // Top-level non-object JSON должен быть invalid_request (-32600),
        // а НЕ parse_error (-32700): это валидный JSON, но не объект request.
        let line = r#"[1,2,3]"#;
        let (error, id) = JsonRpcRequest::parse(line).unwrap_err();
        assert_eq!(error.code, INVALID_REQUEST);
        assert_eq!(id, None);
    }

    #[test]
    fn top_level_string_is_invalid_request_with_null_id() {
        let line = r#""hello""#;
        let (error, id) = JsonRpcRequest::parse(line).unwrap_err();
        assert_eq!(error.code, INVALID_REQUEST);
        assert_eq!(id, None);
    }

    #[test]
    fn malformed_json_is_parse_error() {
        let line = r#"{"jsonrpc":"2.0","method":"x","id":1,"#;
        let (error, id) = JsonRpcRequest::parse(line).unwrap_err();
        assert_eq!(error.code, PARSE_ERROR);
        assert_eq!(id, None);
    }

    #[test]
    fn wrong_jsonrpc_version_is_invalid_request() {
        let line = r#"{"jsonrpc":"1.0","id":1,"method":"session/new"}"#;
        let (error, id) = JsonRpcRequest::parse(line).unwrap_err();
        assert_eq!(error.code, INVALID_REQUEST);
        assert_eq!(id, Some(JsonRpcId::Number(1)));
    }

    #[test]
    fn missing_method_is_invalid_request() {
        let line = r#"{"jsonrpc":"2.0","id":2}"#;
        let (error, id) = JsonRpcRequest::parse(line).unwrap_err();
        assert_eq!(error.code, INVALID_REQUEST);
        assert_eq!(id, Some(JsonRpcId::Number(2)));
    }
}
