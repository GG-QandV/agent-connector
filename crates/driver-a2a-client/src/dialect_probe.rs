//! crates/driver-a2a-client/src/dialect_probe.rs
//!
//! Идентичен по алгоритму серверной половине (gatewayd/src/dialect_probe.rs):
//! пробуем SDK (GetTask/{"name":"tasks/<uuid>"}), затем Spec (tasks/get/
//! {"id":"<uuid>"}); решение — по ДВУМ независимым признакам "метод не
//! распознан": коду -32601 (стандарт JSON-RPC 2.0) ЛИБО нормализованному
//! тексту ошибки, содержащему "method not found" (покрывает наш маркер
//! шлюза "method_not_found:", стандартное "Method not found" и lowercase —
//! см. looks_like_method_not_found и error.rs::from_jsonrpc_error).

use crate::error::A2aClientError;
use crate::wire::{sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

/// Детект по Agent Card (ТЗ §3.2 п.4).
///
/// ВАЖНО (D2, честное признание — не заглушка "для вида"): канал сейчас
/// НЕФУНКЦИОНАЛЕН и намеренно всегда возвращает None. Причина
/// семантическая: поле `protocolVersion` спеки AgentCard описывает версию
/// A2A-протокола ("1.0", "0.9", ...), а НЕ выбор wire-реализации
/// (sdk vs spec). Прежний маппинг "1.x -> Sdk, 0.x -> Spec" был ошибкой:
/// шлюз на спеке 1.0 с плоским Task честно отдаст "1.0" в карточке, но
/// это spec-диалект, а не sdk. Спека не содержит поля, которое надёжно
/// отличало бы wire-реализацию, поэтому единственно корректное поведение —
/// None: резолюция (resolve_auto_wire) падает на эмпирический зонд,
/// который хотя бы проверяет реальное поведение endpoint.
///
/// Функция оставлена как ТОЧКА РАСШИРЕНИЯ: если спека когда-нибудь
/// добавит поле, отличающее wire-реализацию, детект реализуется здесь —
/// порядок в resolve_auto_wire() (AgentCard первым, зонд fallback) менять
/// не придётся.
pub async fn detect_from_agent_card(
    _client: &reqwest::Client,
    _agent_card_url: &str,
) -> Option<Arc<dyn A2aWire>> {
    None
}

pub async fn probe_wire_format(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Arc<dyn A2aWire>, A2aClientError> {
    let probe_task_id = Uuid::new_v4().to_string();

    let sdk_wire: Arc<dyn A2aWire> = Arc::new(A2aSdkWire);
    if probe_recognizes(
        client,
        endpoint,
        token,
        &sdk_wire,
        &A2aOperation::GetTask {
            task_id: probe_task_id.as_str(),
        },
    )
    .await?
    {
        return Ok(sdk_wire);
    }

    let spec_wire: Arc<dyn A2aWire> = Arc::new(A2aSpecWire);
    if probe_recognizes(
        client,
        endpoint,
        token,
        &spec_wire,
        &A2aOperation::GetTask {
            task_id: probe_task_id.as_str(),
        },
    )
    .await?
    {
        return Ok(spec_wire);
    }

    Err(A2aClientError::ProtocolError(
        "dialect probe: endpoint did not recognize SDK or Spec GetTask/tasks-get — \
         cannot auto-detect wire_format, specify it explicitly in config"
            .into(),
    ))
}

async fn probe_recognizes(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
    wire: &Arc<dyn A2aWire>,
    op: &A2aOperation<'_>,
) -> Result<bool, A2aClientError> {
    let method = wire.jsonrpc_method(op);
    let params = wire.build_params(op);

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    let mut req = client.post(endpoint).json(&payload);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| A2aClientError::Http(e.to_string()))?;
    let body: JsonValue = resp
        .json()
        .await
        .map_err(|e| A2aClientError::Http(e.to_string()))?;

    if let Some(error) = body.get("error") {
        let code = error
            .get("code")
            .and_then(JsonValue::as_i64)
            .unwrap_or(-32000);
        let message = error
            .get("message")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        return Ok(!looks_like_method_not_found(code, message));
    }

    Ok(true)
}

/// Распознаёт "метод не найден" в JSON-RPC-ошибке по ДВУМ независимым
/// признакам (D1):
///   1. code == -32601 — стандарт JSON-RPC 2.0, зарезервирован ровно под
///      "Method not found". Любой сервер, отвечающий по спеке (не только
///      наш шлюз), вернёт его на неизвестный метод — без нашего
///      специфичного маркера.
///   2. Нормализованный текст ошибки содержит подстроку "method not found".
///      Нормализация = lowercase + '_' -> ' ', поэтому покрываются три
///      варианта формулировки: наш маркер шлюза "method_not_found:<name>",
///      стандартный JSON-RPC "Method not found" и распространённый
///      lowercase "method not found".
///
/// До D1-фикса зонд смотрел только на подстроку "method_not_found:" и
/// ложно принимал стандартный ответ -32601 "Method not found" за признак
/// того, что диалект подходит (маркер не найден -> Ok(true)), т.е. считал
/// неверный диалект распознанным.
fn looks_like_method_not_found(code: i64, message: &str) -> bool {
    if code == -32601 {
        return true;
    }
    let normalized = message.to_lowercase().replace('_', " ");
    normalized.contains("method not found")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_recognizes_sdk_when_server_understands_get_task() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found: tasks/deadbeef" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must detect a dialect");
        assert_eq!(wire.name(), "sdk");
    }

    #[tokio::test]
    async fn probe_falls_back_to_spec_when_sdk_method_not_found() {
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "GetTask" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method_not_found: GetTask" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "tasks/get" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must fall back to spec");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn probe_errors_when_neither_dialect_recognized() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method_not_found: whatever" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let result = probe_wire_format(&client, &server.uri(), None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn probe_falls_back_to_spec_when_standard_jsonrpc_method_not_found() {
        // D1-регрессия: сервер, отвечающий СТАНДАРТНЫМ JSON-RPC -32601
        // "Method not found" (без нашего маркера "method_not_found:"), на
        // GetTask. До фикса зонд ложно принимал такой ответ за признак
        // SDK-диалекта. Теперь -32601 распознаётся как "метод не найден",
        // и зонд корректно падает на spec.
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "GetTask" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32601, "message": "Method not found" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "tasks/get" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must fall back to spec when SDK answers standard -32601 Method not found");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn probe_falls_back_to_spec_when_textual_method_not_found_variants() {
        // D1: тот же сценарий через нормализованный текст — сервер отвечает
        // lowercase "method not found" (без кода -32601 и без маркера
        // шлюза). Три варианта формулировки сводятся нормализацией к одной
        // подстроке "method not found".
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "GetTask" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method not found" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "tasks/get" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must fall back to spec on textual 'method not found' variant");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn agent_card_always_returns_none_until_spec_grows_a_wire_field() {
        // D2: детект по Agent Card НЕФУНКЦИОНАЛЕН — спека AgentCard не
        // содержит поля, надёжно отличающего wire-реализацию (protocolVersion
        // — версия протокола, не выбор реализации). detect_from_agent_card
        // всегда возвращает None, резолюция падает на зонд. Даже с доступной
        // карточкой с protocolVersion 1.0 не угадываем wire.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "test-agent",
                "protocolVersion": "1.0",
                "url": "https://example.com/rpc"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let result = detect_from_agent_card(&client, &card_url).await;
        assert!(
            result.is_none(),
            "agent card must not guess wire from protocolVersion — fall through to probe (D2)"
        );
    }
}
