// ============================================================================
// ДИФФ v2: wire_format: auto — построен на РЕАЛЬНОМ тексте lib.rs
// (feat/a2a-acp-client-drivers, прочитан целиком через приложенный файл).
//
// Отличия от моего предыдущего (неприменимого) диффа, которые здесь
// исправлены по факту реального кода:
// 1. A2aClientConfig НЕ содержит agent_id — только endpoint/token/
//    wire_format/timeout_secs. AgentDriver::id() возвращает &config.endpoint.
// 2. Нет отдельной struct WireExecutor — execute() это приватный метод
//    самого A2aClientDriver.
// 3. Есть чистовой публичный API (invoke(text,...), get_task, cancel_task) —
//    ОТДЕЛЬНЫЙ от AgentDriver::invoke. Зонд должен резолвить wire ДО первого
//    вызова execute(), а execute() сейчас читает self.wire напрямую (Arc,
//    не Option) — поэтому поле придётся сделать Option<Arc<dyn A2aWire>> +
//    OnceCell, и execute() должен получить wire как параметр или через
//    resolved_wire().
// 4. Структура НЕ ДЕРИВИТ Clone целиком — есть ручной clone_state(). Новое
//    поле auto_wire_cache должно быть добавлено и туда.
// 5. В этой версии НЕТ поллинга — invoke() (AgentDriver) делает один
//    send_parts() и сразу шлёт terminal-событие по его результату. Значит
//    зонд достаточно выполнить один раз перед этим единственным вызовом —
//    не нужно синхронизировать с циклом поллинга (которого нет).
// ============================================================================

/*
--- a/crates/driver-a2a-client/src/lib.rs
+++ b/crates/driver-a2a-client/src/lib.rs
@@ #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
 pub enum A2aWireFormat {
     #[default]
     Sdk,
     Spec,
+    /// Диалект неизвестен на момент конфигурации — определяется зондом
+    /// (dialect_probe::probe_wire_format) при первом вызове execute(),
+    /// результат кэшируется в OnceCell на весь lifetime драйвера.
+    /// Приоритет при неоднозначности — Sdk (§3.4 ТЗ).
+    Auto,
 }
*/

//! crates/driver-a2a-client/src/dialect_probe.rs
//!
//! Идентичен по алгоритму серверной половине (gatewayd/src/dialect_probe.rs):
//! пробуем SDK (GetTask/{"name":"tasks/<uuid>"}), затем Spec (tasks/get/
//! {"id":"<uuid>"}); решение — по маркеру "method_not_found:" в тексте
//! ошибки (см. error.rs::from_jsonrpc_error — тот же принцип), не по коду.

use crate::error::A2aClientError;
use crate::wire::{sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

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
            task_id: &probe_task_id,
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
            task_id: &probe_task_id,
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
    let body: Value = resp
        .json()
        .await
        .map_err(|e| A2aClientError::Http(e.to_string()))?;

    const METHOD_NOT_FOUND_MARKER: &str = "method_not_found:";

    if let Some(error) = body.get("error") {
        let message = error.get("message").and_then(Value::as_str).unwrap_or("");
        return Ok(!message.contains(METHOD_NOT_FOUND_MARKER));
    }

    Ok(true)
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
            .and(body_partial_json(serde_json::json!({ "method": "GetTask" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method_not_found: GetTask" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({ "method": "tasks/get" })))
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
}

// ---------------------------------------------------------------------------
// Правка crates/driver-a2a-client/src/lib.rs — по РЕАЛЬНОЙ структуре
// ---------------------------------------------------------------------------

/*
--- a/crates/driver-a2a-client/src/lib.rs
+++ b/crates/driver-a2a-client/src/lib.rs
@@ pub mod error;
+pub mod dialect_probe;
 pub mod wire;

@@ use wire::{
     sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire, NormalizedPart, NormalizedState,
     NormalizedTask,
 };
+use dialect_probe::probe_wire_format;
+use tokio::sync::OnceCell;

 /// Чистовой A2A-клиент: wire-формат-нейтрален, оперирует NormalizedTask.
 pub struct A2aClientDriver {
     config: A2aClientConfig,
     client: reqwest::Client,
-    wire: Arc<dyn A2aWire>,
+    /// Заполнено сразу в new() при wire_format != Auto. При Auto — None,
+    /// резолвится лениво через auto_wire_cache при первом execute().
+    wire: Option<Arc<dyn A2aWire>>,
+    /// Кэш результата зонда — заполняется один раз при wire_format == Auto,
+    /// повторный зонд не выполняется (OnceCell гарантирует однократность
+    /// инициализации сам по себе).
+    auto_wire_cache: OnceCell<Arc<dyn A2aWire>>,
     remote_task_ids: RemoteTaskIds,
     cancellation_tokens: CancellationTokens,
 }

 impl A2aClientDriver {
     pub fn new(config: A2aClientConfig) -> Result<Self, A2aClientError> {
-        let wire: Arc<dyn A2aWire> = match config.wire_format {
-            A2aWireFormat::Sdk => Arc::new(A2aSdkWire),
-            A2aWireFormat::Spec => Arc::new(A2aSpecWire),
-        };
+        let wire: Option<Arc<dyn A2aWire>> = match config.wire_format {
+            A2aWireFormat::Sdk => Some(Arc::new(A2aSdkWire)),
+            A2aWireFormat::Spec => Some(Arc::new(A2aSpecWire)),
+            A2aWireFormat::Auto => None,
+        };

         let client = reqwest::Client::builder()
             .timeout(Duration::from_secs(config.timeout_secs))
             .build()
             .map_err(|e| A2aClientError::Http(e.to_string()))?;

         Ok(Self {
             config,
             client,
             wire,
+            auto_wire_cache: OnceCell::new(),
             remote_task_ids: Arc::new(DashMap::new()),
             cancellation_tokens: Arc::new(DashMap::new()),
         })
     }

+    /// Возвращает актуальный wire — сразу для Sdk/Spec, лениво через зонд
+    /// для Auto (кэшируется в auto_wire_cache). Единственная точка, через
+    /// которую execute() получает wire.
+    async fn resolved_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
+        if let Some(w) = &self.wire {
+            return Ok(w.clone());
+        }
+        self.auto_wire_cache
+            .get_or_try_init(|| {
+                probe_wire_format(&self.client, &self.config.endpoint, self.config.token.as_deref())
+            })
+            .await
+            .cloned()
+    }

     async fn execute(&self, op: A2aOperation<'_>) -> Result<NormalizedTask, A2aClientError> {
-        let method = self.wire.jsonrpc_method(&op);
-        let params = self.wire.build_params(&op);
+        let wire = self.resolved_wire().await?;
+        let method = wire.jsonrpc_method(&op);
+        let params = wire.build_params(&op);

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
             let message = err
                 .get("message")
                 .and_then(Value::as_str)
                 .unwrap_or("unknown error");
-            return Err(from_jsonrpc_error(code, message, method, self.wire.name()));
+            return Err(from_jsonrpc_error(code, message, method, wire.name()));
         }

         let result = body.get("result").ok_or_else(|| {
             A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into())
         })?;

-        self.wire.parse_task(result)
+        wire.parse_task(result)
     }

@@ impl A2aClientDriver {
     fn clone_state(&self) -> Self {
         Self {
             config: self.config.clone(),
             client: self.client.clone(),
             wire: self.wire.clone(),
+            // OnceCell не Clone напрямую при непустом значении в общем
+            // случае, но здесь нужно ПЕРЕДАТЬ УЖЕ РЕЗОЛВЛЕННЫЙ wire в
+            // спавненную задачу invoke() — не начинать новый зонд в клоне.
+            // Если auto_wire_cache уже инициализирован, копируем его
+            // содержимое в новый OnceCell; если нет — оставляем пустым
+            // (резолвится при первом execute() уже внутри клона).
+            auto_wire_cache: {
+                let cloned = OnceCell::new();
+                if let Some(w) = self.auto_wire_cache.get() {
+                    // set() не может провалиться на свежем OnceCell.
+                    let _ = cloned.set(w.clone());
+                }
+                cloned
+            },
             remote_task_ids: self.remote_task_ids.clone(),
             cancellation_tokens: self.cancellation_tokens.clone(),
         }
     }
 }
*/

#[cfg(test)]
mod auto_wire_lib_tests {
    // Регрессия на реальную интеграцию: A2aClientDriver с wire_format: Auto
    // должен успешно выполнить execute() через зонд, без явного wire в
    // конфиге. Тест не имеет доступа к приватным полям — проверяет только
    // наблюдаемое поведение через публичный invoke().

    use crate::{A2aClientConfig, A2aClientDriver, A2aWireFormat};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn auto_wire_format_resolves_and_completes_invoke() {
        let server = MockServer::start().await;

        // Зонд (GetTask) -> "task not found" -> SDK распознан.
        // Реальный invoke (SendMessage) -> Completed.
        Mock::given(method("POST"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let method_name = body.get("method").and_then(serde_json::Value::as_str).unwrap_or("");
                if method_name == "GetTask" {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32001, "message": "task not found" }
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {
                            "task": {
                                "id": "task-auto-1",
                                "status": { "state": "TASK_STATE_COMPLETED" },
                                "artifacts": [{ "parts": [{ "text": "auto-detected ok" }] }]
                            }
                        }
                    }))
                }
            })
            .mount(&server)
            .await;

        let driver = A2aClientDriver::new(A2aClientConfig {
            endpoint: server.uri(),
            token: None,
            wire_format: A2aWireFormat::Auto,
            timeout_secs: 10,
        })
        .expect("driver builds even with Auto and no wire resolved yet");

        let task = driver
            .invoke("hello", None, None)
            .await
            .expect("invoke must resolve wire via probe and then complete");
        assert_eq!(task.id, "task-auto-1");
    }
}
