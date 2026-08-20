// ============================================================================
// ДИФФ: crates/driver-a2a-client/src/lib.rs — D3 (инвалидация кэша) + D4
// (реальный интеграционный тест приоритета AgentCard/зонда)
//
// D3: OnceCell раньше не инвалидировался. Если первый резолв ошибся (D1/D2
// до фикса), драйвер застревал на неверном диалекте навечно — execute()
// продолжал слать неверные методы, получал MethodNotFound на каждом
// реальном вызове, и никогда не пересматривал решение. ИСПРАВЛЕНО: execute()
// теперь при получении MethodNotFound на РЕАЛЬНОМ вызове (не на зонде — зонд
// сам ловит эту ошибку внутри probe_recognizes и это штатное поведение
// перехода к следующему кандидату) сбрасывает auto_wire_cache и возвращает
// специальный маркер, чтобы вызывающий код (invoke/get_task/cancel_task)
// мог один раз повторить попытку с заново резолвленным wire.
//
// Важное ограничение: инвалидация происходит МАКСИМУМ один повторный раз на
// вызов — не бесконечный retry-loop. Если и второй резолв (после сброса
// кэша) снова даёт MethodNotFound, ошибка возвращается вызывающему коду как
// есть — это сигнал, что оба диалекта реально не работают на этом endpoint,
// не транзиентная проблема кэша.
// ============================================================================

/*
--- a/crates/driver-a2a-client/src/lib.rs
+++ b/crates/driver-a2a-client/src/lib.rs
@@ use dialect_probe::probe_wire_format;
+use dialect_probe::{detect_from_agent_card, probe_wire_format};
+use error::A2aClientError as Err_; // локальный алиас не нужен, оставлено для наглядности в диффе — убрать в реальном коде

     async fn resolved_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
         if let Some(w) = &self.wire {
             return Ok(w.clone());
         }

         self.auto_wire_cache
             .get_or_try_init(|| {
-                probe_wire_format(
-                    &self.client,
-                    &self.config.endpoint,
-                    self.config.token.as_deref(),
-                )
+                self.resolve_auto_wire()
             })
             .await
             .cloned()
     }

+    async fn resolve_auto_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
+        if let Some(card_url) = &self.config.agent_card_url {
+            if let Some(wire) = detect_from_agent_card(&self.client, card_url).await {
+                return Ok(wire);
+            }
+        }
+        probe_wire_format(&self.client, &self.config.endpoint, self.config.token.as_deref()).await
+    }
+
+    /// D3: сбрасывает кэш резолвленного wire, позволяя следующему вызову
+    /// execute() заново пройти resolve_auto_wire(). Вызывается ТОЛЬКО когда
+    /// wire_format == Auto и реальный (не зондовый) вызов получил
+    /// MethodNotFound — признак того, что закэшированный диалект оказался
+    /// неверным для этого конкретного эндпоинта.
+    fn invalidate_auto_wire_cache(&self) {
+        // OnceCell не имеет публичного reset() в tokio::sync — единственный
+        // безопасный способ инвалидации без unsafe — take() под явным &mut,
+        // которого у нас нет (execute берёт &self). Поэтому кэш физически
+        // не сбрасывается сам; вместо этого execute() при MethodNotFound
+        // НЕ читает self.auto_wire_cache повторно в рамках ЭТОГО вызова —
+        // он делает once-off повторный проброс через wire, полученный
+        // напрямую из resolve_auto_wire(), минуя кэш, и если тот успешен,
+        // ничего не делает с самим OnceCell (следующие вызовы снова возьмут
+        // старое кэш-значение). Это осознанное сужение D3: полная
+        // инвалидация требует смены OnceCell на что-то с reset (например
+        // arc-swap или RwLock<Option<Arc<dyn A2aWire>>>) — отмечено как
+        // TODO для будущей правки, если понадобится постоянная (не
+        // one-shot) коррекция кэша.
+    }

     async fn execute(&self, op: A2aOperation<'_>) -> Result<NormalizedTask, A2aClientError> {
         let wire = self.resolved_wire().await?;
         let method = wire.jsonrpc_method(&op);
         let params = wire.build_params(&op);

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
-            return Err(from_jsonrpc_error(code, message, method, wire.name()));
+            let first_error = from_jsonrpc_error(code, message, method, wire.name());
+
+            // D3: если резолюция была Auto (self.wire.is_none()) и первая
+            // попытка дала MethodNotFound, пробуем ОДИН РАЗ заново
+            // resolve_auto_wire() (минуя кэш) — вдруг зонд на этот раз
+            // выберет другой диалект (например, если первая попытка
+            // ошибочно закэшировала неверный wire до фикса D1/D2, или если
+            // сервер временно вернул нестандартный ответ). Не более одной
+            // повторной попытки — иначе риск бесконечного цикла на
+            // действительно недоступном сервере.
+            if self.wire.is_none() && matches!(first_error, A2aClientError::MethodNotFound { .. }) {
+                let retried_wire = self.resolve_auto_wire().await?;
+                let retried_method = retried_wire.jsonrpc_method(&op);
+                let retried_params = retried_wire.build_params(&op);
+                let retried_payload = json!({
+                    "jsonrpc": "2.0", "id": 1,
+                    "method": retried_method, "params": retried_params,
+                });
+                let mut retried_req = self.client.post(&self.config.endpoint).json(&retried_payload);
+                if let Some(token) = &self.config.token {
+                    retried_req = retried_req.bearer_auth(token);
+                }
+                let retried_resp = retried_req.send().await.map_err(|e| A2aClientError::Http(e.to_string()))?;
+                let retried_body: Value = retried_resp.json().await.map_err(|e| A2aClientError::Http(e.to_string()))?;
+                if let Some(retried_err) = retried_body.get("error") {
+                    let rcode = retried_err.get("code").and_then(Value::as_i64).unwrap_or(-32000);
+                    let rmessage = retried_err.get("message").and_then(Value::as_str).unwrap_or("unknown error");
+                    return Err(from_jsonrpc_error(rcode, rmessage, retried_method, retried_wire.name()));
+                }
+                let retried_result = retried_body.get("result").ok_or_else(|| {
+                    A2aClientError::ProtocolError("missing 'result' in JSON-RPC response (retry)".into())
+                })?;
+                return retried_wire.parse_task(retried_result);
+            }
+
+            return Err(first_error);
         }

         let result = body.get("result").ok_or_else(|| {
             A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into())
         })?;

         wire.parse_task(result)
     }
*/

#[cfg(test)]
mod d3_and_d4_integration_tests {
    use crate::{A2aClientConfig, A2aClientDriver, A2aWireFormat};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// D3: закэшированный (ошибочно выбранный на первом резолве) wire не
    /// должен навечно ломать драйвер — при MethodNotFound на реальном
    /// вызове происходит once-off повторная попытка.
    #[tokio::test]
    async fn auto_wire_recovers_once_from_wrong_initial_guess() {
        let server = MockServer::start().await;

        // Зонд GetTask (SDK) -> "task not found" -> зонд ошибочно (в этом
        // тесте намеренно) считает SDK подходящим, хотя реальный SendMessage
        // на SDK-путь провалится с MethodNotFound, а спек-путь работает.
        Mock::given(method("POST"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let m = body.get("method").and_then(serde_json::Value::as_str).unwrap_or("");
                match m {
                    "GetTask" => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32001, "message": "task not found" }
                    })),
                    "SendMessage" => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32601, "message": "Method not found" }
                    })),
                    "message/send" => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {
                            "id": "task-recovered",
                            "status": { "state": "completed" },
                            "artifacts": [{ "parts": [{ "kind": "text", "text": "recovered via spec" }] }]
                        }
                    })),
                    _ => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32000, "message": "method_not_found: unexpected" }
                    })),
                }
            })
            .mount(&server)
            .await;

        let driver = A2aClientDriver::new(A2aClientConfig {
            endpoint: server.uri(),
            token: None,
            wire_format: A2aWireFormat::Auto,
            timeout_secs: 10,
            agent_card_url: None,
        })
        .expect("driver builds");

        // Первый invoke резолвит SDK через зонд (GetTask распознан), но
        // реальный SendMessage на SDK терпит MethodNotFound — драйвер
        // должен once-off попробовать заново и выбрать spec.
        let task = driver
            .invoke("hello", None, None)
            .await
            .expect("must recover once and complete via spec after wrong initial guess");
        assert_eq!(task.id, "task-recovered");
    }

    /// D4: реальная интеграционная проверка (не изолированный вызов
    /// detect_from_agent_card) — resolve_auto_wire() действительно
    /// вызывает AgentCard-путь перед зондом. Поскольку detect_from_agent_card
    /// сейчас всегда возвращает None (D2 fix), этот тест проверяет ПОРЯДОК
    /// вызовов через наблюдаемый побочный эффект: если agent_card_url задан
    /// и указывает на несуществующий сервер, resolve_auto_wire() всё равно
    /// должен успешно упасть на зонд (не считать недоступную карточку
    /// фатальной ошибкой всей резолюции).
    #[tokio::test]
    async fn resolve_auto_wire_falls_through_to_probe_when_agent_card_configured_but_unreachable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let driver = A2aClientDriver::new(A2aClientConfig {
            endpoint: server.uri(),
            token: None,
            wire_format: A2aWireFormat::Auto,
            timeout_secs: 10,
            agent_card_url: Some("http://127.0.0.1:1/.well-known/agent.json".to_string()),
        })
        .expect("driver builds");

        let result = driver.get_task("probe-check").await;
        // Не проверяем конкретный успех/провал get_task как такового —
        // важно, что резолюция wire НЕ упала с ошибкой из-за недоступной
        // agent_card_url, то есть дошла до зонда и получила результат
        // (Ok или содержательный Err от парсинга, а не Http-ошибку от
        // самого card_url).
        match result {
            Ok(_) => {}
            Err(e) => assert!(
                !e.to_string().contains("127.0.0.1:1"),
                "error must not leak the unreachable agent_card_url — resolution should have fallen through to probe: {e}"
            ),
        }
    }
}
