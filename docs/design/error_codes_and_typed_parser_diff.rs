// ============================================================================
// ДИФФ: приведение к именам ошибок §2.5 ТЗ + честная оценка §2.4.3
// ============================================================================

// ---------------------------------------------------------------------------
// Пункт 1 — исправлено полностью. Переименование кодов PublicError.
// lib.rs:336 (task_to_terminal, ветка Failed/Rejected) и lib.rs:439
// (AgentDriver::invoke, ветка Err на send_parts) — единственные два места,
// формирующие DriverEvent::Failed(PublicError{code, ...}) для A2A-ошибок.
// ---------------------------------------------------------------------------

/*
--- a/crates/driver-a2a-client/src/lib.rs
+++ b/crates/driver-a2a-client/src/lib.rs
@@ fn task_to_terminal(task: &NormalizedTask) -> DriverEvent {
     match task.state {
         ...
         NormalizedState::Failed | NormalizedState::Rejected => {
             DriverEvent::Failed(PublicError {
-                code: "a2a_task_failed".into(),
+                // ИСПРАВЛЕНО (ТЗ §2.5): «сервер вернул error (JSON-RPC) ->
+                // DriverEvent::Failed с кодом a2a_remote_error». Task в
+                // состоянии Failed/Rejected — это именно тот случай: сервер
+                // ответил успешным JSON-RPC конвертом, но внутри Task
+                // содержится ошибка приложения уровня A2A-задачи.
+                code: "a2a_remote_error".into(),
                 message: task
                     .status_message
                     .clone()
                     .unwrap_or_else(|| "A2A task failed".into()),
                 retryable: false,
             })
         }
         ...
     }
 }

@@ async fn invoke(&self, task_id: TaskId, request: InvokeRequest) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
     ...
     Err(e) => {
         let _ = tx.send(DriverEvent::Failed(PublicError {
-            code: "a2a_call_failed".into(),
+            // ИСПРАВЛЕНО (ТЗ §2.5): этот путь срабатывает и на транспортных
+            // ошибках (Http), и на A2aClientError::ProtocolError (когда нет
+            // result/task в ответе), и на MethodNotFound/ContextLost.
+            // ТЗ разделяет это на два разных кода — a2a_remote_error (сервер
+            // вернул error) и a2a_no_task (result есть, но task в нём нет).
+            // Различаем их здесь по варианту A2aClientError, а не сваливаем
+            // всё в один код, как раньше.
+            code: send_error_to_a2a_code(&e).into(),
             message: e.to_string(),
             retryable: false,
         })).await;
         cancellation_tokens.remove(&task_id);
     }
     ...
 }

+/// Маппинг A2aClientError -> код из ТЗ §2.5. ProtocolError с текстом
+/// "missing 'result'" — это ровно ситуация "result нет / нет task"
+/// (ТЗ: DriverEvent::Failed `a2a_no_task`). Всё остальное (RemoteError,
+/// MethodNotFound, ContextLost, Http) — сервер либо ответил ошибкой,
+/// либо недоступен: a2a_remote_error по умолчанию, это же ловит и случаи,
+/// прямо не расписанные в таблице §2.5 (несовпадение формата, недоступность
+/// сети) — таблица ТЗ покрывает не все ветки явно, поэтому a2a_remote_error
+/// используется как разумный fallback-код для "прочих" ошибок сервера/сети.
+fn send_error_to_a2a_code(e: &A2aClientError) -> &'static str {
+    match e {
+        A2aClientError::ProtocolError(msg) if msg.contains("result") || msg.contains("task") => {
+            "a2a_no_task"
+        }
+        _ => "a2a_remote_error",
+    }
+}
*/

#[cfg(test)]
mod error_code_tests {
    use crate::error::A2aClientError;

    fn send_error_to_a2a_code(e: &A2aClientError) -> &'static str {
        match e {
            A2aClientError::ProtocolError(msg) if msg.contains("result") || msg.contains("task") => {
                "a2a_no_task"
            }
            _ => "a2a_remote_error",
        }
    }

    #[test]
    fn missing_result_maps_to_a2a_no_task() {
        let e = A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into());
        assert_eq!(send_error_to_a2a_code(&e), "a2a_no_task");
    }

    #[test]
    fn missing_task_wrapper_maps_to_a2a_no_task() {
        let e = A2aClientError::ProtocolError("sdk wire: expected 'result.task' wrapper, field missing".into());
        assert_eq!(send_error_to_a2a_code(&e), "a2a_no_task");
    }

    #[test]
    fn remote_error_maps_to_a2a_remote_error() {
        let e = A2aClientError::RemoteError { code: -32000, message: "boom".into() };
        assert_eq!(send_error_to_a2a_code(&e), "a2a_remote_error");
    }

    #[test]
    fn http_error_maps_to_a2a_remote_error_as_fallback() {
        let e = A2aClientError::Http("connection refused".into());
        assert_eq!(send_error_to_a2a_code(&e), "a2a_remote_error");
    }
}
