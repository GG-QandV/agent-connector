//! gatewayd/src/dialect_probe.rs
//!
//! Диалект-зонд для Направления 2 (A2A-клиент -> A2A-агент, passthrough).
//! Реализует §3 ТЗ (TZ-a2a-dialects-gateway-adapter.md): шлюз сам ходит к
//! сторонним A2A-агентам через transport_a2a_passthrough (Transport::Http{url,
//! push_token}) и должен знать, на каком диалекте (SDK/Spec) агент отвечает,
//! ДО того как проксировать первый реальный запрос клиента — иначе passthrough
//! слепо форвардит байты, не проверяя, что получит осмысленный ответ.
//!
//! Источники истины, прочитанные целиком перед написанием:
//! - transport_a2a_passthrough.rs — PassthroughState{registry, client}, чистый
//!   reverse-proxy без парсинга тела; зонд — ОТДЕЛЬНЫЙ активный вызов той же
//!   reqwest::Client, не перехват проксируемого потока.
//! - transport_http.rs — коды -32601 (AdapterError::UnknownAgent, НЕ про формат),
//!   -32000 с текстом "method_not_found:" (реальный признак нераспознанного
//!   метода), -32010 (CONTEXT_LOST_CODE).
//! - registry.rs — Transport::Http{url, push_token}, откуда зонд берёт base URL.
//!
//! Принцип (§3.1 ТЗ): зонд идемпотентен — GetTask/tasks/get с несуществующим
//! task_id (случайный UUID), НЕ SendMessage/message/send (те создают задачу).
//! Результат кэшируется на agent_id — один зонд на первый контакт.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde_json::{json, Value};
use uuid::Uuid;

/// Диалект A2A, определённый зондом.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum A2aDialect {
    /// JSON-RPC слой SDK a2a-rs: методы SendMessage/GetTask/CancelTask.
    Sdk,
    /// Семантический JSON-RPC (наш шлюз, ACP-A2A_gateway): message/send,
    /// tasks/get, tasks/cancel.
    Spec,
}

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("probe request failed: {0}")]
    Http(String),
    #[error("agent responded with unrecognized dialect (neither SDK nor Spec JSON-RPC methods matched)")]
    Unrecognized,
}

/// Кэш результата детекта на agent_id. Один зонд на первый контакт — повторные
/// запросы к тому же агенту не вызывают зонд снова (§3.3 ТЗ: "Кэширование:
/// результат детекта хранится на эндпоинт").
#[derive(Clone)]
pub struct DialectCache {
    cache: Arc<DashMap<String, A2aDialect>>,
}

impl DialectCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, agent_id: &str) -> Option<A2aDialect> {
        self.cache.get(agent_id).map(|entry| *entry.value())
    }

    pub fn set(&self, agent_id: &str, dialect: A2aDialect) {
        self.cache.insert(agent_id.to_string(), dialect);
    }
}

impl Default for DialectCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Выполняет зонд к базовому URL агента и определяет диалект.
///
/// Алгоритм (§3.3 ТЗ, детерминированный порядок попыток — не эвристика):
/// 1. Пробуем SDK-стиль: `GetTask` с `{ "name": "tasks/<uuid>" }`.
///    - Любой ответ, где НЕ встречается маркер "method_not_found:" и код
///      НЕ равен -32601 в значении, специфичном для неизвестного метода
///      (см. from_probe_response) — сервер понимает SDK.
/// 2. Если SDK не распознан — пробуем Spec-стиль: `tasks/get` с
///    `{ "id": "<uuid>" }`.
///    - Аналогичная проверка на "method_not_found:".
/// 3. Если оба не распознаны — ProbeError::Unrecognized (клиент/вызывающий
///    код решает, пробовать ли ACP или сдаться — это вне ответственности
///    A2A-зонда, см. §3.3 таблицу "и ACP не распознал").
///
/// Зонд НЕ создаёт задач: GetTask/tasks/get на случайный UUID гарантированно
/// не совпадает с реальной задачей на сервере, поэтому единственный
/// осмысленный ответ агента — "task not found" (в любом диалекте), что и
/// является позитивным сигналом распознавания метода.
pub async fn probe_dialect(
    client: &reqwest::Client,
    base_url: &str,
    push_token: Option<&str>,
) -> Result<A2aDialect, ProbeError> {
    let probe_task_id = Uuid::new_v4().to_string();

    let sdk_payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "GetTask",
        "params": { "name": format!("tasks/{probe_task_id}") }
    });

    if let Some(dialect) = try_probe_request(client, base_url, push_token, &sdk_payload, A2aDialect::Sdk).await? {
        return Ok(dialect);
    }

    let spec_payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tasks/get",
        "params": { "id": probe_task_id }
    });

    if let Some(dialect) =
        try_probe_request(client, base_url, push_token, &spec_payload, A2aDialect::Spec).await?
    {
        return Ok(dialect);
    }

    Err(ProbeError::Unrecognized)
}

/// Отправляет один зонд-запрос и решает, распознан ли диалект. Возвращает
/// Ok(Some(dialect)), если сервер понял метод (независимо от того, нашёл ли
/// он задачу — задачи с случайным UUID быть не может, поэтому "не найдена"
/// это ОЖИДАЕМЫЙ положительный исход, а не провал зонда).
/// Возвращает Ok(None), если сервер явно ответил "method_not_found" —
/// нужно пробовать следующий диалект.
/// Возвращает Err на транспортную ошибку (сервер недоступен вовсе).
async fn try_probe_request(
    client: &reqwest::Client,
    base_url: &str,
    push_token: Option<&str>,
    payload: &Value,
    candidate: A2aDialect,
) -> Result<Option<A2aDialect>, ProbeError> {
    let mut req = client
        .post(base_url)
        .timeout(Duration::from_secs(10))
        .json(payload);
    if let Some(token) = push_token {
        req = req.bearer_auth(token);
    }

    let resp = req.send().await.map_err(|e| ProbeError::Http(e.to_string()))?;
    let body: Value = resp.json().await.map_err(|e| ProbeError::Http(e.to_string()))?;

    Ok(interpret_probe_response(&body).then_some(candidate))
}

/// Решает, означает ли тело ответа "метод распознан" для целей зонда.
///
/// Ключевой факт (из transport_http.rs, живой код шлюза): неизвестный метод
/// в dispatch_a2a_method всегда даёт `anyhow::bail!("method_not_found: {other}")`,
/// что оборачивается в generic ветку rpc_handler как {"error": {"code": -32000,
/// "message": "method_not_found: <метод>"}}. Отдельно AdapterError::UnknownAgent
/// (агент не найден) даёт -32601 — но это про адресацию, не про диалект метода,
/// и в контексте зонда (агент уже выбран по agent_id до вызова зонда) такой код
/// не должен встречаться вовсе.
///
/// Поэтому: "метод НЕ распознан" <=> message содержит "method_not_found:".
/// Любой другой ответ (включая error "task not found", "no such task" и т.п.,
/// или success с null-результатом) означает, что сервер понял ФОРМАТ запроса
/// и просто не нашёл несуществующую задачу — то есть диалект определён.
fn interpret_probe_response(body: &Value) -> bool {
    const METHOD_NOT_FOUND_MARKER: &str = "method_not_found:";

    if let Some(error) = body.get("error") {
        let message = error.get("message").and_then(Value::as_str).unwrap_or("");
        return !message.contains(METHOD_NOT_FOUND_MARKER);
    }

    // Нет поля "error" — сервер ответил result (даже пустым/null). Метод
    // распознан по определению JSON-RPC: error возвращается только на сбой.
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_method_not_found_marker_as_unrecognized() {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32000, "message": "method_not_found: GetTask" }
        });
        assert!(!interpret_probe_response(&body));
    }

    #[test]
    fn task_not_found_error_is_recognized_as_dialect_match() {
        // Ключевой позитивный случай: сервер понял метод (GetTask/tasks/get),
        // но не нашёл задачу со случайным UUID — это ОЖИДАЕМЫЙ ответ на зонд,
        // не провал. Текст сообщения не содержит "method_not_found:".
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32001, "message": "task not found: task-deadbeef" }
        });
        assert!(interpret_probe_response(&body));
    }

    #[test]
    fn successful_result_is_recognized_as_dialect_match() {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "task": { "id": "task-x", "status": { "state": "TASK_STATE_UNSPECIFIED" } } }
        });
        assert!(interpret_probe_response(&body));
    }

    #[test]
    fn missing_error_and_missing_result_still_counts_as_recognized() {
        // Вырожденный случай: пустое body без error — не наш штатный формат
        // ответа, но раз нет явного отказа "method_not_found", лучше не
        // блокировать зонд ложноотрицательным решением.
        let body = json!({ "jsonrpc": "2.0", "id": 1 });
        assert!(interpret_probe_response(&body));
    }

    #[test]
    fn dialect_cache_returns_none_before_first_probe() {
        let cache = DialectCache::new();
        assert_eq!(cache.get("unknown-agent"), None);
    }

    #[test]
    fn dialect_cache_stores_and_retrieves_result() {
        let cache = DialectCache::new();
        cache.set("hermes", A2aDialect::Spec);
        assert_eq!(cache.get("hermes"), Some(A2aDialect::Spec));
    }

    #[test]
    fn dialect_cache_overwrites_on_second_set() {
        let cache = DialectCache::new();
        cache.set("hermes", A2aDialect::Sdk);
        cache.set("hermes", A2aDialect::Spec);
        assert_eq!(cache.get("hermes"), Some(A2aDialect::Spec));
    }

    /// Регрессия: -32601 (UnknownAgent на СВОЁМ шлюзе, направление 4) не
    /// перепутан с "method not found" зонда, направленного на ЧУЖОЙ агент
    /// (направление 2). Зонд смотрит только на текстовый маркер
    /// "method_not_found:", не на числовой код — потому что -32601 на
    /// стороне чужого агента может означать что угодно (это не наш код,
    /// у чужого агента своя нумерация), а вот текст "method_not_found:"
    /// специфичен именно нашему собственному dispatch_a2a_method формату
    /// и не должен интерпретироваться при зонде СВОЕГО шлюза чужими агентами
    /// (они его не генерируют). Тест фиксирует: код -32601 БЕЗ маркера в
    /// тексте не блокирует распознавание.
    #[test]
    fn code_32601_without_marker_text_does_not_block_recognition() {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32601, "message": "some other agent-specific meaning" }
        });
        assert!(interpret_probe_response(&body));
    }
}

// ============================================================================
// Интеграция в transport_a2a_passthrough.rs — дифф
// ============================================================================
//
// PassthroughState получает поле dialect_cache: DialectCache. proxy_handler
// вызывает probe_dialect() лениво при первом обращении к agent_id (аналогично
// get_or_spawn_adapter в transport_http.rs, но без подъёма процесса — тут
// агент уже внешний HTTP-сервис, только запрос-ответ).
//
// Использование результата зонда в самом passthrough (§3.4 ТЗ: "Направление 2
// изменений не требует на уровне протокола — passthrough forward'ит байты как
// есть") ограничено диагностикой: если зонд вернул Unrecognized, лог фиксирует
// это ДО первого реального проксирования, чтобы оператор увидел причину
// будущих ошибок клиента сразу в логах шлюза, а не только в ответе клиенту.
//
/*
--- a/gatewayd/src/transport_a2a_passthrough.rs
+++ b/gatewayd/src/transport_a2a_passthrough.rs
@@ use crate::registry::{Registry, Transport};
+use crate::dialect_probe::{probe_dialect, DialectCache};

 pub struct PassthroughState {
     registry: Arc<Registry>,
     client: reqwest::Client,
+    dialect_cache: DialectCache,
 }

 pub fn router(registry: Arc<Registry>) -> Router {
     let state = Arc::new(PassthroughState {
         registry,
         client: reqwest::Client::builder()
             .timeout(std::time::Duration::from_secs(300))
             .connect_timeout(std::time::Duration::from_secs(10))
             .build()
             .expect("reqwest client builds with default TLS backend"),
+        dialect_cache: DialectCache::new(),
     });

@@ async fn proxy_handler(
     let Transport::Http { url, push_token } = entry.transport else {
         return (
             StatusCode::BAD_REQUEST,
             "agent_id is not an A2A/http agent (use TCP transport for ACP targets)",
         )
             .into_response();
     };

+    // Зонд выполняется один раз на agent_id (кэш), не блокирует запрос
+    // клиента результатом — только логирует нераспознанный диалект для
+    // диагностики. Клиентский запрос проксируется в любом случае: зонд
+    // информативен, не блокирует passthrough по духу направления 2
+    // ("reverse-proxy, без семантического преобразования").
+    if state.dialect_cache.get(&agent_id).is_none() {
+        match probe_dialect(&state.client, &url, push_token.as_deref()).await {
+            Ok(dialect) => {
+                state.dialect_cache.set(&agent_id, dialect);
+                tracing::info!(agent_id, ?dialect, "a2a dialect probed");
+            }
+            Err(e) => {
+                tracing::warn!(agent_id, error = %e, "a2a dialect probe failed — proxying anyway");
+            }
+        }
+    }
+
     let target_url = build_target_url(&url, &path, request.uri().query());
*/
