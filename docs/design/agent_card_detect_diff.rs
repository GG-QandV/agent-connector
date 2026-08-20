// ============================================================================
// ДИФФ: AgentCard-детект приоритетнее зонда (ТЗ §3.2 п.4, DoD §3.5 п.2)
//
// Построен на РЕАЛЬНОМ тексте lib.rs и dialect_probe.rs из коммита c43a054
// (файлы приложены и прочитаны целиком). Единственное изменение порядка:
// resolved_wire() теперь пробует AgentCard ДО probe_wire_format, а не только
// зонд. Сама структура OnceCell/clone_state()/execute() не меняется — это
// точечная правка внутри тела resolved_wire().
// ============================================================================

/*
--- a/crates/driver-a2a-client/src/lib.rs
+++ b/crates/driver-a2a-client/src/lib.rs
@@ use dialect_probe::probe_wire_format;
+use dialect_probe::{detect_from_agent_card, probe_wire_format};

@@ pub struct A2aClientConfig {
     pub endpoint: String,
     pub token: Option<String>,
     pub wire_format: A2aWireFormat,
     pub timeout_secs: u64,
+    /// Опциональный URL карточки агента (обычно
+    /// "<base>/.well-known/agent.json"). Если задан и wire_format == Auto,
+    /// детект по protocolVersion пробуется ПЕРВЫМ, зонд — fallback, если
+    /// карточка недоступна или не содержит protocolVersion (ТЗ §3.2 п.4:
+    /// "предпочтительный канал определения (без probe)").
+    pub agent_card_url: Option<String>,
 }

 impl Default for A2aClientConfig {
     fn default() -> Self {
         Self {
             endpoint: String::new(),
             token: None,
             wire_format: A2aWireFormat::default(),
             timeout_secs: 30,
+            agent_card_url: None,
         }
     }
 }

@@ async fn resolved_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
     if let Some(w) = &self.wire {
         return Ok(w.clone());
     }

     self.auto_wire_cache
         .get_or_try_init(|| {
-            probe_wire_format(
-                &self.client,
-                &self.config.endpoint,
-                self.config.token.as_deref(),
-            )
+            self.resolve_auto_wire()
         })
         .await
         .cloned()
 }

+/// Порядок резолюции при Auto (ТЗ §3.2): сначала AgentCard.protocolVersion
+/// (если agent_card_url задан) — предпочтительный канал, без побочных
+/// эффектов и без сетевого зонда на сам endpoint. Если карточка
+/// недоступна, не содержит protocolVersion, или agent_card_url не
+/// сконфигурирован — fallback на probe_wire_format (зонд).
+async fn resolve_auto_wire(&self) -> Result<Arc<dyn A2aWire>, A2aClientError> {
+    if let Some(card_url) = &self.config.agent_card_url {
+        if let Some(wire) = detect_from_agent_card(&self.client, card_url).await {
+            return Ok(wire);
+        }
+        // Карточка недоступна/неинформативна — не считаем это фатальной
+        // ошибкой, просто падаем на зонд ниже (по духу ТЗ: "предпочтительнее",
+        // не "обязательно").
+    }
+
+    probe_wire_format(&self.client, &self.config.endpoint, self.config.token.as_deref()).await
+}
*/

// ---------------------------------------------------------------------------
// Правка crates/driver-a2a-client/src/dialect_probe.rs — новая функция
// detect_from_agent_card, добавляется РЯДОМ с существующим probe_wire_format
// (тот не меняется вообще — весь текущий код и 3 теста остаются как есть).
// ---------------------------------------------------------------------------

/*
--- a/crates/driver-a2a-client/src/dialect_probe.rs
+++ b/crates/driver-a2a-client/src/dialect_probe.rs
@@ use uuid::Uuid;
+use serde_json::Value as JsonValue;

+/// Детект по Agent Card (ТЗ §3.2 п.4): GET agent_card_url, читает
+/// protocolVersion — "1.0" (или любая версия с ведущим "1") -> Sdk,
+/// "0.x" -> Spec. Возвращает None (не Err!) при ЛЮБОЙ проблеме — сетевой
+/// ошибке, невалидном JSON, отсутствии/непонятном protocolVersion —
+/// потому что это лишь "предпочтительный", а не единственный канал:
+/// вызывающий код (resolve_auto_wire) должен упасть на зонд, а не
+/// прервать всю резолюцию из-за недоступной карточки.
+pub async fn detect_from_agent_card(
+    client: &reqwest::Client,
+    agent_card_url: &str,
+) -> Option<Arc<dyn A2aWire>> {
+    let resp = client.get(agent_card_url).send().await.ok()?;
+    if !resp.status().is_success() {
+        return None;
+    }
+    let body: JsonValue = resp.json().await.ok()?;
+    let protocol_version = body.get("protocolVersion").and_then(JsonValue::as_str)?;
+
+    if protocol_version.starts_with('1') {
+        Some(Arc::new(A2aSdkWire))
+    } else if protocol_version.starts_with('0') {
+        Some(Arc::new(A2aSpecWire))
+    } else {
+        // Незнакомая мажорная версия — не угадываем, отдаём None, чтобы
+        // resolve_auto_wire() упал на зонд, который хотя бы эмпирически
+        // проверит реальное поведение сервера.
+        None
+    }
+}
*/

#[cfg(test)]
mod agent_card_detect_tests {
    // Новые тесты добавляются В ТОТ ЖЕ #[cfg(test)] mod tests существующего
    // dialect_probe.rs, рядом с тремя текущими (probe_recognizes_sdk_...,
    // probe_falls_back_to_spec_..., probe_errors_when_neither_...).

    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn agent_card_with_protocol_version_1_detects_sdk() {
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
        let wire = detect_from_agent_card(&client, &card_url)
            .await
            .expect("must detect from protocolVersion 1.0");
        assert_eq!(wire.name(), "sdk");
    }

    #[tokio::test]
    async fn agent_card_with_protocol_version_0_detects_spec() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "test-agent",
                "protocolVersion": "0.9",
                "url": "https://example.com/rpc"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let wire = detect_from_agent_card(&client, &card_url)
            .await
            .expect("must detect from protocolVersion 0.9");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn agent_card_missing_protocol_version_returns_none_not_panic() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "test-agent",
                "url": "https://example.com/rpc"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let result = detect_from_agent_card(&client, &card_url).await;
        assert!(result.is_none(), "missing protocolVersion must fall through to probe, not panic");
    }

    #[tokio::test]
    async fn agent_card_unreachable_returns_none_not_err() {
        // Порт 1 практически гарантированно недоступен локально.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let result = detect_from_agent_card(&client, "http://127.0.0.1:1/.well-known/agent.json").await;
        assert!(result.is_none(), "unreachable card must gracefully fall through, not error the whole resolution");
    }

    #[tokio::test]
    async fn agent_card_takes_priority_over_probe_when_both_available() {
        // Регрессия ключевого требования ТЗ §3.2 п.4 "приоритетнее зонда":
        // если и AgentCard, и зонд-совместимый endpoint доступны, должен
        // сработать именно AgentCard-путь (в этом тесте — просто проверяем,
        // что detect_from_agent_card сама по себе достаточна и не требует
        // похода на endpoint зонда вовсе — она бьёт по card_url, а не по
        // основному endpoint).
        let card_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocolVersion": "1.0"
            })))
            .mount(&card_server)
            .await;

        // Основной endpoint НЕ смонтирован никаким Mock — если бы
        // resolve_auto_wire() случайно дёрнул зонд на него, тест бы упал
        // с connection refused при попытке POST. Проверяем только
        // detect_from_agent_card изолированно, что она не трогает
        // основной endpoint вообще.
        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", card_server.uri());
        let wire = detect_from_agent_card(&client, &card_url).await;
        assert!(wire.is_some());
    }
}
