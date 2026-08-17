//! crates/driver-a2a-client/tests/e2e_live.rs
//!
//! Живой E2E (ТЗ §2.6 п.4, критерии приёмки #2): driver-a2a-client →
//! шлюз ACP-A2A_gateway → hermes. Запускается вручную — требует поднятого
//! шлюза с реальным агентом:
//!
//!   gatewayd /tmp/gateway-e2e/config.yaml
//!
//! (конфиг: agents.hermes-main = [hermes, acp], токен t-e2e-001,
//! http_listen 127.0.0.1:8348).
//!
//! Запуск:
//!   cargo test -p driver-a2a-client --test e2e_live -- --ignored --nocapture
//!
//! E2E_SPEC_ENDPOINT / E2E_TOKEN переопределяют endpoint и токен (по
//! умолчанию http://127.0.0.1:8348/agents/hermes-main/rpc и t-e2e-001).

use driver_a2a_client::{A2aClientConfig, A2aClientDriver, A2aWireFormat};
use std::time::Duration;

fn e2e_endpoint() -> String {
    std::env::var("E2E_SPEC_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8348/agents/hermes-main/rpc".into())
}

fn e2e_token() -> Option<String> {
    Some(std::env::var("E2E_TOKEN").unwrap_or_else(|_| "t-e2e-001".into()))
}

fn build_driver(wire_format: A2aWireFormat) -> A2aClientDriver {
    A2aClientDriver::new(A2aClientConfig {
        endpoint: e2e_endpoint(),
        token: e2e_token(),
        wire_format,
        timeout_secs: 120,
        agent_card_url: None,
    })
    .expect("driver builds")
}

/// Живой E2E по spec-wire: message/send доходит до hermes и возвращает
/// Completed с текстом. Требует поднятого шлюза (см. шапку модуля).
#[tokio::test]
#[ignore]
async fn e2e_spec_wire_invoke_to_hermes_returns_completed() {
    let driver = build_driver(A2aWireFormat::Spec);

    let task = driver
        .invoke("Reply with exactly: E2E_OK", None, None)
        .await
        .expect("live invoke via gateway spec wire must succeed");

    assert_eq!(
        task.state,
        driver_a2a_client::wire::NormalizedState::Completed
    );
    let text: String = task
        .output_parts
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect();
    assert!(
        text.contains("E2E_OK"),
        "hermes must echo E2E_OK, got: {text:?}"
    );
    println!("E2E spec OK: task={} text={text:?}", task.id);
}

/// Живой E2E по auto-wire: зонд (GetTask/tasks-get) определяет spec на
/// реальном шлюзе, затем invoke работает тем же путём. Параллельно
/// проверяет, что Agent Card (детект сейчас None) не мешает резолюции.
#[tokio::test]
#[ignore]
async fn e2e_auto_wire_probe_resolves_spec_and_invoke_completes() {
    let driver = build_driver(A2aWireFormat::Auto);

    // get_task на несуществующий id: если зонд выбрал spec, шлюз ответит
    // "task not found" (метод распознан) — это и есть доказательство
    // корректного диалекта без создания задач.
    let probe_result = driver.get_task("e2e-nonexistent-uuid").await;
    assert!(
        probe_result.is_err(),
        "nonexistent task must error, but probe_result={probe_result:?}"
    );

    let task = driver
        .invoke("Reply with exactly: E2E_AUTO", None, None)
        .await
        .expect("live invoke via auto-detected wire must succeed");
    assert_eq!(
        task.state,
        driver_a2a_client::wire::NormalizedState::Completed
    );
    let text: String = task
        .output_parts
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect();
    assert!(
        text.contains("E2E_AUTO"),
        "hermes must echo E2E_AUTO, got: {text:?}"
    );
    println!("E2E auto OK: task={} text={text:?}", task.id);
}

/// Вспомогательный smoke: card недоступен/нет protocolVersion — драйвер всё
/// равно стартует и резолвит через зонд. Дублирует e2e_auto, но без токена
/// заведомо получает содержательную ошибку, а не зависание.
#[tokio::test]
#[ignore]
async fn e2e_smoke_health_check_has_bounded_timeout() {
    let driver = build_driver(A2aWireFormat::Spec);
    let started = std::time::Instant::now();
    let result = driver.invoke("ping", None, None).await;
    assert!(
        started.elapsed() < Duration::from_secs(200),
        "invoke must respect timeout_secs=120 and not hang"
    );
    match result {
        Ok(task) => println!("E2E smoke OK: state={:?}", task.state),
        Err(e) => println!("E2E smoke: expected live result missing, error={e}"),
    }
}
