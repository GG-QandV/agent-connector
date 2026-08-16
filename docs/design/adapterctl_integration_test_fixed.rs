//! crates/adapterctl/tests/managed_docker_integration.rs — ИСПРАВЛЕНО:
//! ссылается на реально существующую managed_docker::preflight_check()
//! (написана в предыдущем шаге), не на воображаемую функцию.
//!
//! Размещение и порядок запуска на macOS — без изменений относительно
//! предыдущей версии этого файла (tests/ на уровне крейта, #[ignore] по
//! умолчанию, testcontainers как dev-dependency).

use adapterctl::managed_docker;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres as TestPostgres;

#[tokio::test]
#[ignore = "requires Docker daemon reachable (Linux containers mode) — run with `cargo test -- --ignored`"]
async fn managed_docker_full_lifecycle_against_real_daemon() {
    // Реальная функция, теперь используемая и продом (install_flow), и
    // этим тестом — единственный источник истины для "Docker готов?",
    // не отдельная тестовая копия проверки.
    managed_docker::preflight_check().await
        .expect("Docker preflight failed — start Docker Desktop / switch to Linux containers");

    let _reference_container = TestPostgres::default()
        .start()
        .await
        .expect("testcontainers Postgres must start — confirms Docker daemon itself is healthy independent of our own bollard code");

    let plan = managed_docker::plan(true, true, "TEST_ADAPTER_CONNECTOR_PG_DSN")
        .expect("plan() with confirm_docker=true must succeed");

    managed_docker::ensure_running(&plan).await
        .expect("ensure_running() must succeed against a real reachable Docker daemon");

    managed_docker::ensure_running(&plan).await
        .expect("second ensure_running() call must be idempotent, not fail on 'already exists'");

    managed_docker::remove_all_resources(true).await
        .expect("cleanup after integration test must succeed — leaked resources would affect subsequent test runs");
}

#[tokio::test]
#[ignore = "requires Docker daemon reachable — run with `cargo test -- --ignored`"]
async fn managed_docker_rejects_foreign_resource_with_same_name() {
    managed_docker::preflight_check().await
        .expect("Docker preflight failed");

    // Создаём "чужой" контейнер напрямую через bollard, БЕЗ ownership
    // label, имитируя ресурс, который existовал до установки agent-connector
    // (например, пользователь уже запускал Postgres под этим именем сам).
    let docker = bollard::Docker::connect_with_local_defaults()
        .expect("raw bollard connect for test setup must succeed");

    use bollard::container::{Config as ContainerConfig, CreateContainerOptions};
    let foreign_config = ContainerConfig {
        image: Some("postgres:16.4-alpine".to_string()),
        env: Some(vec![
            "POSTGRES_PASSWORD=irrelevant-for-this-test".to_string(),
        ]),
        // Явно НЕ ставим labels с OWNERSHIP_LABEL — это ключевая часть теста.
        ..Default::default()
    };
    docker.create_container(
        Some(CreateContainerOptions { name: "agent-connector-pg", platform: None }),
        foreign_config,
    ).await.expect("test setup: creating the foreign container must succeed");

    let plan = managed_docker::plan(true, true, "TEST_ADAPTER_CONNECTOR_PG_DSN")
        .expect("plan() must succeed");

    let result = managed_docker::ensure_running(&plan).await;

    // Cleanup ПЕРЕД assert — если assert паникует, контейнер всё равно
    // должен быть удалён вручную (не через managed_docker::remove_all_resources,
    // которая САМА откажется трогать этот контейнер по той же причине,
    // что мы тестируем — поэтому здесь прямой bollard remove).
    let cleanup_result = docker.remove_container(
        "agent-connector-pg",
        Some(bollard::container::RemoveContainerOptions { force: true, ..Default::default() }),
    ).await;

    match result {
        Err(managed_docker::DockerError::NotOurResource(name)) => {
            assert_eq!(name, "agent-connector-pg");
        }
        other => panic!("expected DockerError::NotOurResource, got: {other:?}"),
    }

    cleanup_result.expect("test cleanup: removing the foreign container must succeed");
}
