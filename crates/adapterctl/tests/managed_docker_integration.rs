//! crates/adapterctl/tests/managed_docker_integration.rs — интеграционные
//! тесты против реального Docker daemon. Адаптировано из
//! adapterctl_integration_test_fixed.rs: убран testcontainers (конфликт
//! версий bollard-stubs), тест использует preflight_check() как единственный
//! источник истины для "Docker готов?".
//!
//! Запуск: `cargo test -p adapterctl -- --ignored` (требует Docker daemon,
//! Linux containers mode).

use adapterctl::managed_docker;

#[tokio::test]
#[ignore = "requires Docker daemon reachable (Linux containers mode) — run with `cargo test -- --ignored`"]
async fn managed_docker_full_lifecycle_against_real_daemon() {
    managed_docker::preflight_check()
        .await
        .expect("Docker preflight failed — start Docker Desktop / switch to Linux containers");

    let plan = managed_docker::plan(true, true, "TEST_ADAPTER_CONNECTOR_PG_DSN")
        .expect("plan() with confirm_docker=true must succeed");

    managed_docker::ensure_running(&plan)
        .await
        .expect("ensure_running() must succeed against a real reachable Docker daemon");

    managed_docker::ensure_running(&plan)
        .await
        .expect("second ensure_running() call must be idempotent, not fail on 'already exists'");

    managed_docker::remove_all_resources(true).await
        .expect("cleanup after integration test must succeed — leaked resources would affect subsequent test runs");
}

#[tokio::test]
#[ignore = "requires Docker daemon reachable — run with `cargo test -- --ignored`"]
async fn managed_docker_rejects_foreign_resource_with_same_name() {
    managed_docker::preflight_check()
        .await
        .expect("Docker preflight failed");

    // Создаём "чужой" контейнер напрямую через bollard, БЕЗ ownership label,
    // имитируя ресурс, существовавший до установки agent-connector.
    let docker = bollard::Docker::connect_with_local_defaults()
        .expect("raw bollard connect for test setup must succeed");

    use bollard::container::{Config as ContainerConfig, CreateContainerOptions};
    let foreign_config = ContainerConfig {
        image: Some("postgres:16.4-alpine".to_string()),
        env: Some(vec![
            "POSTGRES_PASSWORD=irrelevant-for-this-test".to_string()
        ]),
        // Явно НЕ ставим labels с ownership label — ключевая часть теста.
        ..Default::default()
    };
    docker
        .create_container(
            Some(CreateContainerOptions {
                name: "agent-connector-pg",
                platform: None,
            }),
            foreign_config,
        )
        .await
        .expect("test setup: creating the foreign container must succeed");

    let plan = managed_docker::plan(true, true, "TEST_ADAPTER_CONNECTOR_PG_DSN")
        .expect("plan() must succeed");

    let result = managed_docker::ensure_running(&plan).await;

    // Cleanup ПЕРЕД assert — если assert паникует, контейнер всё равно
    // удалён (прямой bollard remove, т.к. managed_docker::remove_all_resources
    // САМА откажется трогать чужой контейнер по той же причине, что тестируем).
    let cleanup_result = docker
        .remove_container(
            "agent-connector-pg",
            Some(bollard::container::RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await;

    match result {
        Err(managed_docker::DockerError::NotOurResource(name)) => {
            assert_eq!(name, "agent-connector-pg");
        }
        other => panic!("expected DockerError::NotOurResource, got: {other:?}"),
    }

    cleanup_result.expect("test cleanup: removing the foreign container must succeed");
}
