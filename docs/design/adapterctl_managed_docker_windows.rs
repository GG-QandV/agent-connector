//! crates/adapterctl/src/managed_docker.rs — реализация ensure_running()
//! через bollard, подключенный на Windows через named pipe. Linux/macOS
//! используют тот же код с unix-socket endpoint (bollard::Docker::connect_with_local_defaults()),
//! разница инкапсулирована в выборе connect_with_*, остальная логика общая.
//!
//! Контракт с adapterd (подтверждён в config-2.rs/main.rs):
//!   StorageConfig::Postgres { dsn_env: String, schema, max_connections }
//!   buildstore() делает `env::var(dsn_env)` — installer НЕ пишет DSN в
//!   adapter.yaml напрямую, только имя переменной; сам DSN уходит в .env
//!   под этим именем. Секрет никогда не попадает в YAML-файл, который
//!   мог бы случайно закоммититься/отобразиться в конфиг-дампах.

use bollard::container::{Config as ContainerConfig, CreateContainerOptions, StartContainerOptions};
use bollard::models::{HostConfig, PortBinding};
use bollard::network::CreateNetworkOptions;
use bollard::volume::CreateVolumeOptions;
use bollard::Docker;
use std::collections::HashMap;

const POSTGRES_IMAGE_TAG: &str = "postgres:16.4-alpine";
const CONTAINER_NAME: &str = "agent-connector-pg";
const VOLUME_NAME: &str = "agent-connector-pg-data";
const NETWORK_NAME: &str = "agent-connector-internal";
/// Label на всех ресурсах, созданных этим installer'ом — используется для
/// безопасной идентификации "наших" ресурсов при uninstall/upgrade, вместо
/// сопоставления только по имени (имя тоже уникально, но label — вторая,
/// независимая проверка перед любой mutating операцией на чужом Docker хосте).
const OWNERSHIP_LABEL: (&str, &str) = ("io.agent-connector.managed", "true");

pub struct ManagedPostgresPlan {
    pub dsn: String,
    pub dsn_env_var_name: String, // то самое имя, что пойдёт в adapter.yaml как dsn_env
    pub host_port: Option<u16>,
    pub password: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DockerError {
    #[error("failed to connect to Docker daemon: {0}")]
    Connect(String),
    #[error("Docker daemon is not reachable — is Docker Desktop running? (Windows: check the whale icon in the system tray)")]
    NotReachable,
    #[error("Docker is in Windows containers mode, but a Linux-based Postgres image is required. \
             Switch Docker Desktop to Linux containers (right-click tray icon -> 'Switch to Linux containers...').")]
    WrongContainerMode,
    #[error("docker operation failed: {0}")]
    Operation(String),
    #[error("refusing to touch resource '{0}': it exists but is missing the {}={} ownership label — \
             it was not created by this installer and will not be modified or removed", OWNERSHIP_LABEL.0, OWNERSHIP_LABEL.1)]
    NotOurResource(String),
}

/// Платформенное подключение к Docker daemon.
/// Windows: named pipe (Docker Desktop WSL2 backend или native Engine).
/// Linux/macOS: unix socket через connect_with_local_defaults().
#[cfg(target_os = "windows")]
fn connect() -> Result<Docker, DockerError> {
    Docker::connect_with_named_pipe(
        "npipe:////./pipe/docker_engine",
        120, // timeout секунд — pull образа может занять время на первом запуске
        bollard::API_DEFAULT_VERSION,
    )
    .map_err(|e| DockerError::Connect(e.to_string()))
}

#[cfg(not(target_os = "windows"))]
fn connect() -> Result<Docker, DockerError> {
    Docker::connect_with_local_defaults().map_err(|e| DockerError::Connect(e.to_string()))
}

pub async fn ping(docker: &Docker) -> Result<(), DockerError> {
    docker.ping().await.map(|_| ()).map_err(|_| DockerError::NotReachable)
}

#[cfg(target_os = "windows")]
pub async fn assert_linux_containers_mode(docker: &Docker) -> Result<(), DockerError> {
    let info = docker.info().await.map_err(|e| DockerError::Operation(e.to_string()))?;
    match info.os_type.as_deref() {
        Some("linux") => Ok(()),
        _ => Err(DockerError::WrongContainerMode),
    }
}
#[cfg(not(target_os = "windows"))]
pub async fn assert_linux_containers_mode(_docker: &Docker) -> Result<(), DockerError> {
    Ok(()) // Linux/macOS Docker всегда Linux containers, проверка не нужна
}

/// Формирует план (DSN, имена ресурсов, порт) без обращения к Docker —
/// чистая функция, легко тестируемая без реального daemon.
pub fn plan(confirm_docker: bool, network_only: bool, agent_config_dsn_env_name: &str) -> Result<ManagedPostgresPlan, String> {
    if !confirm_docker {
        return Err(
            "managed-docker-postgres requires --confirm-docker (installer will not \
             install, start, or pull images for Docker containers without explicit confirmation)".into()
        );
    }
    let password = super::generate_secure_password();
    let user = "adapter_connector";
    let db = "agent_connector";
    let (host, host_port) = if network_only {
        (CONTAINER_NAME.to_string(), None)
    } else {
        ("127.0.0.1".to_string(), Some(5432u16))
    };
    let port_for_dsn = host_port.unwrap_or(5432);
    let dsn = format!("postgres://{user}:{password}@{host}:{port_for_dsn}/{db}");
    Ok(ManagedPostgresPlan {
        dsn,
        dsn_env_var_name: agent_config_dsn_env_name.to_string(),
        host_port,
        password,
    })
}

/// Создаёт (если не существует) network/volume/container с проверкой
/// ownership label на каждом шаге — если ресурс с нужным именем уже
/// существует, но БЕЗ нашего label, операция останавливается с
/// DockerError::NotOurResource вместо того чтобы молча его переиспользовать
/// или, хуже, пересоздать/удалить.
pub async fn ensure_running(plan: &ManagedPostgresPlan) -> Result<(), DockerError> {
    let docker = connect()?;
    ping(&docker).await?;
    assert_linux_containers_mode(&docker).await?;

    ensure_network(&docker).await?;
    ensure_volume(&docker).await?;
    ensure_container(&docker, plan).await?;
    wait_for_ready(&docker).await?;

    Ok(())
}

async fn ensure_network(docker: &Docker) -> Result<(), DockerError> {
    let networks = docker.list_networks::<String>(None).await
        .map_err(|e| DockerError::Operation(e.to_string()))?;

    if let Some(existing) = networks.iter().find(|n| n.name.as_deref() == Some(NETWORK_NAME)) {
        let has_label = existing.labels.as_ref()
            .is_some_and(|l| l.get(OWNERSHIP_LABEL.0).map(String::as_str) == Some(OWNERSHIP_LABEL.1));
        if !has_label {
            return Err(DockerError::NotOurResource(NETWORK_NAME.to_string()));
        }
        return Ok(()); // уже существует, наш — ничего не делаем
    }

    let mut labels = HashMap::new();
    labels.insert(OWNERSHIP_LABEL.0.to_string(), OWNERSHIP_LABEL.1.to_string());

    docker.create_network(CreateNetworkOptions {
        name: NETWORK_NAME,
        driver: "bridge",
        labels,
        ..Default::default()
    }).await.map_err(|e| DockerError::Operation(format!("create_network: {e}")))?;

    Ok(())
}

async fn ensure_volume(docker: &Docker) -> Result<(), DockerError> {
    match docker.inspect_volume(VOLUME_NAME).await {
        Ok(existing) => {
            let has_label = existing.labels.get(OWNERSHIP_LABEL.0).map(String::as_str) == Some(OWNERSHIP_LABEL.1);
            if !has_label {
                return Err(DockerError::NotOurResource(VOLUME_NAME.to_string()));
            }
            Ok(())
        }
        Err(_) => {
            let mut labels = HashMap::new();
            labels.insert(OWNERSHIP_LABEL.0.to_string(), OWNERSHIP_LABEL.1.to_string());
            docker.create_volume(CreateVolumeOptions {
                name: VOLUME_NAME,
                driver: "local",
                labels,
                ..Default::default()
            }).await.map_err(|e| DockerError::Operation(format!("create_volume: {e}")))?;
            Ok(())
        }
    }
}

async fn ensure_container(docker: &Docker, plan: &ManagedPostgresPlan) -> Result<(), DockerError> {
    match docker.inspect_container(CONTAINER_NAME, None).await {
        Ok(existing) => {
            let has_label = existing.config.as_ref()
                .and_then(|c| c.labels.as_ref())
                .is_some_and(|l| l.get(OWNERSHIP_LABEL.0).map(String::as_str) == Some(OWNERSHIP_LABEL.1));
            if !has_label {
                return Err(DockerError::NotOurResource(CONTAINER_NAME.to_string()));
            }
            let is_running = existing.state.as_ref().and_then(|s| s.running).unwrap_or(false);
            if !is_running {
                docker.start_container(CONTAINER_NAME, None::<StartContainerOptions<String>>).await
                    .map_err(|e| DockerError::Operation(format!("start_container: {e}")))?;
            }
            return Ok(());
        }
        Err(_) => { /* не существует — создаём ниже */ }
    }

    pull_image_if_missing(docker).await?;

    let mut labels = HashMap::new();
    labels.insert(OWNERSHIP_LABEL.0.to_string(), OWNERSHIP_LABEL.1.to_string());

    let port_bindings = plan.host_port.map(|port| {
        let mut map = HashMap::new();
        map.insert(
            "5432/tcp".to_string(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()), // правило #3: bind localhost, не 0.0.0.0
                host_port: Some(port.to_string()),
            }]),
        );
        map
    });

    let host_config = HostConfig {
        binds: Some(vec![format!("{VOLUME_NAME}:/var/lib/postgresql/data")]),
        network_mode: Some(NETWORK_NAME.to_string()),
        port_bindings,
        restart_policy: Some(bollard::models::RestartPolicy {
            name: Some(bollard::models::RestartPolicyNameEnum::UNLESS_STOPPED),
            ..Default::default()
        }),
        ..Default::default()
    };

    let env = vec![
        "POSTGRES_USER=adapter_connector".to_string(),
        format!("POSTGRES_PASSWORD={}", plan.password),
        "POSTGRES_DB=agent_connector".to_string(),
    ];

    let config = ContainerConfig {
        image: Some(POSTGRES_IMAGE_TAG.to_string()),
        env: Some(env),
        labels: Some(labels),
        host_config: Some(host_config),
        ..Default::default()
    };

    docker.create_container(
        Some(CreateContainerOptions { name: CONTAINER_NAME, platform: None }),
        config,
    ).await.map_err(|e| DockerError::Operation(format!("create_container: {e}")))?;

    docker.start_container(CONTAINER_NAME, None::<StartContainerOptions<String>>).await
        .map_err(|e| DockerError::Operation(format!("start_container: {e}")))?;

    Ok(())
}

async fn pull_image_if_missing(docker: &Docker) -> Result<(), DockerError> {
    use bollard::image::CreateImageOptions;
    use futures_util::StreamExt;

    let images = docker.list_images::<String>(None).await
        .map_err(|e| DockerError::Operation(e.to_string()))?;
    let already_present = images.iter().any(|img| {
        img.repo_tags.iter().any(|tag| tag == POSTGRES_IMAGE_TAG)
    });
    if already_present {
        return Ok(());
    }

    let mut stream = docker.create_image(
        Some(CreateImageOptions { from_image: POSTGRES_IMAGE_TAG, ..Default::default() }),
        None,
        None,
    );
    while let Some(progress) = stream.next().await {
        progress.map_err(|e| DockerError::Operation(format!("image pull failed: {e}")))?;
    }
    Ok(())
}

/// Ждёт, пока Postgres реально принимает соединения (не только "контейнер
/// running" — Postgres может ещё инициализировать data directory). Опрашивает
/// через docker exec pg_isready внутри контейнера, не через прямое TCP —
/// работает одинаково независимо от того, опубликован ли host_port.
async fn wait_for_ready(docker: &Docker) -> Result<(), DockerError> {
    use bollard::exec::{CreateExecOptions, StartExecResults};

    for attempt in 1..=30 {
        let exec = docker.create_exec(CONTAINER_NAME, CreateExecOptions {
            cmd: Some(vec!["pg_isready", "-U", "adapter_connector"]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        }).await.map_err(|e| DockerError::Operation(format!("create_exec: {e}")))?;

        if let Ok(StartExecResults::Attached { .. }) = docker.start_exec(&exec.id, None).await {
            let inspect = docker.inspect_exec(&exec.id).await
                .map_err(|e| DockerError::Operation(e.to_string()))?;
            if inspect.exit_code == Some(0) {
                return Ok(());
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500 * attempt.min(10))).await;
    }

    Err(DockerError::Operation(
        "Postgres container started but did not become ready within timeout (pg_isready never succeeded)".into()
    ))
}

/// Обратная операция для uninstall --purge-data / upgrade-postgres.
/// Требует явного подтверждения ownership label перед любым удалением —
/// то же правило #9, зеркально применённое к destructive-пути.
pub async fn remove_all_resources(remove_volume: bool) -> Result<(), DockerError> {
    let docker = connect()?;

    if let Ok(existing) = docker.inspect_container(CONTAINER_NAME, None).await {
        let has_label = existing.config.as_ref()
            .and_then(|c| c.labels.as_ref())
            .is_some_and(|l| l.get(OWNERSHIP_LABEL.0).map(String::as_str) == Some(OWNERSHIP_LABEL.1));
        if !has_label {
            return Err(DockerError::NotOurResource(CONTAINER_NAME.to_string()));
        }
        docker.stop_container(CONTAINER_NAME, None).await.ok();
        docker.remove_container(CONTAINER_NAME, None).await
            .map_err(|e| DockerError::Operation(format!("remove_container: {e}")))?;
    }

    if remove_volume {
        if let Ok(existing) = docker.inspect_volume(VOLUME_NAME).await {
            let has_label = existing.labels.get(OWNERSHIP_LABEL.0).map(String::as_str) == Some(OWNERSHIP_LABEL.1);
            if !has_label {
                return Err(DockerError::NotOurResource(VOLUME_NAME.to_string()));
            }
            docker.remove_volume(VOLUME_NAME, None).await
                .map_err(|e| DockerError::Operation(format!("remove_volume: {e}")))?;
        }
    }

    Ok(())
}

pub fn image_tag() -> &'static str { POSTGRES_IMAGE_TAG }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_rejects_without_confirm_docker() {
        let result = plan(false, false, "AGENT_CONNECTOR_PG_DSN");
        assert!(result.is_err(), "must reject managed-docker-postgres without explicit confirmation");
    }

    #[test]
    fn plan_network_only_has_no_host_port() {
        let plan = plan(true, true, "AGENT_CONNECTOR_PG_DSN").unwrap();
        assert!(plan.host_port.is_none(), "--docker-network-only must not expose a host port");
        assert!(plan.dsn.contains(CONTAINER_NAME), "DSN host must be the container name when network-only");
    }

    #[test]
    fn plan_default_binds_localhost_port_5432() {
        let plan = plan(true, false, "AGENT_CONNECTOR_PG_DSN").unwrap();
        assert_eq!(plan.host_port, Some(5432));
        assert!(plan.dsn.contains("127.0.0.1:5432"), "default DSN must target localhost, not 0.0.0.0");
    }

    #[test]
    fn plan_password_is_not_the_old_static_default() {
        let plan = plan(true, false, "AGENT_CONNECTOR_PG_DSN").unwrap();
        // Regression guard against reverting to the old docker-compose.postgres-2.yaml
        // default creds (adapter/adapter) — rule #4.
        assert_ne!(plan.password, "adapter");
        assert!(plan.password.len() >= 32, "generated password must be substantial, not a short default");
    }

    #[test]
    fn image_tag_is_pinned_not_latest() {
        assert_ne!(image_tag(), "latest");
        assert!(image_tag().contains(':'), "image tag must be explicit, e.g. postgres:16.4-alpine");
    }
}
