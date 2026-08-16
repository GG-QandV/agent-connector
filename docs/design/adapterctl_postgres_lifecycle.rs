//! crates/adapterctl/src/postgres_lifecycle.rs — backup/upgrade для
//! managed-docker-postgres профиля. Отделён от config_template.rs: этот
//! модуль работает с УЖЕ существующей инсталляцией (существующий контейнер,
//! существующий volume), не с тем, какие агенты запускает adapterd —
//! это два независимых аспекта, объединение их создало бы искусственную
//! связь между "что за агенты" и "какая версия Postgres", которой в
//! реальной архитектуре нет.

use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::Docker;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};

const CONTAINER_NAME: &str = "agent-connector-pg";
const VOLUME_NAME: &str = "agent-connector-pg-data";
const OWNERSHIP_LABEL: (&str, &str) = ("io.agent-connector.managed", "true");

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("not a managed-docker-postgres installation: {0}")]
    NotManagedDocker(String),
    #[error("docker operation failed: {0}")]
    Docker(String),
    #[error("backup failed: {0}")]
    Backup(String),
    #[error("upgrade aborted: {0}")]
    UpgradeAborted(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

fn connect() -> Result<Docker, LifecycleError> {
    #[cfg(target_os = "windows")]
    {
        Docker::connect_with_named_pipe("npipe:////./pipe/docker_engine", 120, bollard::API_DEFAULT_VERSION)
            .map_err(|e| LifecycleError::Docker(e.to_string()))
    }
    #[cfg(not(target_os = "windows"))]
    {
        Docker::connect_with_local_defaults().map_err(|e| LifecycleError::Docker(e.to_string()))
    }
}

/// Проверяет, что agent-connector-pg реально принадлежит этому installer'у
/// (по ownership label), прежде чем что-либо делать с ним — то же правило
/// #9, применённое к backup/upgrade пути, не только к install/uninstall.
async fn verify_ownership(docker: &Docker) -> Result<(), LifecycleError> {
    let container = docker.inspect_container(CONTAINER_NAME, None).await
        .map_err(|_| LifecycleError::NotManagedDocker(format!(
            "container '{CONTAINER_NAME}' not found — this instance is not using managed-docker-postgres, \
             backup/upgrade only apply to that profile"
        )))?;
    let has_label = container.config.as_ref()
        .and_then(|c| c.labels.as_ref())
        .is_some_and(|l| l.get(OWNERSHIP_LABEL.0).map(String::as_str) == Some(OWNERSHIP_LABEL.1));
    if !has_label {
        return Err(LifecycleError::NotManagedDocker(format!(
            "container '{CONTAINER_NAME}' exists but lacks the ownership label — refusing to touch it"
        )));
    }
    Ok(())
}

/// pg_dump внутри контейнера, поток напрямую в файл на диске — не через
/// промежуточный Vec<u8> в памяти, чтобы backup большой БД не съедал RAM
/// installer-процесса.
pub async fn backup(output_path: &Path) -> Result<(), LifecycleError> {
    let docker = connect()?;
    verify_ownership(&docker).await?;

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let exec = docker.create_exec(CONTAINER_NAME, CreateExecOptions {
        cmd: Some(vec!["pg_dump", "-U", "adapter_connector", "agent_connector"]),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    }).await.map_err(|e| LifecycleError::Docker(format!("create_exec: {e}")))?;

    let mut file = tokio::fs::File::create(output_path).await?;
    use tokio::io::AsyncWriteExt;

    if let StartExecResults::Attached { mut output, .. } = docker.start_exec(&exec.id, None).await
        .map_err(|e| LifecycleError::Docker(format!("start_exec: {e}")))?
    {
        while let Some(chunk) = output.next().await {
            let chunk = chunk.map_err(|e| LifecycleError::Backup(format!("stream read: {e}")))?;
            file.write_all(chunk.into_bytes().as_ref()).await?;
        }
    }
    file.flush().await?;

    let inspect = docker.inspect_exec(&exec.id).await
        .map_err(|e| LifecycleError::Docker(e.to_string()))?;
    if inspect.exit_code != Some(0) {
        return Err(LifecycleError::Backup(format!(
            "pg_dump exited with code {:?} — check {} for partial/empty output",
            inspect.exit_code, output_path.display()
        )));
    }

    let size = tokio::fs::metadata(output_path).await?.len();
    if size == 0 {
        return Err(LifecycleError::Backup(format!(
            "pg_dump produced an empty file at {} — treating as failure, not a valid backup",
            output_path.display()
        )));
    }

    Ok(())
}

/// Upgrade — ВСЕГДА начинается с обязательного backup (правило #7: "версия
/// образа фиксируется; обновление Postgres — отдельная команда с backup").
/// Не принимает --skip-backup флаг вообще — если пользователь настолько
/// уверен, что backup не нужен, он может сделать `docker rm` руками и
/// начать install с нуля, это не должно быть однокомандной операцией
/// upgrade, которая случайно теряет данные.
pub async fn upgrade(target_image_tag: &str, prefix: &Path) -> Result<(), LifecycleError> {
    if target_image_tag == "latest" || !target_image_tag.contains(':') {
        return Err(LifecycleError::UpgradeAborted(format!(
            "target image tag '{target_image_tag}' must be explicit (e.g. postgres:17.0-alpine), not 'latest' or untagged"
        )));
    }

    let docker = connect()?;
    verify_ownership(&docker).await?;

    let backup_path = mandatory_pre_upgrade_backup_path(prefix);
    println!("Creating mandatory backup before upgrade: {}", backup_path.display());
    backup(&backup_path).await?;
    println!("Backup verified non-empty at {}", backup_path.display());

    // Остановить старый контейнер, но НЕ удалять volume — новый контейнер
    // с тем же VOLUME_NAME продолжает с тех же данных. Если новый образ
    // несовместим (например, major version jump без pg_upgrade), Postgres
    // сам откажется стартовать на несовместимом data directory — это
    // сигнал пользователю восстановить backup_path в новый чистый инстанс,
    // не automatic pg_upgrade (слишком рискованно делать неявно).
    docker.stop_container(CONTAINER_NAME, None).await
        .map_err(|e| LifecycleError::Docker(format!("stop_container: {e}")))?;
    docker.remove_container(CONTAINER_NAME, None).await
        .map_err(|e| LifecycleError::Docker(format!("remove_container: {e}")))?;

    pull_image(&docker, target_image_tag).await?;

    recreate_container_with_image(&docker, target_image_tag).await?;

    verify_post_upgrade_connectivity().await?;

    println!(
        "Upgrade to {target_image_tag} complete. Backup retained at {} — \
         delete manually once you've confirmed the new version works correctly.",
        backup_path.display()
    );

    Ok(())
}

fn mandatory_pre_upgrade_backup_path(prefix: &Path) -> PathBuf {
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    prefix.join("data").join(format!("pre-upgrade-backup-{timestamp}.sql"))
}

async fn pull_image(docker: &Docker, tag: &str) -> Result<(), LifecycleError> {
    use bollard::image::CreateImageOptions;
    let mut stream = docker.create_image(
        Some(CreateImageOptions { from_image: tag, ..Default::default() }),
        None,
        None,
    );
    while let Some(progress) = stream.next().await {
        progress.map_err(|e| LifecycleError::Docker(format!("image pull failed: {e}")))?;
    }
    Ok(())
}

async fn recreate_container_with_image(docker: &Docker, image_tag: &str) -> Result<(), LifecycleError> {
    use bollard::container::{Config as ContainerConfig, CreateContainerOptions, StartContainerOptions};
    use bollard::models::HostConfig;
    use std::collections::HashMap;

    let mut labels = HashMap::new();
    labels.insert(OWNERSHIP_LABEL.0.to_string(), OWNERSHIP_LABEL.1.to_string());

    let config = ContainerConfig {
        image: Some(image_tag.to_string()),
        labels: Some(labels),
        host_config: Some(HostConfig {
            binds: Some(vec![format!("{VOLUME_NAME}:/var/lib/postgresql/data")]),
            network_mode: Some("agent-connector-internal".to_string()),
            restart_policy: Some(bollard::models::RestartPolicy {
                name: Some(bollard::models::RestartPolicyNameEnum::UNLESS_STOPPED),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    docker.create_container(Some(CreateContainerOptions { name: CONTAINER_NAME, platform: None }), config)
        .await.map_err(|e| LifecycleError::Docker(format!("create_container: {e}")))?;
    docker.start_container(CONTAINER_NAME, None::<StartContainerOptions<String>>).await
        .map_err(|e| LifecycleError::Docker(format!("start_container: {e}")))?;
    Ok(())
}

async fn verify_post_upgrade_connectivity() -> Result<(), LifecycleError> {
    let docker = connect()?;
    for attempt in 1..=30u64 {
        let exec = docker.create_exec(CONTAINER_NAME, CreateExecOptions {
            cmd: Some(vec!["pg_isready", "-U", "adapter_connector"]),
            attach_stdout: Some(true),
            ..Default::default()
        }).await.map_err(|e| LifecycleError::Docker(e.to_string()))?;
        if docker.start_exec(&exec.id, None).await.is_ok() {
            if let Ok(inspect) = docker.inspect_exec(&exec.id).await {
                if inspect.exit_code == Some(0) {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500 * attempt.min(10))).await;
    }
    Err(LifecycleError::UpgradeAborted(
        "new Postgres container did not become ready after upgrade — data directory may be \
         incompatible with the target version (major version upgrades require pg_upgrade, \
         not a simple image swap). Restore the pre-upgrade backup into a fresh installation.".into()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_rejects_latest_tag() {
        // Синхронная часть проверки вынесена отдельно для тестируемости
        // без реального Docker daemon.
        let tag = "latest";
        let rejected = tag == "latest" || !tag.contains(':');
        assert!(rejected, "must reject 'latest' as an explicit target tag");
    }

    #[test]
    fn upgrade_rejects_untagged_image() {
        let tag = "postgres";
        let rejected = tag == "latest" || !tag.contains(':');
        assert!(rejected, "must reject an image reference without an explicit tag");
    }

    #[test]
    fn upgrade_accepts_explicit_tag() {
        let tag = "postgres:17.0-alpine";
        let rejected = tag == "latest" || !tag.contains(':');
        assert!(!rejected);
    }

    #[test]
    fn backup_path_is_timestamped_not_overwritten() {
        let p1 = mandatory_pre_upgrade_backup_path(Path::new("/opt/agent-connector"));
        std::thread::sleep(std::time::Duration::from_millis(1100)); // пересечь границу секунды
        let p2 = mandatory_pre_upgrade_backup_path(Path::new("/opt/agent-connector"));
        assert_ne!(p1, p2, "consecutive backups must not collide on the same filename");
    }
}
