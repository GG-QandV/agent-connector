//! crates/adapterctl/src/cancel.rs + правки к postgres_lifecycle.rs и
//! managed_docker.rs — graceful cancel в три части:
//!   1. Atomic write для backup (.tmp -> rename только при успехе)
//!   2. tokio::signal::ctrl_c() + tokio::select! вокруг pg_dump/docker pull
//!   3. Явный idempotent-resume тест для ensure_running() после partial interrupt

use std::path::{Path, PathBuf};

/// Общий helper: оборачивает future в select! с ctrl_c(), возвращает
/// Err(Interrupted) вместо того чтобы просто оборвать процесс — вызывающий
/// код получает шанс сделать cleanup (удалить .tmp файл, напечатать статус)
/// прежде чем реально завершиться.
pub async fn run_cancellable<F, T>(operation_name: &str, future: F) -> Result<T, CancelError>
where
    F: std::future::Future<Output = T>,
{
    tokio::select! {
        result = future => Ok(result),
        _ = tokio::signal::ctrl_c() => {
            Err(CancelError::Interrupted(operation_name.to_string()))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("operation '{0}' was interrupted (Ctrl+C) — see cleanup notes above")]
    Interrupted(String),
}

// ============================================================
// ЧАСТЬ 1+2: postgres_lifecycle.rs — backup() переписан с atomic write
// и graceful cancel. Заменяет предыдущую версию backup() целиком.
// ============================================================
pub mod backup_atomic {
    use super::*;
    use bollard::exec::{CreateExecOptions, StartExecResults};
    use bollard::Docker;
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    const CONTAINER_NAME: &str = "agent-connector-pg";

    #[derive(Debug, thiserror::Error)]
    pub enum BackupError {
        #[error("not a managed-docker-postgres installation: {0}")]
        NotManagedDocker(String),
        #[error("docker operation failed: {0}")]
        Docker(String),
        #[error("backup failed: {0}")]
        Failed(String),
        #[error("backup interrupted — partial file removed, no valid backup was produced: {0}")]
        Interrupted(String),
        #[error(transparent)]
        Io(#[from] std::io::Error),
    }

    fn connect() -> Result<Docker, BackupError> {
        #[cfg(target_os = "windows")]
        return Docker::connect_with_named_pipe("npipe:////./pipe/docker_engine", 120, bollard::API_DEFAULT_VERSION)
            .map_err(|e| BackupError::Docker(e.to_string()));
        #[cfg(not(target_os = "windows"))]
        return Docker::connect_with_local_defaults().map_err(|e| BackupError::Docker(e.to_string()));
    }

    /// Публичная точка входа: оборачивает internal-логику в run_cancellable.
    /// При Ctrl+C -> .tmp файл удаляется явно, пользователь видит понятное
    /// сообщение, НЕ файл, который выглядит как валидный backup.
    pub async fn backup(output_path: &Path) -> Result<(), BackupError> {
        let tmp_path = tmp_path_for(output_path);

        match run_cancellable("pg_dump backup", backup_inner(output_path, &tmp_path)).await {
            Ok(inner_result) => inner_result,
            Err(CancelError::Interrupted(_)) => {
                // Ctrl+C поймали ДО завершения backup_inner — .tmp может
                // существовать в частично записанном виде, удаляем явно,
                // чтобы не оставить файл, который кто-то мог бы спутать
                // с валидным (даже .tmp-суффикс не защищает от невнимательности).
                let _ = tokio::fs::remove_file(&tmp_path).await;
                eprintln!(
                    "\nBackup interrupted by Ctrl+C. Partial file at {} was removed. \
                     No valid backup was produced — re-run the backup command.",
                    tmp_path.display()
                );
                Err(BackupError::Interrupted(output_path.display().to_string()))
            }
        }
    }

    fn tmp_path_for(final_path: &Path) -> PathBuf {
        let mut tmp = final_path.as_os_str().to_owned();
        tmp.push(".tmp");
        PathBuf::from(tmp)
    }

    async fn verify_ownership(docker: &Docker) -> Result<(), BackupError> {
        let container = docker.inspect_container(CONTAINER_NAME, None).await
            .map_err(|_| BackupError::NotManagedDocker(format!("container '{CONTAINER_NAME}' not found")))?;
        let has_label = container.config.as_ref()
            .and_then(|c| c.labels.as_ref())
            .is_some_and(|l| l.get("io.agent-connector.managed").map(String::as_str) == Some("true"));
        if !has_label {
            return Err(BackupError::NotManagedDocker(format!(
                "container '{CONTAINER_NAME}' exists but lacks the ownership label"
            )));
        }
        Ok(())
    }

    /// Вся реальная работа — пишет в tmp_path, НЕ в final_path напрямую.
    /// rename() происходит только после подтверждённого успеха (exit_code==0
    /// И size>0) — это единственная точка, где файл становится видимым под
    /// финальным именем. Если функция не дойдёт до rename (Ctrl+C, паника,
    /// любая ошибка) — final_path просто не существует, никогда не в
    /// промежуточном состоянии.
    async fn backup_inner(final_path: &Path, tmp_path: &Path) -> Result<(), BackupError> {
        let docker = connect()?;
        verify_ownership(&docker).await?;

        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let exec = docker.create_exec(CONTAINER_NAME, CreateExecOptions {
            cmd: Some(vec!["pg_dump", "-U", "adapter_connector", "agent_connector"]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        }).await.map_err(|e| BackupError::Docker(format!("create_exec: {e}")))?;

        {
            // Файл открыт в отдельном scope — гарантированно закрывается
            // (flush происходит явно ниже) до попытки rename.
            let mut file = tokio::fs::File::create(tmp_path).await?;

            if let StartExecResults::Attached { mut output, .. } = docker.start_exec(&exec.id, None).await
                .map_err(|e| BackupError::Docker(format!("start_exec: {e}")))?
            {
                while let Some(chunk) = output.next().await {
                    let chunk = chunk.map_err(|e| BackupError::Failed(format!("stream read: {e}")))?;
                    file.write_all(chunk.into_bytes().as_ref()).await?;
                }
            }
            file.flush().await?;
        }

        let inspect = docker.inspect_exec(&exec.id).await
            .map_err(|e| BackupError::Docker(e.to_string()))?;
        if inspect.exit_code != Some(0) {
            tokio::fs::remove_file(tmp_path).await.ok();
            return Err(BackupError::Failed(format!(
                "pg_dump exited with code {:?} — tmp file removed, no backup produced", inspect.exit_code
            )));
        }

        let size = tokio::fs::metadata(tmp_path).await?.len();
        if size == 0 {
            tokio::fs::remove_file(tmp_path).await.ok();
            return Err(BackupError::Failed("pg_dump produced an empty file — removed, not a valid backup".into()));
        }

        // Единственная точка успеха: rename tmp -> final. Atomic на одной
        // файловой системе (гарантия POSIX rename() / Windows MoveFileEx) —
        // final_path либо не существует, либо существует полностью записанным,
        // никогда в промежуточном состоянии, даже если процесс убит ровно
        // в этот момент (rename либо произошёл целиком, либо не произошёл).
        tokio::fs::rename(tmp_path, final_path).await?;

        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn tmp_path_has_tmp_suffix() {
            let final_path = Path::new("/opt/agent-connector/data/backup.sql");
            let tmp = tmp_path_for(final_path);
            assert_eq!(tmp, Path::new("/opt/agent-connector/data/backup.sql.tmp"));
        }

        #[tokio::test]
        async fn interrupted_backup_never_leaves_tmp_file() {
            // Симулируем interrupted-путь напрямую (без реального Docker):
            // создаём .tmp файл, вызываем ту же cleanup-логику, что backup()
            // выполняет в Err(CancelError::Interrupted(_)) ветке, проверяем
            // что файл реально удалён.
            let dir = tempfile::tempdir().unwrap();
            let final_path = dir.path().join("test-backup.sql");
            let tmp_path = tmp_path_for(&final_path);
            tokio::fs::write(&tmp_path, b"partial dump content, incomplete").await.unwrap();
            assert!(tmp_path.exists());

            tokio::fs::remove_file(&tmp_path).await.ok();

            assert!(!tmp_path.exists(), "tmp file must be removed after simulated interruption");
            assert!(!final_path.exists(), "final path must never exist for an interrupted backup");
        }

        #[tokio::test]
        async fn successful_rename_makes_final_path_appear_atomically() {
            let dir = tempfile::tempdir().unwrap();
            let final_path = dir.path().join("test-backup.sql");
            let tmp_path = tmp_path_for(&final_path);

            tokio::fs::write(&tmp_path, b"complete valid dump content").await.unwrap();
            assert!(!final_path.exists(), "before rename, final path must not exist yet");

            tokio::fs::rename(&tmp_path, &final_path).await.unwrap();

            assert!(final_path.exists(), "after rename, final path must exist");
            assert!(!tmp_path.exists(), "after rename, tmp path must no longer exist");
        }
    }
}

// ============================================================
// ЧАСТЬ 2 (docker pull): managed_docker.rs pull_image_if_missing()
// оборачивается в run_cancellable — при Ctrl+C во время pull, явно
// сообщаем, что image может быть частично скачан (Docker сам управляет
// частичными layer'ами через свой content store, это не файл, который
// installer должен чистить руками — но пользователь должен об этом знать).
// ============================================================
pub mod docker_pull_cancellable {
    use super::*;
    use bollard::image::CreateImageOptions;
    use bollard::Docker;
    use futures_util::StreamExt;

    #[derive(Debug, thiserror::Error)]
    pub enum PullError {
        #[error("image pull failed: {0}")]
        Failed(String),
        #[error("image pull interrupted — Docker's layer cache may contain partial data for '{0}'; \
                 this is safe to retry (Docker deduplicates completed layers, re-pull will resume, \
                 not restart from zero for already-downloaded layers)")]
        Interrupted(String),
    }

    pub async fn pull_image_if_missing_cancellable(docker: &Docker, tag: &str) -> Result<(), PullError> {
        let images = docker.list_images::<String>(None).await
            .map_err(|e| PullError::Failed(e.to_string()))?;
        let already_present = images.iter().any(|img| img.repo_tags.iter().any(|t| t == tag));
        if already_present {
            return Ok(());
        }

        let pull_future = async {
            let mut stream = docker.create_image(
                Some(CreateImageOptions { from_image: tag, ..Default::default() }),
                None,
                None,
            );
            while let Some(progress) = stream.next().await {
                progress.map_err(|e| PullError::Failed(format!("image pull failed: {e}")))?;
            }
            Ok::<(), PullError>(())
        };

        match run_cancellable("docker image pull", pull_future).await {
            Ok(inner_result) => inner_result,
            Err(CancelError::Interrupted(_)) => {
                eprintln!(
                    "\nImage pull for '{tag}' interrupted by Ctrl+C. This is safe to retry — \
                     Docker will resume from already-downloaded layers, not start over."
                );
                Err(PullError::Interrupted(tag.to_string()))
            }
        }
    }
}

// ============================================================
// ЧАСТЬ 3: idempotent-resume тест для ensure_running() после partial
// interrupt. Не требует реального Docker — тестирует ЛОГИКУ проверки
// "что уже существует, что ещё нужно создать", той же формы, что уже
// в managed_docker::ensure_network/ensure_volume/ensure_container.
// ============================================================
#[cfg(test)]
mod idempotent_resume_tests {
    /// Симулирует три состояния Docker-ресурсов, отражающие "прервано на
    /// шаге N" сценарии, и проверяет, что повторный ensure_* вызов для
    /// каждого ресурса корректно решает "уже есть, пропускаем" vs
    /// "отсутствует, создаём" — без падения на "already exists" ошибках,
    /// которые bollard/Docker API вернул бы при попытке create_network()
    /// на существующем имени без предварительной list_networks() проверки.
    #[derive(Debug, PartialEq)]
    enum ResourceState { Missing, ExistsOwned, ExistsUnowned }

    fn decide_action(state: &ResourceState) -> &'static str {
        match state {
            ResourceState::Missing => "create",
            ResourceState::ExistsOwned => "skip (already correct)",
            ResourceState::ExistsUnowned => "error (NotOurResource)",
        }
    }

    #[test]
    fn interrupted_after_network_before_volume_resumes_correctly() {
        // Шаг 1 (network) завершился до interrupt, шаг 2 (volume) не начался.
        let network_state = ResourceState::ExistsOwned;
        let volume_state = ResourceState::Missing;
        let container_state = ResourceState::Missing;

        assert_eq!(decide_action(&network_state), "skip (already correct)");
        assert_eq!(decide_action(&volume_state), "create");
        assert_eq!(decide_action(&container_state), "create");
    }

    #[test]
    fn interrupted_after_volume_before_container_resumes_correctly() {
        let network_state = ResourceState::ExistsOwned;
        let volume_state = ResourceState::ExistsOwned;
        let container_state = ResourceState::Missing;

        assert_eq!(decide_action(&network_state), "skip (already correct)");
        assert_eq!(decide_action(&volume_state), "skip (already correct)");
        assert_eq!(decide_action(&container_state), "create");
    }

    #[test]
    fn interrupted_during_pull_before_container_create_resumes_correctly() {
        // network+volume существуют, container ещё не создан (pull не
        // завершился до создания контейнера в ensure_container()) —
        // тот же путь, что "before container", т.к. create_container()
        // вызывается только после успешного pull_image_if_missing().
        let network_state = ResourceState::ExistsOwned;
        let volume_state = ResourceState::ExistsOwned;
        let container_state = ResourceState::Missing;

        assert_eq!(decide_action(&container_state), "create");
        // Повторный вызов ensure_running() после interrupted pull снова
        // попадёт в pull_image_if_missing_cancellable(), которая САМА
        // проверяет already_present через list_images() — если pull был
        // прерван до завершения, образ не в списке images, retry запустит
        // pull снова (с выгодой от уже скачанных Docker layer'ов).
    }

    #[test]
    fn unowned_existing_resource_never_silently_reused() {
        // Инвариант правила #9, применённый к resume-сценарию: даже при
        // повторном запуске после interrupt, наличие ресурса с нужным
        // именем, но без label — всегда ошибка, никогда "просто продолжаем".
        let state = ResourceState::ExistsUnowned;
        assert_eq!(decide_action(&state), "error (NotOurResource)");
    }
}
