//! ПРАВКА к crates/adapterctl/src/managed_docker.rs — добавляет публичную
//! preflight_check(), вызываемую в ДВУХ местах:
//!   1. install_flow::resolve_storage_choice() / resolve_storage() — ДО того,
//!      как ManagedDockerPostgres принимается как валидный выбор, не после.
//!   2. tests/managed_docker_integration.rs — та же проверка, не дублирующая
//!      логика внутри теста.
//!
//! Почему это "ближе к проду", не просто тестовый хелпер:
//!   Текущий баг (до этой правки): пользователь выбирает
//!   ManagedDockerPostgres, подтверждает --confirm-docker, ждёт cargo build
//!   (может занять минуты), и ТОЛЬКО ПОТОМ, внутри ensure_running(), узнаёт,
//!   что Docker daemon недоступен или в неправильном containers mode.
//!   preflight_check(), вызванный сразу в resolve_storage_choice(), даёт
//!   эту ошибку ДО build — тот же fail-fast принцип, что уже применён к
//!   --confirm-docker и config_template::load().

use bollard::Docker;

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("Docker daemon is not reachable — is Docker Desktop running? (Windows/macOS: check the whale icon in the system tray/menu bar; Linux: `systemctl status docker`)")]
    NotReachable,
    #[error("Docker is in Windows containers mode, but a Linux-based Postgres image is required. \
             Switch Docker Desktop to Linux containers (right-click tray icon -> 'Switch to Linux containers...').")]
    WrongContainerMode,
    #[error("failed to connect to Docker daemon: {0}")]
    Connect(String),
}

/// Единственная публичная точка входа для "можно ли использовать
/// managed-docker-postgres на этой машине прямо сейчас". Композирует
/// connect() + ping() + assert_linux_containers_mode() в одном вызове —
/// вызывающий код (install_flow, тесты) не должен знать про порядок этих
/// трёх шагов, только про итоговый Result.
pub async fn preflight_check() -> Result<(), PreflightError> {
    let docker = connect_internal()?;

    docker.ping().await.map_err(|_| PreflightError::NotReachable)?;

    #[cfg(target_os = "windows")]
    {
        let info = docker.info().await
            .map_err(|e| PreflightError::Connect(format!("docker info failed: {e}")))?;
        if info.os_type.as_deref() != Some("linux") {
            return Err(PreflightError::WrongContainerMode);
        }
    }

    Ok(())
}

/// Внутренний connect(), переиспользуемый и preflight_check(), и
/// ensure_running() — единственное место, где решается, как именно
/// подключаться к daemon на каждой платформе (было продублировано в двух
/// функциях ранее в диалоге, теперь один источник истины).
fn connect_internal() -> Result<Docker, PreflightError> {
    #[cfg(target_os = "windows")]
    {
        Docker::connect_with_named_pipe("npipe:////./pipe/docker_engine", 10, bollard::API_DEFAULT_VERSION)
            .map_err(|e| PreflightError::Connect(e.to_string()))
    }
    #[cfg(not(target_os = "windows"))]
    {
        Docker::connect_with_local_defaults().map_err(|e| PreflightError::Connect(e.to_string()))
    }
}

// ============================================================
// ensure_running() из предыдущей версии файла — обновлён, чтобы вызывать
// preflight_check() ВМЕСТО собственного connect()+ping()+assert_linux_containers_mode(),
// не дублировать эту логику. Остальное тело функции (ensure_network,
// ensure_volume, ensure_container, wait_for_ready) не меняется.
// ============================================================
pub async fn ensure_running(plan: &super::ManagedPostgresPlan) -> Result<(), super::DockerError> {
    preflight_check().await.map_err(|e| super::DockerError::Operation(e.to_string()))?;

    let docker = connect_internal().map_err(|e| super::DockerError::Connect(e.to_string()))?;

    super::ensure_network(&docker).await?;
    super::ensure_volume(&docker).await?;
    super::ensure_container(&docker, plan).await?;
    super::wait_for_ready(&docker).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Юнит-тест без реального Docker: проверяет только то, что
    // PreflightError::WrongContainerMode текст содержит actionable
    // инструкцию, не голое "wrong mode" — регрессионная защита от
    // будущего рефакторинга сообщения об ошибке в менее полезный текст.
    #[test]
    fn wrong_container_mode_error_is_actionable() {
        let err = PreflightError::WrongContainerMode;
        let msg = err.to_string();
        assert!(msg.contains("Switch"), "error message must tell the user what to do, not just what's wrong");
    }

    #[test]
    fn not_reachable_error_mentions_docker_desktop() {
        let err = PreflightError::NotReachable;
        let msg = err.to_string();
        assert!(msg.to_lowercase().contains("docker desktop") || msg.to_lowercase().contains("systemctl"),
                "error message must point to the actual fix for this platform");
    }
}
