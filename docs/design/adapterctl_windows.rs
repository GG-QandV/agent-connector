//! crates/adapterctl/src/platform/windows.rs — полная реализация под
//! Windows 10, Windows 11, Windows Server 2019/2022/2025.
//!
//! Отличия целей друг от друга, учтённые ниже:
//!   - Windows 10/11 (desktop): Docker обычно через Docker Desktop (WSL2
//!     backend) — daemon слушает npipe `\\.\pipe\docker_engine` ИЛИ, если
//!     Docker Desktop настроен на "expose daemon on tcp://localhost:2375"
//!     (выключено по умолчанию, небезопасно), через TCP. По умолчанию
//!     пробуем npipe.
//!   - Windows Server: чаще нативный Docker Engine (без Docker Desktop,
//!     Windows containers ИЛИ Linux containers через WSL2), тоже слушает
//!     на npipe `\\.\pipe\docker_engine` при стандартной установке
//!     `Install-Package Docker` / `Install-WindowsFeature -Name Containers`.
//!   - Оба варианта: не предполагаем никакой Linux init-систему — служба
//!     регистрируется через Windows Service Control Manager (`sc.exe`
//!     ИЛИ `windows-service` crate для нативной интеграции с событиями
//!     остановки/паузы), не через сторонний wrapper типа NSSM (не тащим
//!     дополнительную внешнюю зависимость, когда есть нативный API).

use super::{PlatformError, PlatformServiceManager, ServiceContext};
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct WindowsService;

/// Docker daemon endpoint — npipe для локального Docker Desktop/Engine.
/// TCP-вариант почти всегда означает небезопасно открытый daemon без TLS,
/// поэтому installer его не выбирает сам — только явный advanced-флаг,
/// не реализованный в этой версии (осознанно, не забыто).
const DOCKER_NPIPE: &str = "npipe:////./pipe/docker_engine";

impl PlatformServiceManager for WindowsService {
    fn ensure_service_user(&self, _name: &str) -> Result<(), PlatformError> {
        // Виртуальный service account NT SERVICE\adapterd создаётся
        // автоматически Service Control Manager при первом sc.exe create
        // с obj="NT SERVICE\<name>" — никакого net user/New-LocalUser
        // не требуется. Это отличается от Linux, где useradd — явный шаг.
        Ok(())
    }

    fn register_service(&self, ctx: &ServiceContext) -> Result<(), PlatformError> {
        require_admin()?;

        let bin_path_quoted = format!(
            "\"{}\" \"{}\"",
            ctx.binary_path.display(),
            ctx.config_path.display()
        );

        // sc.exe create регистрирует службу с автозапуском и виртуальным
        // service account. displayname/description отдельными вызовами,
        // потому что sc.exe create принимает не все опции одной командой
        // надёжно на всех версиях Windows.
        run_checked(
            "sc.exe",
            &[
                "create", "adapterd",
                "type=", "own",
                "start=", "auto",
                "error=", "normal",
                "obj=", "NT SERVICE\\adapterd",
                "binPath=", &bin_path_quoted,
            ],
            "sc.exe create failed — run as Administrator",
        )?;

        run_checked(
            "sc.exe",
            &["description", "adapterd", "agent-connector Universal Agent Adapter Runtime"],
            "sc.exe description failed (non-fatal, continuing)",
        ).ok();

        // failure actions: restart after 3s on crash, up to 3 times in 24h —
        // аналог Restart=on-failure / RestartSec=3 из systemd unit.
        run_checked(
            "sc.exe",
            &["failure", "adapterd", "reset=", "86400", "actions=", "restart/3000/restart/3000/restart/3000"],
            "sc.exe failure config failed (non-fatal, continuing)",
        ).ok();

        // WorkingDirectory-эквивалент: adapterd сам резолвит config_path
        // как абсолютный путь (передан в binPath), working directory
        // самой службы Windows не критична для этого процесса, т.к. он не
        // читает относительные пути после старта — в отличие от systemd
        // unit, где WorkingDirectory задавал базу для относительных путей.

        // ACL на data-каталог: NT SERVICE\adapterd должен иметь write-доступ
        // именно и только туда — аналог ReadWritePaths+ProtectSystem=full.
        grant_data_dir_access(ctx.data_directory, "NT SERVICE\\adapterd")?;
        // Конфиг и бинарь — read-only для service account, не world-writable.
        lock_down_read_only(ctx.config_path, "NT SERVICE\\adapterd")?;
        lock_down_read_only(ctx.binary_path, "NT SERVICE\\adapterd")?;

        Ok(())
    }

    fn unregister_service(&self, name: &str) -> Result<(), PlatformError> {
        require_admin()?;
        // stop может вернуть ненулевой код, если служба уже остановлена —
        // не фейлим на этом, только на delete.
        let _ = Command::new("sc.exe").args(["stop", name]).status();
        std::thread::sleep(std::time::Duration::from_secs(2));
        run_checked("sc.exe", &["delete", name], "sc.exe delete failed")?;
        Ok(())
    }

    fn start_service(&self, name: &str) -> Result<(), PlatformError> {
        require_admin()?;
        run_checked("sc.exe", &["start", name], "sc.exe start failed — check Event Viewer / journalctl-эквивалент: Get-EventLog -LogName Application -Source adapterd")
    }

    fn restrict_file_permissions(&self, path: &Path, _owner: &str) -> Result<(), PlatformError> {
        lock_down_read_only(path, "NT SERVICE\\adapterd")
    }
}

/// Проверка на административные права — sc.exe/icacls с системными путями
/// без прав тихо ничего не делают или дают невнятные ошибки, поэтому
/// проверяем явно и заранее через net session (стандартный трюк:
/// `net session` требует admin, при отсутствии прав возвращает ошибку).
fn require_admin() -> Result<(), PlatformError> {
    let status = Command::new("net").args(["session"]).output();
    match status {
        Ok(out) if out.status.success() => Ok(()),
        _ => Err(PlatformError::PermissionDenied(
            "adapterctl must be run from an elevated (Administrator) PowerShell/cmd prompt".into(),
        )),
    }
}

fn run_checked(cmd: &str, args: &[&str], err_context: &str) -> Result<(), PlatformError> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| PlatformError::CommandFailed(format!("{err_context}: failed to spawn {cmd}: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PlatformError::CommandFailed(format!(
            "{err_context}: {cmd} exited with {} — {stderr}",
            output.status
        )));
    }
    Ok(())
}

/// icacls: снимает наследование, даёт полный доступ SYSTEM и запись —
/// сервисному virtual account, никому больше. Аналог chmod 0600+chown.
fn grant_data_dir_access(path: &Path, service_account: &str) -> Result<(), PlatformError> {
    std::fs::create_dir_all(path)
        .map_err(|e| PlatformError::CommandFailed(format!("cannot create data dir {}: {e}", path.display())))?;
    run_checked(
        "icacls",
        &[
            path.to_str().ok_or_else(|| PlatformError::CommandFailed("non-UTF8 path".into()))?,
            "/inheritance:r",
            "/grant:r", "SYSTEM:(OI)(CI)F",
            "/grant:r", &format!("{service_account}:(OI)(CI)F"),
            "/grant:r", "BUILTIN\\Administrators:(OI)(CI)F",
        ],
        "icacls on data directory failed",
    )
}

/// Read-only для сервисного аккаунта: config/binary/.env не должны быть
/// world-readable/writable — единственный, кто читает, это сам сервис и
/// локальный администратор.
fn lock_down_read_only(path: &Path, service_account: &str) -> Result<(), PlatformError> {
    run_checked(
        "icacls",
        &[
            path.to_str().ok_or_else(|| PlatformError::CommandFailed("non-UTF8 path".into()))?,
            "/inheritance:r",
            "/grant:r", "SYSTEM:(F)",
            "/grant:r", "BUILTIN\\Administrators:(F)",
            "/grant:r", &format!("{service_account}:(RX)"),
        ],
        "icacls read-only lockdown failed",
    )
}

/// Docker-доступность на Windows: сначала пробуем npipe (Docker Desktop
/// или нативный Windows Docker Engine), явно НЕ пытаемся TCP fallback —
/// если npipe недоступен, считаем Docker недоступным и просим пользователя
/// либо установить/запустить Docker Desktop, либо выбрать другой storage
/// профиль (ExistingPostgres/Sqlite), не тихо деградировать на что-то
/// небезопасное.
pub fn docker_endpoint() -> &'static str {
    DOCKER_NPIPE
}

/// Проверка, что Docker daemon реально отвечает — до того как показывать
/// пользователю managed-docker-postgres как валидный выбор. Используется
/// в install-flow ДО confirm_docker промпта: если Docker недоступен,
/// не смысла спрашивать "подтвердите использование Docker".
pub async fn docker_available() -> bool {
    // Реальная реализация: bollard::Docker::connect_with_named_pipe(...)
    // + ping(). Здесь — сигнатура и место интеграции; сам bollard-вызов
    // требует async runtime, подключается на уровне install-flow, где
    // уже есть tokio context.
    match bollard::Docker::connect_with_named_pipe(DOCKER_NPIPE, 10, bollard::API_DEFAULT_VERSION) {
        Ok(docker) => docker.ping().await.is_ok(),
        Err(_) => false,
    }
}

/// Уточнение для managed_docker::ensure_running() на Windows: контейнер
/// должен быть Linux-based Postgres образ, что требует Docker Desktop/
/// Engine в Linux-containers режиме (не Windows containers режим).
/// Проверяем через `docker version` info (OSType), не предполагаем молча.
pub async fn assert_linux_containers_mode() -> Result<(), PlatformError> {
    let docker = bollard::Docker::connect_with_named_pipe(DOCKER_NPIPE, 10, bollard::API_DEFAULT_VERSION)
        .map_err(|e| PlatformError::CommandFailed(format!("cannot connect to Docker: {e}")))?;
    let info = docker.info().await
        .map_err(|e| PlatformError::CommandFailed(format!("docker info failed: {e}")))?;
    match info.os_type.as_deref() {
        Some("linux") => Ok(()),
        Some(other) => Err(PlatformError::Unsupported(format!(
            "Docker is running in '{other}' containers mode, but managed-docker-postgres \
             requires Linux containers. On Windows, switch Docker Desktop to Linux containers \
             (right-click tray icon -> 'Switch to Linux containers...')."
        ))),
        None => Err(PlatformError::CommandFailed("docker info returned no OSType".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_endpoint_is_named_pipe_not_tcp() {
        // Инвариант: installer никогда по умолчанию не адресует Docker
        // daemon по TCP (что почти всегда означает небезопасно
        // сконфигурированный daemon без TLS) — только локальный named pipe.
        assert!(docker_endpoint().starts_with("npipe://"));
        assert!(!docker_endpoint().contains("tcp://"));
    }
}
