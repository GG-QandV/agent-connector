//! crates/adapterctl/src/platform/windows.rs — sc.exe-based реализация
//! `PlatformServiceManager` для Windows 10/11/Server. Копия черновика
//! adapterctl_windows.rs с адаптацией под собранный контракт.

use super::{PlatformError, PlatformServiceManager, ServiceContext};
use std::path::Path;
use std::process::Command;

pub struct WindowsService;

/// Docker daemon endpoint — npipe для локального Docker Desktop/Engine.
/// TCP-вариант означает небезопасно открытый daemon без TLS — installer
/// его не выбирает сам.
const DOCKER_NPIPE: &str = "npipe:////./pipe/docker_engine";

impl PlatformServiceManager for WindowsService {
    fn ensure_service_user(&self, _name: &str) -> Result<(), PlatformError> {
        // Виртуальный service account NT SERVICE\adapterd создаётся
        // автоматически Service Control Manager при sc.exe create.
        Ok(())
    }

    fn register_service(&self, ctx: &ServiceContext) -> Result<(), PlatformError> {
        require_admin()?;

        let bin_path_quoted = format!(
            "\"{}\" \"{}\"",
            ctx.binary_path.display(),
            ctx.config_path.display()
        );

        run_checked(
            "sc.exe",
            &[
                "create",
                "adapterd",
                "type=",
                "own",
                "start=",
                "auto",
                "error=",
                "normal",
                "obj=",
                "NT SERVICE\\adapterd",
                "binPath=",
                &bin_path_quoted,
            ],
            "sc.exe create failed — run as Administrator",
        )?;

        run_checked(
            "sc.exe",
            &[
                "description",
                "adapterd",
                "agent-connector Universal Agent Adapter Runtime",
            ],
            "sc.exe description failed (non-fatal, continuing)",
        )
        .ok();

        run_checked(
            "sc.exe",
            &[
                "failure",
                "adapterd",
                "reset=",
                "86400",
                "actions=",
                "restart/3000/restart/3000/restart/3000",
            ],
            "sc.exe failure config failed (non-fatal, continuing)",
        )
        .ok();

        grant_data_dir_access(ctx.data_directory, "NT SERVICE\\adapterd")?;
        lock_down_read_only(ctx.config_path, "NT SERVICE\\adapterd")?;
        lock_down_read_only(ctx.binary_path, "NT SERVICE\\adapterd")?;

        Ok(())
    }

    fn unregister_service(&self, name: &str) -> Result<(), PlatformError> {
        require_admin()?;
        let _ = Command::new("sc.exe").args(["stop", name]).status();
        std::thread::sleep(std::time::Duration::from_secs(2));
        run_checked("sc.exe", &["delete", name], "sc.exe delete failed")?;
        Ok(())
    }

    fn start_service(&self, name: &str) -> Result<(), PlatformError> {
        require_admin()?;
        run_checked(
            "sc.exe",
            &["start", name],
            "sc.exe start failed — check Get-EventLog -LogName Application -Source adapterd",
        )
    }

    fn restrict_file_permissions(&self, path: &Path, _owner: &str) -> Result<(), PlatformError> {
        lock_down_read_only(path, "NT SERVICE\\adapterd")
    }
}

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
    let output = Command::new(cmd).args(args).output().map_err(|e| {
        PlatformError::CommandFailed(format!("{err_context}: failed to spawn {cmd}: {e}"))
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PlatformError::CommandFailed(format!(
            "{err_context}: {cmd} exited with {} — {stderr}",
            output.status
        )));
    }
    Ok(())
}

fn grant_data_dir_access(path: &Path, service_account: &str) -> Result<(), PlatformError> {
    std::fs::create_dir_all(path).map_err(|e| {
        PlatformError::CommandFailed(format!("cannot create data dir {}: {e}", path.display()))
    })?;
    run_checked(
        "icacls",
        &[
            path.to_str()
                .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 path".into()))?,
            "/inheritance:r",
            "/grant:r",
            "SYSTEM:(OI)(CI)F",
            "/grant:r",
            &format!("{service_account}:(OI)(CI)F"),
            "/grant:r",
            "BUILTIN\\Administrators:(OI)(CI)F",
        ],
        "icacls on data directory failed",
    )
}

fn lock_down_read_only(path: &Path, service_account: &str) -> Result<(), PlatformError> {
    run_checked(
        "icacls",
        &[
            path.to_str()
                .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 path".into()))?,
            "/inheritance:r",
            "/grant:r",
            "SYSTEM:(F)",
            "/grant:r",
            "BUILTIN\\Administrators:(F)",
            "/grant:r",
            &format!("{service_account}:(RX)"),
        ],
        "icacls read-only lockdown failed",
    )
}

/// Docker-доступность на Windows: только npipe, без TCP fallback.
pub fn docker_endpoint() -> &'static str {
    DOCKER_NPIPE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_endpoint_is_named_pipe_not_tcp() {
        assert!(docker_endpoint().starts_with("npipe://"));
        assert!(!docker_endpoint().contains("tcp://"));
    }
}
