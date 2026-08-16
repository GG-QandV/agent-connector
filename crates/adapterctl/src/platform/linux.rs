//! crates/adapterctl/src/platform/linux.rs — systemd-based реализация
//! `PlatformServiceManager`. Копия черновика adapterctl_linux_register_service.rs
//! с адаптацией под собранный `platform::mod` контракт.

use super::{PlatformError, PlatformServiceManager, ServiceContext};
use std::path::Path;
use std::process::Command;

pub struct Systemd;

const UNIT_NAME: &str = "adapterd.service";
const UNIT_DEST: &str = "/etc/systemd/system/adapterd.service";
/// Путь к оригинальному deploy-артефакту внутри репозитория.
const UNIT_SRC_RELATIVE: &str = "deploy/systemd/adapterd.service";
const DEFAULT_HARDCODED_PREFIX: &str = "/opt/agent-connector";
const DEFAULT_HARDCODED_USER: &str = "adapterd";

impl PlatformServiceManager for Systemd {
    fn ensure_service_user(&self, name: &str) -> Result<(), PlatformError> {
        let exists = Command::new("id")
            .arg(name)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if exists {
            return Ok(());
        }
        let status = Command::new("useradd")
            .args([
                "--system",
                "--no-create-home",
                "--shell",
                "/usr/sbin/nologin",
                name,
            ])
            .status()
            .map_err(|e| PlatformError::CommandFailed(format!("useradd spawn failed: {e}")))?;
        if !status.success() {
            return Err(PlatformError::CommandFailed(format!(
                "useradd exited with {status}"
            )));
        }
        Ok(())
    }

    fn register_service(&self, ctx: &ServiceContext) -> Result<(), PlatformError> {
        require_root()?;

        let repo_root = find_repo_root(ctx.working_directory)?;
        let unit_src_path = repo_root.join(UNIT_SRC_RELATIVE);
        if !unit_src_path.exists() {
            return Err(PlatformError::CommandFailed(format!(
                "systemd unit template not found at {} — expected it inside the agent-connector \
                 repository checkout, not just the install prefix",
                unit_src_path.display()
            )));
        }

        let raw_unit = std::fs::read_to_string(&unit_src_path)
            .map_err(|e| PlatformError::CommandFailed(format!("cannot read unit template: {e}")))?;

        let prefix_str = ctx
            .working_directory
            .to_str()
            .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 prefix path".into()))?;

        let rendered_unit =
            if prefix_str == DEFAULT_HARDCODED_PREFIX && ctx.user == DEFAULT_HARDCODED_USER {
                raw_unit
            } else {
                render_unit_with_substitutions(&raw_unit, prefix_str, ctx.user)?
            };

        std::fs::write(UNIT_DEST, rendered_unit)
            .map_err(|e| PlatformError::CommandFailed(format!("cannot write {UNIT_DEST}: {e}")))?;

        run_checked(
            "systemctl",
            &["daemon-reload"],
            "systemctl daemon-reload failed",
        )?;
        run_checked(
            "systemctl",
            &["enable", UNIT_NAME],
            "systemctl enable failed",
        )?;

        Ok(())
    }

    fn unregister_service(&self, name: &str) -> Result<(), PlatformError> {
        require_root()?;
        let _ = Command::new("systemctl")
            .args(["disable", "--now", name])
            .status();
        std::fs::remove_file(format!("/etc/systemd/system/{name}")).ok();
        run_checked(
            "systemctl",
            &["daemon-reload"],
            "systemctl daemon-reload failed",
        )?;
        Ok(())
    }

    fn start_service(&self, name: &str) -> Result<(), PlatformError> {
        require_root()?;
        run_checked(
            "systemctl",
            &["start", name],
            "systemctl start failed — check `journalctl -u adapterd -e` for details",
        )
    }

    fn stop_service(&self, name: &str) -> Result<(), PlatformError> {
        require_root()?;
        // systemctl stop идемпотентен для уже-остановленной службы (exit 0).
        run_checked(
            "systemctl",
            &["stop", name],
            "systemctl stop failed — check `journalctl -u adapterd -e` for details",
        )
    }

    fn restrict_file_permissions(&self, path: &Path, owner: &str) -> Result<(), PlatformError> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| PlatformError::CommandFailed(format!("chmod 0600 failed: {e}")))?;
        run_checked(
            "chown",
            &[&format!("{owner}:{owner}"), path.to_str().unwrap_or("")],
            "chown failed",
        )
    }
}

/// Точечная замена ровно тех мест, что реально существуют в unit-файле:
/// ExecStart (два абсолютных пути), EnvironmentFile, ReadWritePaths,
/// WorkingDirectory, User, Group. Строковая замена PREFIX, не regex.
fn render_unit_with_substitutions(
    raw: &str,
    new_prefix: &str,
    new_user: &str,
) -> Result<String, PlatformError> {
    let rendered = raw.replace(DEFAULT_HARDCODED_PREFIX, new_prefix);

    let mut result_lines = Vec::new();
    for line in rendered.lines() {
        if line.starts_with("User=") {
            result_lines.push(format!("User={new_user}"));
        } else if line.starts_with("Group=") {
            result_lines.push(format!("Group={new_user}"));
        } else {
            result_lines.push(line.to_string());
        }
    }
    let mut rendered = result_lines.join("\n");
    rendered.push('\n');

    Ok(rendered)
}

fn find_repo_root(_prefix: &Path) -> Result<std::path::PathBuf, PlatformError> {
    std::env::current_dir().map_err(|e| {
        PlatformError::CommandFailed(format!("cannot determine current directory: {e}"))
    })
}

fn require_root() -> Result<(), PlatformError> {
    let uid = unsafe { libc_geteuid() };
    if uid != 0 {
        return Err(PlatformError::PermissionDenied(
            "adapterctl must be run as root (sudo adapterctl ...) for systemd/useradd/chown operations".into()
        ));
    }
    Ok(())
}

extern "C" {
    fn geteuid() -> u32;
}
unsafe fn libc_geteuid() -> u32 {
    geteuid()
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

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_UNIT: &str = "[Unit]\nDescription=agent-connector adapterd\n\n[Service]\nUser=adapterd\nGroup=adapterd\nWorkingDirectory=/opt/agent-connector\nExecStart=/opt/agent-connector/target/release/adapterd /opt/agent-connector/adapter.yaml\nEnvironmentFile=-/opt/agent-connector/.env\nReadWritePaths=/opt/agent-connector/data\n\n[Install]\nWantedBy=multi-user.target\n";

    #[test]
    fn default_prefix_and_user_unit_is_copied_verbatim_logic() {
        let prefix = "/opt/agent-connector";
        let user = "adapterd";
        let is_default = prefix == DEFAULT_HARDCODED_PREFIX && user == DEFAULT_HARDCODED_USER;
        assert!(
            is_default,
            "default values must trigger verbatim copy path, not substitution"
        );
    }

    #[test]
    fn custom_prefix_substitutes_all_occurrences() {
        let rendered =
            render_unit_with_substitutions(SAMPLE_UNIT, "/srv/agent-connector", "svc_adapterd")
                .unwrap();
        assert!(
            !rendered.contains("/opt/agent-connector"),
            "old prefix must not remain anywhere in the unit"
        );
        assert!(rendered.contains("/srv/agent-connector/target/release/adapterd"));
        assert!(rendered.contains("/srv/agent-connector/adapter.yaml"));
        assert!(rendered.contains("EnvironmentFile=-/srv/agent-connector/.env"));
        assert!(rendered.contains("ReadWritePaths=/srv/agent-connector/data"));
    }

    #[test]
    fn custom_user_replaces_user_and_group_lines_only() {
        let rendered =
            render_unit_with_substitutions(SAMPLE_UNIT, DEFAULT_HARDCODED_PREFIX, "svc_adapterd")
                .unwrap();
        assert!(rendered.contains("User=svc_adapterd"));
        assert!(rendered.contains("Group=svc_adapterd"));
        assert!(
            !rendered.contains("User=adapterd\n"),
            "old User= line must be fully replaced, not appended to"
        );
    }

    #[test]
    fn rendered_unit_still_has_required_sections() {
        let rendered =
            render_unit_with_substitutions(SAMPLE_UNIT, "/custom/path", "custom_user").unwrap();
        assert!(rendered.contains("[Unit]"));
        assert!(rendered.contains("[Service]"));
        assert!(rendered.contains("[Install]"));
    }
}
