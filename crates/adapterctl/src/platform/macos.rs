//! crates/adapterctl/src/platform/macos.rs — полная реализация через
//! launchd, заменяющая Err(Unsupported) заглушку. Приоритет поднят:
//! macOS покрывает ~25-30% пользователей (Windows 65-70%, Linux остаток) —
//! это одна из двух ключевых платформ, не опциональная.
//!
//! Ключевые решения, подтверждённые через актуальную документацию:
//!   - Современный синтаксис `launchctl bootstrap system <plist>` /
//!     `launchctl kickstart` (macOS 10.10+), НЕ устаревший load/unload —
//!     bootstrap корректно работает с System Integrity Protection и новыми
//!     ограничениями на background items.
//!   - System account UID диапазон 200-400 (стандартный macOS диапазон для
//!     system services).
//!   - LaunchDaemon (не LaunchAgent) — daemon работает без залогиненного
//!     пользователя, аналогично systemd system service.

use super::{PlatformError, PlatformServiceManager, ServiceContext};
use std::path::Path;
use std::process::Command;

pub struct Launchd;

const SERVICE_LABEL: &str = "com.agent-connector.adapterd";
const PLIST_PATH: &str = "/Library/LaunchDaemons/com.agent-connector.adapterd.plist";
/// Системные UID 200-400 зарезервированы под service accounts на macOS.
const SYSTEM_UID_RANGE: std::ops::RangeInclusive<u32> = 200..=400;
const DEFAULT_SERVICE_ACCOUNT: &str = "_adapterd";

impl PlatformServiceManager for Launchd {
    fn ensure_service_user(&self, name: &str) -> Result<(), PlatformError> {
        require_root()?;

        if user_exists(name)? {
            return Ok(());
        }

        let uid = find_free_system_uid()?;
        let user_path = format!("/Users/{name}");

        // dscl создание аккаунта — последовательность обязательных полей.
        run_checked("dscl", &[".", "-create", &user_path], "dscl create failed")?;
        run_checked(
            "dscl",
            &[".", "-create", &user_path, "UniqueID", &uid.to_string()],
            "dscl set UniqueID failed",
        )?;
        run_checked(
            "dscl",
            &[".", "-create", &user_path, "PrimaryGroupID", "1"],
            "dscl set PrimaryGroupID failed",
        )?; // gid 1 = daemon group
        run_checked(
            "dscl",
            &[".", "-create", &user_path, "UserShell", "/usr/bin/false"],
            "dscl set UserShell failed",
        )?;
        run_checked(
            "dscl",
            &[".", "-create", &user_path, "NFSHomeDirectory", "/var/empty"],
            "dscl set NFSHomeDirectory failed",
        )?;
        run_checked(
            "dscl",
            &[
                ".",
                "-create",
                &user_path,
                "RealName",
                "agent-connector adapterd service account",
            ],
            "dscl set RealName failed",
        )?;
        // IsHidden скрывает аккаунт из UI логина — service account не должен
        // предлагаться как вариант логина.
        run_checked(
            "dscl",
            &[".", "-create", &user_path, "IsHidden", "1"],
            "dscl set IsHidden failed",
        )?;

        Ok(())
    }

    fn register_service(&self, ctx: &ServiceContext) -> Result<(), PlatformError> {
        require_root()?;

        let plist_xml = render_plist(ctx)?;
        std::fs::write(PLIST_PATH, plist_xml)
            .map_err(|e| PlatformError::CommandFailed(format!("cannot write {PLIST_PATH}: {e}")))?;

        // root:wheel + 644 — launchd отказывается загружать LaunchDaemon
        // plist с неправильными правами/владельцем.
        run_checked(
            "chown",
            &["root:wheel", PLIST_PATH],
            "chown on plist failed",
        )?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(PLIST_PATH, std::fs::Permissions::from_mode(0o644))
            .map_err(|e| PlatformError::CommandFailed(format!("chmod on plist failed: {e}")))?;

        // Современный синтаксис (macOS 10.10+): bootstrap в system domain.
        run_checked(
            "launchctl", &["bootstrap", "system", PLIST_PATH],
            "launchctl bootstrap failed — is the plist already loaded? try `launchctl bootout system <label>` first",
        )?;

        Ok(())
    }

    fn unregister_service(&self, _name: &str) -> Result<(), PlatformError> {
        require_root()?;

        // bootout может вернуть ошибку, если сервис уже не загружен — не
        // фейлим на этом (та же логика, что в Linux/Windows).
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("system/{SERVICE_LABEL}")])
            .status();

        std::fs::remove_file(PLIST_PATH).ok();
        Ok(())
    }

    fn start_service(&self, _name: &str) -> Result<(), PlatformError> {
        require_root()?;
        // kickstart -k = kill existing instance if running, then start.
        run_checked(
            "launchctl", &["kickstart", "-k", &format!("system/{SERVICE_LABEL}")],
            "launchctl kickstart failed — check `sudo launchctl print system/com.agent-connector.adapterd` for diagnostics",
        )
    }

    fn stop_service(&self, _name: &str) -> Result<(), PlatformError> {
        require_root()?;
        // SIGTERM останавливает процесс; при graceful shutdown (exit 0)
        // KeepAlive {SuccessfulExit: false} НЕ перезапускает его — это
        // штатная остановка, а не сбой (в отличие от launchctl bootout,
        // который выгружает job целиком).
        run_checked(
            "launchctl", &["kill", "SIGTERM", &format!("system/{SERVICE_LABEL}")],
            "launchctl kill SIGTERM failed — is the service running? `sudo launchctl print system/com.agent-connector.adapterd`",
        )
    }

    fn restart_service(&self, _name: &str) -> Result<(), PlatformError> {
        require_root()?;
        // kickstart -k = kill existing instance if running, then start —
        // это и есть restart одним вызовом.
        run_checked(
            "launchctl", &["kickstart", "-k", &format!("system/{SERVICE_LABEL}")],
            "launchctl kickstart -k failed — check `sudo launchctl print system/com.agent-connector.adapterd` for diagnostics",
        )
    }

    fn restrict_file_permissions(&self, path: &Path, owner: &str) -> Result<(), PlatformError> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| PlatformError::CommandFailed(format!("chmod 0600 failed: {e}")))?;
        run_checked(
            "chown",
            &[&format!("{owner}:daemon"), path.to_str().unwrap_or("")],
            "chown failed",
        )
    }
}

/// Генерирует .plist XML. ProgramArguments = [binary_path, config_path] —
/// зеркалит ExecStart из systemd unit. KeepAlive с SuccessfulExit:false —
/// restart только при ненулевом коде выхода, аналог Restart=on-failure.
fn render_plist(ctx: &ServiceContext) -> Result<String, PlatformError> {
    let binary = ctx
        .binary_path
        .to_str()
        .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 binary path".into()))?;
    let config = ctx
        .config_path
        .to_str()
        .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 config path".into()))?;
    let working_dir = ctx
        .working_directory
        .to_str()
        .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 working directory".into()))?;
    let data_dir = ctx
        .data_directory
        .to_str()
        .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 data directory".into()))?;
    let env_file = ctx
        .env_file_path
        .to_str()
        .ok_or_else(|| PlatformError::CommandFailed("non-UTF8 env file path".into()))?;

    // launchd не умеет читать .env файл напрямую (в отличие от systemd
    // EnvironmentFile=). Передаём путь через EnvironmentVariables ->
    // ADAPTERD_ENV_FILE, adapterd сам читает его при старте.
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{SERVICE_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{binary}</string>
        <string>{config}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{working_dir}</string>
    <key>UserName</key>
    <string>{user}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>ADAPTERD_ENV_FILE</key>
        <string>{env_file}</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>/var/log/agent-connector/adapterd.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/agent-connector/adapterd.error.log</string>
    <key>ProcessType</key>
    <string>Background</string>
    <!-- Аналог ReadWritePaths+ProtectSystem=full из systemd unit: launchd
         не имеет прямого эквивалента файловой изоляции systemd (sandbox-exec
         profile мог бы, но это отдельный механизм) — вместо этого полагаемся
         на Unix-права: {data_dir} принадлежит {user}, остальное — root:daemon
         read-only (см. restrict_file_permissions). -->
</dict>
</plist>
"#,
        user = DEFAULT_SERVICE_ACCOUNT,
        data_dir = data_dir,
    ))
}

fn user_exists(name: &str) -> Result<bool, PlatformError> {
    let status = Command::new("dscl")
        .args([".", "-read", &format!("/Users/{name}")])
        .output()
        .map_err(|e| PlatformError::CommandFailed(format!("dscl -read spawn failed: {e}")))?;
    Ok(status.status.success())
}

/// Ищет первый свободный UID в системном диапазоне 200-400. Линейный
/// перебор с dscl — приемлемо, выполняется один раз при install.
fn find_free_system_uid() -> Result<u32, PlatformError> {
    let output = Command::new("dscl")
        .args([".", "-list", "/Users", "UniqueID"])
        .output()
        .map_err(|e| PlatformError::CommandFailed(format!("dscl -list failed: {e}")))?;
    let text = String::from_utf8_lossy(&output.stdout);

    let taken: std::collections::HashSet<u32> = text
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter_map(|s| s.parse::<u32>().ok())
        .collect();

    SYSTEM_UID_RANGE
        .into_iter()
        .find(|uid| !taken.contains(uid))
        .ok_or_else(|| PlatformError::CommandFailed(
            "no free UID found in the 200-400 system account range — highly unusual, check `dscl . -list /Users UniqueID` manually".into()
        ))
}

fn require_root() -> Result<(), PlatformError> {
    // На macOS geteuid() тоже стабильный libc symbol, тот же приём, что на Linux.
    let uid = unsafe { geteuid() };
    if uid != 0 {
        return Err(PlatformError::PermissionDenied(
            "adapterctl must be run with sudo on macOS for dscl/launchctl/chown operations".into(),
        ));
    }
    Ok(())
}

extern "C" {
    fn geteuid() -> u32;
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

    fn test_ctx() -> ServiceContext<'static> {
        ServiceContext {
            binary_path: Path::new("/opt/agent-connector/target/release/adapterd"),
            config_path: Path::new("/opt/agent-connector/adapter.yaml"),
            env_file_path: Path::new("/opt/agent-connector/.env"),
            working_directory: Path::new("/opt/agent-connector"),
            data_directory: Path::new("/opt/agent-connector/data"),
            user: DEFAULT_SERVICE_ACCOUNT,
        }
    }

    #[test]
    fn plist_contains_modern_keepalive_dict_not_bare_true() {
        let plist = render_plist(&test_ctx()).unwrap();
        assert!(plist.contains("<key>SuccessfulExit</key>"));
        assert!(!plist.contains("<key>KeepAlive</key>\n    <true/>"));
    }

    #[test]
    fn plist_is_well_formed_xml_structure() {
        let plist = render_plist(&test_ctx()).unwrap();
        assert!(plist.starts_with("<?xml"));
        assert!(plist.contains("<plist version=\"1.0\">"));
        assert_eq!(
            plist.matches("<dict>").count(),
            plist.matches("</dict>").count()
        );
        assert_eq!(
            plist.matches("<array>").count(),
            plist.matches("</array>").count()
        );
    }

    #[test]
    fn system_uid_range_excludes_os_reserved_and_regular_users() {
        assert!(!SYSTEM_UID_RANGE.contains(&0), "must not touch root's UID");
        assert!(
            !SYSTEM_UID_RANGE.contains(&100),
            "1-199 reserved for OS itself"
        );
        assert!(
            !SYSTEM_UID_RANGE.contains(&501),
            "501+ reserved for regular user accounts"
        );
        assert!(SYSTEM_UID_RANGE.contains(&250));
    }
}
