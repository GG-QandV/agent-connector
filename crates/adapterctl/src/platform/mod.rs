//! crates/adapterctl/src/platform/mod.rs — общий контракт платформенного
//! слоя: trait `PlatformServiceManager`, `PlatformError`, `ServiceContext`,
//! `StorageChoice` и фабрика `platform_manager()`. Типы, которые в черновиках
//! были разбросаны по нескольким файлам, собраны здесь в едином месте.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;
#[cfg(target_os = "windows")]
pub mod windows;

use std::path::Path;

/// Выбор storage backend, который делает install-flow. Определён здесь
/// (platform-независимый выбор), не в managed_docker — это не про Docker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageChoice {
    Sqlite,
    ExistingPostgres,
    ManagedDockerPostgres,
    ExternalManagedPostgres,
}

/// Платформенно-специфичный результат — единый error type для всех
/// операций service-manager слоя (useradd/sc.exe/systemctl/chown/icacls).
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("command failed: {0}")]
    CommandFailed(String),
    #[allow(dead_code)]
    #[error("unsupported on this platform: {0}")]
    Unsupported(String),
}

/// Контекст, необходимый платформенному слою для регистрации службы.
#[derive(Debug)]
pub struct ServiceContext<'a> {
    #[allow(dead_code)]
    pub binary_path: &'a Path,
    #[allow(dead_code)]
    pub config_path: &'a Path,
    #[allow(dead_code)]
    pub env_file_path: &'a Path,
    /// install-prefix (/opt/agent-connector), куда копируются файлы.
    pub working_directory: &'a Path,
    #[allow(dead_code)]
    pub data_directory: &'a Path,
    pub user: &'a str,
}

/// Единый контракт управления службой для всех платформ.
pub trait PlatformServiceManager: Send + Sync {
    /// Создаёт системного пользователя для службы, если не существует.
    fn ensure_service_user(&self, name: &str) -> Result<(), PlatformError>;
    /// Регистрирует службу (systemd unit / sc.exe) с автозапуском.
    fn register_service(&self, ctx: &ServiceContext) -> Result<(), PlatformError>;
    /// Удаляет службу. Идемпотентно для уже-остановленной.
    fn unregister_service(&self, name: &str) -> Result<(), PlatformError>;
    /// Запускает службу.
    fn start_service(&self, name: &str) -> Result<(), PlatformError>;
    /// Останавливает службу, НЕ удаляя её регистрацию. Идемпотентно для
    /// уже-остановленной — остановка несуществующей службы не ошибка.
    fn stop_service(&self, name: &str) -> Result<(), PlatformError>;
    /// Перезапускает службу (stop + start). Платформенно-оптимальный путь:
    /// Linux — systemctl restart, macOS — kickstart -k (уже kill+start),
    /// Windows — sc stop (ignore) + sc start.
    fn restart_service(&self, name: &str) -> Result<(), PlatformError>;
    /// Ограничивает права на файл (chmod 0600+chown / icacls readonly).
    fn restrict_file_permissions(&self, path: &Path, owner: &str) -> Result<(), PlatformError>;
}

/// Выбирает реализацию `PlatformServiceManager` для текущей ОС.
pub fn platform_manager() -> Result<Box<dyn PlatformServiceManager>, PlatformError> {
    #[cfg(target_os = "windows")]
    {
        Ok(Box::new(windows::WindowsService))
    }
    #[cfg(target_os = "linux")]
    {
        Ok(Box::new(linux::Systemd))
    }
    #[cfg(target_os = "macos")]
    {
        Ok(Box::new(macos::Launchd))
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        Err(PlatformError::Unsupported(
            "this operating system is not supported by adapterctl".into(),
        ))
    }
}

/// Генератор достаточно длинного (>= 32 символов) пароля для managed-docker
/// Postgres. Никогда не возвращает короткий статичный дефолт — правило #4.
pub fn generate_secure_password() -> String {
    use rand::Rng;
    const CHARS: &[u8] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*-_=+";
    let mut rng = rand::rng();
    (0..40)
        .map(|_| {
            let idx = rng.random_range(0..CHARS.len());
            CHARS[idx] as char
        })
        .collect()
}
