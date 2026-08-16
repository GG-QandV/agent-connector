//! ВТОРАЯ ПРАВКА к install_flow.rs (первая была config_template интеграция) —
//! добавляет preflight_check() ДО cargo build, закрывая реальный прод-баг:
//! раньше пользователь узнавал о недоступности Docker только после
//! --confirm-docker + минут ожидания сборки + внутри ensure_running().
//!
//! Вставить эту проверку в run_install() сразу после проверки
//! --confirm-docker, перед блоком `if !skip_build`.

use crate::managed_docker;
use crate::StorageChoice;

// Фрагмент run_install() — показан только изменённый участок, остальное
// тело функции (config_template::load(), platform.ensure_service_user(),
// resolve_storage(), write_config_files(), register_service()) не меняется.
pub async fn run_install_preflight_fragment(
    choice: StorageChoice,
    confirm_docker: bool,
) -> Result<(), crate::install_flow::InstallError> {
    if choice == StorageChoice::ManagedDockerPostgres {
        if !confirm_docker {
            return Err(crate::install_flow::InstallError::Storage(
                "ManagedDockerPostgres selected without --confirm-docker".into()
            ));
        }

        // НОВОЕ: preflight ДО cargo build. Если Docker недоступен или в
        // неправильном containers mode — пользователь узнаёт об этом за
        // секунды, не после минут компиляции adapterd в release режиме.
        managed_docker::preflight_check().await.map_err(|e| {
            crate::install_flow::InstallError::Storage(format!(
                "Docker preflight check failed before proceeding with managed-docker-postgres: {e}"
            ))
        })?;
    }

    Ok(())
    // ... остальная функция run_install() продолжается без изменений:
    // config_template::load(), cargo build (только теперь мы точно знаем,
    // что Docker готов, если он вообще нужен), ensure_service_user(),
    // resolve_storage() (который внутри для ManagedDockerPostgres тоже
    // вызовет ensure_running() -> preflight_check() повторно — небольшая
    // избыточность, но безопасная: preflight_check() идемпотентен, не
    // мутирует состояние, только проверяет), и так далее.
}
