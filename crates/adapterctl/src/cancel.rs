//! crates/adapterctl/src/cancel.rs — graceful cancel helper (часть 1+3 из
//! adapterctl_graceful_cancel.rs): `run_cancellable()` оборачивает future в
//! select! с ctrl_c(). Часть 2 (docker pull) встроена в managed_docker.rs;
//! atomic backup — в postgres_lifecycle.rs.

#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("operation '{0}' was interrupted (Ctrl+C)")]
    Interrupted(String),
}

/// Обёртывает future в select! с ctrl_c(), возвращает Err(Interrupted)
/// вместо мгновенного обрыва процесса — вызывающий код получает шанс
/// сделать cleanup (удалить .tmp, напечатать статус).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completes_when_not_interrupted() {
        let result = run_cancellable("test", async { 42 }).await;
        assert_eq!(result.unwrap(), 42);
    }
}
