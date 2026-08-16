//! ПРАВКА к crates/adapterd/src/main.rs — добавляет чтение ADAPTERD_ENV_FILE
//! ДО tracing_subscriber::init() и ДО Config::load(). Подтверждено точным
//! текущим содержимым fn main() через search_files (file:432):
//!
//! Текущий код (без изменений структуры, только вставка):
//!   #[tokio::main]
//!   async fn main() -> Result<(), StartupError> {
//!       tracing_subscriber::fmt()
//!           .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
//!               .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
//!           .init();
//!       let config_path = env::args().nth(1).unwrap_or_else(|| "adapter.yaml".into());
//!       let config = Config::load(config_path)?;
//!       let daemon = Daemon::build(config).await?;
//!       daemon.run().await
//!       Ok(())
//!   }
//!
//! Почему вставка ИМЕННО перед tracing_subscriber::fmt().init(), не после:
//!   RUST_LOG сам может быть задан через .env файл (не только напрямую в
//!   launchd EnvironmentVariables) — если читать .env ПОСЛЕ инициализации
//!   subscriber, переменная RUST_LOG из .env не подхватится try_from_default_env(),
//!   потому что она читает std::env в момент вызова, который уже прошёл.
//!
//! Почему без внешнего dotenv/dotenvy crate:
//!   Формат .env, который install_flow.rs реально генерирует — простой
//!   `KEY=value`, без кавычек/экспортов/multiline (см. write_config_files()
//!   в install_flow.rs: `env_text.push_str(&format!("{key}={value}\n"))`).
//!   Полноценный dotenv-парсер — избыточная зависимость для этого случая;
//!   ~15 строк здесь покрывают ровно то, что install_flow.rs пишет.

use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

mod config;

use adapter_core::{AdapterCore, AgentDriver, AgentRegistry, AllowAllPolicy, RegisteredAgent};
use adapter_store_contract::TaskStore;
use config::{AgentTransportConfig, Config, StorageConfig};
use driver_http_sse::{Credential, HttpSseDriver, HttpSseDriverConfig};
use driver_stdio::{StdioDriver, StdioDriverConfig};
use memory_task_store::MemoryTaskStore;
use postgres_task_store_adapter::PostgresTaskStore;
use sqlite_task_store_adapter::SqliteTaskStore;
use thiserror::Error;
use tokio::{signal, task::JoinHandle, time};
use url::Url;

#[derive(Error, Debug)]
enum StartupError {
    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("driver error: {0}")]
    Driver(String),
    #[error("environment variable is missing: {0}")]
    MissingEnv(String),
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    // НОВОЕ: читаем ADAPTERD_ENV_FILE ДО инициализации tracing и ДО
    // Config::load(). systemd делает это сам через EnvironmentFile=-...;
    // launchd (macOS) не умеет — plist передаёт нам только путь к файлу
    // через EnvironmentVariables, читать содержимое должны мы сами.
    //
    // На Linux (systemd) ADAPTERD_ENV_FILE обычно НЕ задана вообще —
    // .env уже применён systemd до старта процесса, эта функция там
    // просто ничего не находит и тихо пропускается (env::var возвращает
    // Err, ветка if let Ok(...) не выполняется — нет побочных эффектов,
    // безопасно оставлять этот код на всех платформах, не только macOS).
    load_env_file_if_specified();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = env::args().nth(1).unwrap_or_else(|| "adapter.yaml".into());
    let config = Config::load(config_path)?;
    let daemon = Daemon::build(config).await?;
    daemon.run().await
}

/// Читает простой `KEY=value` формат из файла, путь к которому задан в
/// ADAPTERD_ENV_FILE. Поддерживает: пустые строки, строки-комментарии
/// начинающиеся с `#`, обрезку whitespace вокруг key/value. НЕ поддерживает
/// (осознанно, не нужно для формата, который install_flow.rs генерирует):
/// кавычки вокруг значений, export-префиксы, multiline значения,
/// интерполяцию переменных.
///
/// std::env::set_var не переопределяет переменные, которые уже заданы в
/// окружении процесса (например, если launchd/пользователь явно передал
/// что-то через EnvironmentVariables в plist напрямую) — .env файл
/// заполняет только то, что ещё не задано, не имеет приоритета над
/// явным окружением.
fn load_env_file_if_specified() {
    let Ok(env_file_path) = env::var("ADAPTERD_ENV_FILE") else {
        return; // не задано — обычный путь на Linux/Windows, ничего не делаем
    };

    let path = Path::new(&env_file_path);
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) => {
            // tracing ещё не инициализирован в этой точке — используем
            // eprintln! напрямую, не tracing::warn!. Это единственное
            // место в main(), где это оправдано именно из-за порядка
            // инициализации (см. комментарий выше про RUST_LOG timing).
            eprintln!(
                "[adapterd] warning: ADAPTERD_ENV_FILE={} set but unreadable: {e}",
                env_file_path
            );
            return;
        }
    };

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            eprintln!("[adapterd] warning: skipping malformed line in {env_file_path}: {line}");
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if env::var(key).is_err() {
            // Только если ещё не задано — явное окружение процесса всегда
            // приоритетнее содержимого .env файла.
            env::set_var(key, value);
        }
    }
}

// --- остальной файл (struct Daemon, impl Daemon, build_store,
// build_driver, ensure_parent_dir) НЕ меняется относительно текущей
// версии в репозитории — эта правка касается только начала main() и
// добавления одной новой функции load_env_file_if_specified(). ---

#[cfg(test)]
mod env_file_tests {
    use super::*;
    use std::io::Write;

    fn with_temp_env_file(content: &str, test_fn: impl FnOnce(&str)) {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        let path = file.path().to_str().unwrap().to_string();
        test_fn(&path);
    }

    #[test]
    fn loads_simple_key_value_pairs() {
        env::remove_var("TEST_ADAPTERD_ENV_VAR_1");
        with_temp_env_file("TEST_ADAPTERD_ENV_VAR_1=hello\n", |path| {
            env::set_var("ADAPTERD_ENV_FILE", path);
            load_env_file_if_specified();
            assert_eq!(env::var("TEST_ADAPTERD_ENV_VAR_1").unwrap(), "hello");
            env::remove_var("TEST_ADAPTERD_ENV_VAR_1");
            env::remove_var("ADAPTERD_ENV_FILE");
        });
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        env::remove_var("TEST_ADAPTERD_ENV_VAR_2");
        with_temp_env_file("# a comment\n\nTEST_ADAPTERD_ENV_VAR_2=world\n", |path| {
            env::set_var("ADAPTERD_ENV_FILE", path);
            load_env_file_if_specified();
            assert_eq!(env::var("TEST_ADAPTERD_ENV_VAR_2").unwrap(), "world");
            env::remove_var("TEST_ADAPTERD_ENV_VAR_2");
            env::remove_var("ADAPTERD_ENV_FILE");
        });
    }

    #[test]
    fn does_not_override_existing_process_env() {
        env::set_var("TEST_ADAPTERD_ENV_VAR_3", "explicit-value");
        with_temp_env_file("TEST_ADAPTERD_ENV_VAR_3=from-file\n", |path| {
            env::set_var("ADAPTERD_ENV_FILE", path);
            load_env_file_if_specified();
            assert_eq!(
                env::var("TEST_ADAPTERD_ENV_VAR_3").unwrap(),
                "explicit-value",
                "explicit process env must win over .env file content"
            );
            env::remove_var("TEST_ADAPTERD_ENV_VAR_3");
            env::remove_var("ADAPTERD_ENV_FILE");
        });
    }

    #[test]
    fn no_op_when_adapterd_env_file_not_set() {
        env::remove_var("ADAPTERD_ENV_FILE");
        // Не должно паниковать или иметь побочные эффекты — критично для
        // Linux/Windows путей, где эта переменная никогда не задаётся.
        load_env_file_if_specified();
    }
}
