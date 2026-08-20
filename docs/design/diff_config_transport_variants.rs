//! DIFF — crates/adapterd/src/config.rs
//! Добавляет A2aClient/AcpClient варианты в AgentTransportConfig. Показан
//! ТОЛЬКО изменённый enum, остальной config.rs (Config, StorageConfig,
//! RuntimeConfig, RetentionConfig, validate()) — без изменений.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "driver", rename_all = "kebab-case")]
pub enum AgentTransportConfig {
    Stdio {
        command: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        working_dir: Option<PathBuf>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    HttpSse {
        endpoint: String,
        #[serde(default)]
        token_env: Option<String>,
        #[serde(default)]
        allow_http_development: bool,
    },
    // НОВОЕ:
    A2aClient {
        endpoint: String,
        #[serde(default)]
        token_env: Option<String>,
        #[serde(default)]
        allow_http_development: bool,
        #[serde(default = "default_a2a_timeout_seconds")]
        request_timeout_seconds: u64,
    },
    // НОВОЕ:
    AcpClient {
        command: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        working_dir: Option<PathBuf>,
    },
}

fn default_a2a_timeout_seconds() -> u64 {
    30
}

// ============================================================
// ПРАВКА в Config::validate() — тот же https-check, что уже есть для
// HttpSse, зеркально применён к A2aClient. Вставить рядом с существующей
// проверкой `if let AgentTransportConfig::HttpSse { endpoint, allow_http_development, .. } = &agent.transport`.
// ============================================================
//
// БЫЛО (существующий код, не менять):
//   if let AgentTransportConfig::HttpSse { endpoint, allow_http_development, .. } = &agent.transport {
//       if !allow_http_development && !endpoint.starts_with("https") {
//           return Err(ConfigError::Validation(format!("agent {} HTTP+SSE endpoint must use https", agent.id)));
//       }
//   }
//
// ДОБАВИТЬ рядом (новая ветка, тот же паттерн):
//   if let AgentTransportConfig::A2aClient { endpoint, allow_http_development, .. } = &agent.transport {
//       if !allow_http_development && !endpoint.starts_with("https") {
//           return Err(ConfigError::Validation(format!("agent {} A2A client endpoint must use https", agent.id)));
//       }
//   }

// ============================================================
// ПРАВКА crates/adapterd/src/main.rs — build_driver(), два новых match-рукава
// ============================================================
//
// БЫЛО (существующий код, структура match не меняется, только добавляются
// ветки):
//   async fn build_driver(agent: &AgentConfig) -> Result<Arc<dyn AgentDriver>, StartupError> {
//       match &agent.transport {
//           AgentTransportConfig::Stdio { command, args, working_dir, env } => { ... }
//           AgentTransportConfig::HttpSse { endpoint, token_env, allow_http_development } => { ... }
//           // НОВОЕ:
//           AgentTransportConfig::A2aClient { endpoint, token_env, allow_http_development, request_timeout_seconds } => {
//               let mut url = Url::parse(endpoint)
//                   .map_err(|e| StartupError::Driver(format!("invalid A2A endpoint for {}: {e}", agent.id)))?;
//               let bearer_token = match token_env {
//                   Some(name) => Some(env::var(name).map_err(|_| StartupError::MissingEnv(name.clone()))?),
//                   None => None,
//               };
//               let config = driver_a2a_client::A2aClientConfig {
//                   endpoint: url,
//                   bearer_token,
//                   request_timeout: Duration::from_secs(*request_timeout_seconds),
//               };
//               let driver = driver_a2a_client::A2aClientDriver::new(agent.id.clone(), config)
//                   .map_err(|e| StartupError::Driver(e.to_string()))?;
//               Ok(Arc::new(driver))
//           }
//           // НОВОЕ:
//           AgentTransportConfig::AcpClient { command, args, working_dir } => {
//               let config = driver_acp_client::AcpClientConfig {
//                   command: command.clone(),
//                   args: args.clone(),
//                   working_dir: working_dir.clone(),
//               };
//               let driver = driver_acp_client::AcpClientDriver::spawn(agent.id.clone(), config)
//                   .await
//                   .map_err(|e| StartupError::Driver(e.to_string()))?;
//               Ok(Arc::new(driver))
//           }
//       }
//   }
//
// Плюс: crates/adapterd/Cargo.toml — добавить
//   driver-a2a-client = { path = "../driver-a2a-client" }
//   driver-acp-client = { path = "../driver-acp-client" }
// и в main.rs use-секцию:
//   use driver_a2a_client::{A2aClientConfig, A2aClientDriver};
//   use driver_acp_client::{AcpClientConfig, AcpClientDriver};
