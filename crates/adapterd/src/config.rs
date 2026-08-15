//! `adapterd-config` — validated configuration for Adapter Daemon.
//!
//! Configuration supports zero-config-ish local SQLite, existing Postgres and
//! installer-managed Docker Postgres. Docker is never managed by adapterd;
//! installer writes DSN/config and adapterd only connects to the database.
//!
//! Cargo.toml dependencies:
//! serde = { version = "1", features = ["derive"] }
//! serde_yaml = "0.9"
//! thiserror = "2"

use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub agents: Vec<AgentConfig>,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Local,
    Remote,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum StorageConfig {
    Memory,
    Sqlite {
        #[serde(default = "default_sqlite_path")]
        path: PathBuf,
    },
    Postgres {
        dsn_env: String,
        #[serde(default = "default_schema")]
        schema: String,
        #[serde(default = "default_pg_connections")]
        max_connections: u32,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Sqlite {
            path: default_sqlite_path(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_global_tasks")]
    pub max_concurrent_tasks: usize,
    #[serde(default = "default_cleanup_seconds")]
    pub cleanup_interval_seconds: u64,
    #[serde(default = "default_shutdown_seconds")]
    #[allow(dead_code)]
    pub shutdown_grace_seconds: u64,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: default_global_tasks(),
            cleanup_interval_seconds: default_cleanup_seconds(),
            shutdown_grace_seconds: default_shutdown_seconds(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RetentionConfig {
    #[serde(default = "default_task_days")]
    pub task_ttl_days: u64,
    #[serde(default = "default_event_days")]
    pub event_ttl_days: u64,
    #[serde(default = "default_key_hours")]
    pub idempotency_ttl_hours: u64,
    #[serde(default = "default_cleanup_batch")]
    pub cleanup_batch_size: u32,
}
impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            task_ttl_days: default_task_days(),
            event_ttl_days: default_event_days(),
            idempotency_ttl_hours: default_key_hours(),
            cleanup_batch_size: default_cleanup_batch(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct AgentConfig {
    pub id: String,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub limits: AgentLimitsConfig,
    #[serde(flatten)]
    pub transport: AgentTransportConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AgentLimitsConfig {
    #[serde(default = "default_agent_tasks")]
    pub max_concurrent_tasks: usize,
    #[serde(default = "default_agent_queue")]
    pub max_queued_tasks: usize,
    #[serde(default = "default_input_bytes")]
    pub max_input_bytes: usize,
    #[serde(default = "default_event_bytes")]
    pub max_event_bytes: usize,
    #[serde(default = "default_task_timeout")]
    pub default_timeout_seconds: u64,
}
impl Default for AgentLimitsConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: default_agent_tasks(),
            max_queued_tasks: default_agent_queue(),
            max_input_bytes: default_input_bytes(),
            max_event_bytes: default_event_bytes(),
            default_timeout_seconds: default_task_timeout(),
        }
    }
}

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
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("cannot read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid config: {0}")]
    Validation(String),
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.runtime.max_concurrent_tasks == 0 {
            return Err(ConfigError::Validation(
                "runtime.max_concurrent_tasks must be > 0".into(),
            ));
        }
        if self.agents.is_empty() {
            return Err(ConfigError::Validation(
                "at least one agent is required".into(),
            ));
        }
        let mut ids = std::collections::HashSet::new();
        for agent in &self.agents {
            if agent.id.trim().is_empty() || !ids.insert(&agent.id) {
                return Err(ConfigError::Validation(format!(
                    "invalid or duplicate agent id: {}",
                    agent.id
                )));
            }
            if agent.limits.max_concurrent_tasks == 0 {
                return Err(ConfigError::Validation(format!(
                    "agent {} max_concurrent_tasks must be > 0",
                    agent.id
                )));
            }
            if let AgentTransportConfig::HttpSse {
                endpoint,
                allow_http_development,
                ..
            } = &agent.transport
            {
                if !*allow_http_development && !endpoint.starts_with("https://") {
                    return Err(ConfigError::Validation(format!(
                        "agent {} HTTP/SSE endpoint must use https",
                        agent.id
                    )));
                }
            }
        }
        if let StorageConfig::Postgres { schema, .. } = &self.storage {
            if schema.is_empty()
                || !schema
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(ConfigError::Validation(
                    "postgres schema must match [A-Za-z0-9_]+".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn retention_policy(&self) -> adapter_store_contract::RetentionPolicy {
        adapter_store_contract::RetentionPolicy {
            task_ttl: Duration::from_secs(self.retention.task_ttl_days * 86_400),
            event_ttl: Duration::from_secs(self.retention.event_ttl_days * 86_400),
            idempotency_ttl: Duration::from_secs(self.retention.idempotency_ttl_hours * 3_600),
            cleanup_batch_size: self.retention.cleanup_batch_size,
        }
    }

    pub fn agent_limits(&self, agent: &AgentConfig) -> adapter_model::AgentLimits {
        adapter_model::AgentLimits {
            max_concurrent_tasks: agent.limits.max_concurrent_tasks,
            max_queued_tasks: agent.limits.max_queued_tasks,
            max_input_bytes: agent.limits.max_input_bytes,
            max_event_bytes: agent.limits.max_event_bytes,
            default_timeout: Duration::from_secs(agent.limits.default_timeout_seconds),
        }
    }
}

fn default_sqlite_path() -> PathBuf {
    PathBuf::from("./data/adapter.db")
}
fn default_schema() -> String {
    "agent_adapter".into()
}
fn default_pg_connections() -> u32 {
    10
}
fn default_global_tasks() -> usize {
    32
}
fn default_cleanup_seconds() -> u64 {
    3_600
}
fn default_shutdown_seconds() -> u64 {
    30
}
fn default_task_days() -> u64 {
    7
}
fn default_event_days() -> u64 {
    7
}
fn default_key_hours() -> u64 {
    24
}
fn default_cleanup_batch() -> u32 {
    1_000
}
fn default_agent_tasks() -> usize {
    4
}
fn default_agent_queue() -> usize {
    32
}
fn default_input_bytes() -> usize {
    1024 * 1024
}
fn default_event_bytes() -> usize {
    256 * 1024
}
fn default_task_timeout() -> u64 {
    900
}
