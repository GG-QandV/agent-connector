//! `adapterd` — composition root and daemon lifecycle.
//!
//! This binary owns config loading, storage/driver construction, background
//! retention cleanup and graceful shutdown. It deliberately does not contain
//! A2A/ACP wire server code: those protocol routers receive Arc<AdapterCore>.
//!
//! Expected workspace crates:
//! adapter_core, adapter_model, adapter_store_contract, adapterd_config,
//! memory_task_store, sqlite_task_store_adapter, postgres_task_store_adapter,
//! driver_stdio, driver_http_sse.

use std::{env, path::Path, sync::Arc, time::Duration};

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
    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "./adapter.yaml".into());
    let config = Config::load(config_path)?;
    let daemon = Daemon::build(config).await?;
    daemon.run().await;
    Ok(())
}

struct Daemon {
    config: Config,
    core: Arc<AdapterCore>,
    cleanup_task: JoinHandle<()>,
}

impl Daemon {
    async fn build(config: Config) -> Result<Self, StartupError> {
        let store = build_store(&config).await?;
        let registry = Arc::new(AgentRegistry::new());
        for agent in &config.agents {
            let driver = build_driver(agent).await?;
            registry.register(RegisteredAgent::new(
                adapter_model::AgentId(agent.id.clone()),
                agent.skills.clone(),
                driver,
                config.agent_limits(agent),
            ));
        }
        let core = Arc::new(AdapterCore::new(
            store.clone(),
            registry,
            Arc::new(AllowAllPolicy), // replace with PolicyEngine in remote profile
            config.runtime.max_concurrent_tasks,
        ));
        let retention = config.retention_policy();
        let interval = Duration::from_secs(config.runtime.cleanup_interval_seconds);
        let cleanup_store = store.clone();
        let cleanup_task = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match cleanup_store.cleanup(&retention).await {
                    Ok(report) => tracing::info!(
                        tasks = report.tasks_deleted,
                        events = report.events_deleted,
                        "retention cleanup complete"
                    ),
                    Err(error) => tracing::warn!(%error, "retention cleanup failed"),
                }
            }
        });
        Ok(Self {
            config,
            core,
            cleanup_task,
        })
    }

    async fn run(self) {
        tracing::info!(mode=?self.config.mode, agents=self.config.agents.len(), "adapterd started");
        // A2A HTTP router and/or ACP stdio loop are started here as independent
        // tasks, each holding self.core.clone(). Their absence does not affect
        // storage, driver lifecycle or cleanup.
        let _ = signal::ctrl_c().await;
        tracing::info!("shutdown requested");
        self.cleanup_task.abort();
        // Future TaskSupervisor performs: stop accepting work -> readiness false
        // -> wait/cancel active tasks within shutdown_grace -> close protocol streams.
        let _ = self.core;
        tracing::info!("adapterd stopped");
    }
}

async fn build_store(config: &Config) -> Result<Arc<dyn TaskStore>, StartupError> {
    match &config.storage {
        StorageConfig::Memory => Ok(Arc::new(MemoryTaskStore::new())),
        StorageConfig::Sqlite { path } => {
            ensure_parent_dir(path)?;
            let store = SqliteTaskStore::open(path)
                .await
                .map_err(|e| StartupError::Storage(e.to_string()))?;
            Ok(Arc::new(store))
        }
        StorageConfig::Postgres {
            dsn_env,
            schema,
            max_connections,
        } => {
            let dsn = env::var(dsn_env).map_err(|_| StartupError::MissingEnv(dsn_env.clone()))?;
            let store = PostgresTaskStore::connect(&dsn, schema, *max_connections)
                .await
                .map_err(|e| StartupError::Storage(e.to_string()))?;
            Ok(Arc::new(store))
        }
    }
}

async fn build_driver(agent: &config::AgentConfig) -> Result<Arc<dyn AgentDriver>, StartupError> {
    match &agent.transport {
        AgentTransportConfig::Stdio {
            command,
            args,
            working_dir,
            env,
        } => {
            let config = StdioDriverConfig {
                id: agent.id.clone(),
                command: command.clone(),
                args: args.clone(),
                working_dir: working_dir.clone(),
                env: env.clone(),
                ..Default::default()
            };
            Ok(Arc::new(StdioDriver::new(config)))
        }
        AgentTransportConfig::HttpSse {
            endpoint,
            token_env,
            allow_http_development,
        } => {
            let mut config = HttpSseDriverConfig::new(Url::parse(endpoint).map_err(|e| {
                StartupError::Driver(format!("invalid endpoint for {}: {e}", agent.id))
            })?);
            config.id = agent.id.clone();
            config.require_https = !allow_http_development;
            config.credential = match token_env {
                Some(name) => Credential::Bearer(
                    env::var(name).map_err(|_| StartupError::MissingEnv(name.clone()))?,
                ),
                None => Credential::None,
            };
            let driver =
                HttpSseDriver::new(config).map_err(|e| StartupError::Driver(e.to_string()))?;
            Ok(Arc::new(driver))
        }
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), StartupError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StartupError::Storage(e.to_string()))?;
    }
    Ok(())
}
