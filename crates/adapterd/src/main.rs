//! `adapterd` — composition root и daemon lifecycle.
//!
//! Владеет config, storage, drivers, background retention-cleanup и запускает
//! A2A HTTP router (a2a-server) + health/readiness. ACP stdio runtime
//! поднимается отдельным процессом/профилем.

use std::{env, path::Path, sync::Arc, time::Duration};

mod config;

use adapter_core::{
    AdapterCore, AgentDriver, AgentRegistry, AllowAllPolicy, BearerTokenPolicy, RegisteredAgent,
    TokenGrant,
};
use adapter_store_contract::TaskStore;
use config::{AgentTransportConfig, AuthConfig, Config, McpTransportConfig, StorageConfig};
use driver_http_sse::{Credential, HttpSseDriver, HttpSseDriverConfig};
use driver_mcp::{McpDriver, McpDriverError, McpStdioConfig};
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

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
    store: Arc<dyn TaskStore>,
    registry: Arc<AgentRegistry>,
    auth_state: Option<protocol_a2a_server::AuthState>,
    cleanup_task: JoinHandle<()>,
    draining: Arc<std::sync::atomic::AtomicBool>,
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
        let auth_state = build_auth_state(&config.auth)?;
        let core = Arc::new(AdapterCore::new(
            store.clone(),
            registry.clone(),
            match &auth_state {
                Some(state) => state.policy.clone() as Arc<dyn adapter_core::PolicyEngine>,
                None => Arc::new(AllowAllPolicy),
            },
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
            store,
            registry,
            auth_state,
            cleanup_task,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    async fn run(self) {
        let addr = env::var("ADAPTERD_LISTEN").unwrap_or_else(|_| "0.0.0.0:8348".into());
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::error!(error = %e, addr = %addr, "cannot bind http listener");
                return;
            }
        };
        tracing::info!(mode=?self.config.mode, agents=self.config.agents.len(), addr=%addr, "adapterd started");

        let auth_state = self.auth_state;
        let executor = Arc::new(match &auth_state {
            Some(state) => protocol_a2a_server::AdapterAgentExecutor::with_auth(
                self.core.clone(),
                "a2a-client",
                state.policy.clone(),
            ),
            None => protocol_a2a_server::AdapterAgentExecutor::new(self.core.clone(), "a2a-client"),
        });
        let task_store = Arc::new(protocol_a2a_server::AdapterTaskStore::new(
            self.store.clone(),
        ));
        let card = Arc::new(protocol_a2a_server::AdapterCardProducer::new(
            self.registry.clone(),
            protocol_a2a_server::AdapterCardConfig {
                name: "agent-connector".into(),
                description: "Universal Agent Adapter Runtime".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                endpoint_url: format!("http://{addr}"),
            },
        ));
        let health = protocol_a2a_server::HealthState::new(
            self.store.clone(),
            self.registry.clone(),
            self.draining.clone(),
        );
        let app = protocol_a2a_server::build_router(executor, task_store, card, health, auth_state);

        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "http server error");
            }
        });

        let _ = signal::ctrl_c().await;
        tracing::info!("shutdown requested");
        self.draining
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.cleanup_task.abort();
        server.abort();
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
        AgentTransportConfig::Mcp {
            transport,
            allowed_tools,
            discovery_timeout_seconds,
        } => {
            let _ = discovery_timeout_seconds; // discovery выполняется в connect_*; см. spec раздел 5
            let driver = match transport {
                McpTransportConfig::Stdio { command, args, env } => McpDriver::connect_stdio(
                    agent.id.clone(),
                    McpStdioConfig {
                        command: command.clone(),
                        args: args.clone(),
                        env: env.clone(),
                    },
                    allowed_tools.clone(),
                    Duration::from_secs(agent.limits.default_timeout_seconds),
                )
                .await
                .map_err(|e: McpDriverError| StartupError::Driver(e.to_string()))?,
                McpTransportConfig::Http { .. } => {
                    return Err(StartupError::Driver(
                        "MCP HTTP transport not yet implemented".into(),
                    ));
                }
            };
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

/// AuthState для Axum middleware, если bearer-аутентификация включена.
fn build_auth_state(
    auth: &AuthConfig,
) -> Result<Option<protocol_a2a_server::AuthState>, StartupError> {
    if auth.bearer_tokens.is_empty() {
        return Ok(None);
    }
    let entries = auth
        .bearer_tokens
        .iter()
        .map(|entry| {
            (
                entry.token_env.clone(),
                TokenGrant {
                    caller_id: adapter_core::CallerId(entry.caller_id.clone()),
                    allowed_scopes: entry.allowed_scopes.clone(),
                },
            )
        })
        .collect();
    let policy = Arc::new(
        BearerTokenPolicy::from_env(entries)
            .map_err(|e| StartupError::Config(config::ConfigError::Validation(e.to_string())))?,
    );
    Ok(Some(protocol_a2a_server::AuthState { policy }))
}
