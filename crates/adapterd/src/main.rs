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
use config::{
    A2aWireFormat, AgentTransportConfig, AuthConfig, Config, McpTransportConfig, StorageConfig,
};
use driver_a2a_client::{A2aClientConfig, A2aClientDriver, A2aWireFormat as DriverA2aWireFormat};
use driver_acp_client::{AcpClientConfig, AcpClientDriver};
use driver_http_sse::{Credential, HttpSseDriver, HttpSseDriverConfig};
use driver_mcp::{McpDriver, McpDriverError, McpHttpConfig, McpStdioConfig};
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

    // Чтение .env ДО инициализации tracing: RUST_LOG сам может быть задан
    // через .env файл (не только в launchd EnvironmentVariables) — если
    // читать .env после init, try_from_default_env() уже прошёл.
    load_env_files(&config_path);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::load(config_path)?;
    let daemon = Daemon::build(config).await?;
    daemon.run().await;
    Ok(())
}

/// Определяет и читает `.env` файл для процесса. Два источника, в порядке
/// приоритета:
///
/// 1. Явный `ADAPTERD_ENV_FILE` (путь от launchd на macOS — plist передаёт
///    его через EnvironmentVariables, т.к. launchd не умеет EnvironmentFile=).
/// 2. Файл `.env` рядом с конфигом (ручной запуск `./adapterd adapter.yaml`
///    на Linux/Windows без systemd — там .env применяет systemd сам через
///    EnvironmentFile=-..., а при ручном запуске этого не происходит).
///
/// В обоих случаях читается простой `KEY=value` формат. Поддерживает:
/// пустые строки, строки-комментарии начинающиеся с `#`, обрезку whitespace
/// вокруг key/value. НЕ поддерживает (осознанно, не нужно для формата,
/// который install_flow.rs генерирует): кавычки вокруг значений,
/// export-префиксы, multiline значения, интерполяцию переменных.
///
/// std::env::set_var НЕ переопределяет переменные, которые уже заданы в
/// окружении процесса — .env файл заполняет только то, что ещё не задано,
/// не имеет приоритета над явным окружением.
fn load_env_files(config_path: &str) {
    if let Ok(explicit) = std::env::var("ADAPTERD_ENV_FILE") {
        load_env_file(&explicit);
        return;
    }

    // Ручной запуск: .env рядом с конфигом, если существует.
    let config = std::path::Path::new(config_path);
    if let Some(dir) = config.parent() {
        let candidate = dir.join(".env");
        if candidate.is_file() {
            if let Some(path) = candidate.to_str() {
                load_env_file(path);
            }
        }
    }
}

fn load_env_file(env_file_path: &str) {
    let contents = match std::fs::read_to_string(env_file_path) {
        Ok(contents) => contents,
        Err(e) => {
            // tracing ещё не инициализирован в этой точке — используем
            // eprintln! напрямую, не tracing::warn!. Это единственное
            // место в main(), где это оправдано именно из-за порядка
            // инициализации (см. комментарий выше про RUST_LOG timing).
            eprintln!("[adapterd] warning: {env_file_path} set but unreadable: {e}");
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
        if std::env::var(key).is_err() {
            // Только если ещё не задано — явное окружение процесса всегда
            // приоритетнее содержимого .env файла.
            std::env::set_var(key, value);
        }
    }
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
            match build_driver(agent, Arc::downgrade(&registry)).await {
                Ok(driver) => {
                    registry.register(RegisteredAgent::new(
                        adapter_model::AgentId(agent.id.clone()),
                        agent.skills.clone(),
                        driver,
                        config.agent_limits(agent),
                    ));
                }
                Err(error) => {
                    // Graceful degradation: агент не поднялся (например MCP
                    // discovery timeout) — логируем и продолжаем с остальными.
                    // Весь процесс не падает из-за одного сломанного агента
                    // (docs/driver-mcp-spec.md раздел 5 п.6).
                    tracing::error!(
                        agent = %agent.id,
                        error = %error,
                        "agent failed to start, skipping"
                    );
                }
            }
        }
        if registry.agents().is_empty() {
            return Err(StartupError::Driver(
                "no agents started successfully".into(),
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

async fn build_driver(
    agent: &config::AgentConfig,
    registry: std::sync::Weak<AgentRegistry>,
) -> Result<Arc<dyn AgentDriver>, StartupError> {
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
            let connect = async {
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
                        adapter_model::AgentId(agent.id.clone()),
                        registry.clone(),
                    )
                    .await
                    .map_err(|e: McpDriverError| StartupError::Driver(e.to_string()))?,
                    McpTransportConfig::Http {
                        endpoint,
                        token_env,
                        allow_http_development,
                    } => {
                        if !*allow_http_development && !endpoint.starts_with("https://") {
                            return Err(StartupError::Driver(format!(
                                "agent {} MCP HTTP endpoint must use https",
                                agent.id
                            )));
                        }
                        let token = match token_env {
                            Some(name) => Some(
                                env::var(name)
                                    .map_err(|_| StartupError::MissingEnv(name.clone()))?,
                            ),
                            None => None,
                        };
                        McpDriver::connect_http(
                            agent.id.clone(),
                            McpHttpConfig {
                                endpoint: endpoint.clone(),
                                token,
                            },
                            allowed_tools.clone(),
                            Duration::from_secs(agent.limits.default_timeout_seconds),
                            adapter_model::AgentId(agent.id.clone()),
                            registry.clone(),
                        )
                        .await
                        .map_err(|e: McpDriverError| StartupError::Driver(e.to_string()))?
                    }
                };
                Ok::<_, StartupError>(driver as Arc<dyn AgentDriver>)
            };
            // discovery_timeout_seconds ограничивает весь connect + tools/list.
            // Если discovery не уложился — агент не поднимается, остальные
            // продолжают (graceful degradation, см. Daemon::build).
            match tokio::time::timeout(Duration::from_secs(*discovery_timeout_seconds), connect)
                .await
            {
                Ok(result) => result,
                Err(_elapsed) => Err(StartupError::Driver(format!(
                    "agent {} MCP discovery timed out after {}s",
                    agent.id, discovery_timeout_seconds
                ))),
            }
        }
        AgentTransportConfig::A2aClient {
            endpoint,
            token_env,
            allow_http_development,
            request_timeout_seconds,
            wire_format,
        } => {
            let url = Url::parse(endpoint).map_err(|e| {
                StartupError::Driver(format!("invalid A2A endpoint for {}: {e}", agent.id))
            })?;
            if !*allow_http_development && url.scheme() != "https" {
                return Err(StartupError::Driver(format!(
                    "agent {} A2A client endpoint must use https",
                    agent.id
                )));
            }
            let token = match token_env {
                Some(name) => {
                    Some(env::var(name).map_err(|_| StartupError::MissingEnv(name.clone()))?)
                }
                None => None,
            };
            let config = A2aClientConfig {
                endpoint: url.to_string(),
                token,
                wire_format: match wire_format {
                    A2aWireFormat::Sdk => DriverA2aWireFormat::Sdk,
                    A2aWireFormat::Spec => DriverA2aWireFormat::Spec,
                    A2aWireFormat::Auto => DriverA2aWireFormat::Auto,
                },
                timeout_secs: *request_timeout_seconds,
            };
            let driver =
                A2aClientDriver::new(config).map_err(|e| StartupError::Driver(e.to_string()))?;
            Ok(Arc::new(driver))
        }
        AgentTransportConfig::AcpClient {
            command,
            args,
            working_dir,
        } => {
            let config = AcpClientConfig {
                command: command.clone(),
                args: args.clone(),
                working_dir: working_dir.clone(),
            };
            let driver = AcpClientDriver::spawn(agent.id.clone(), config)
                .await
                .map_err(|e| StartupError::Driver(e.to_string()))?;
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
            load_env_file(path);
            assert_eq!(env::var("TEST_ADAPTERD_ENV_VAR_1").unwrap(), "hello");
            env::remove_var("TEST_ADAPTERD_ENV_VAR_1");
        });
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        env::remove_var("TEST_ADAPTERD_ENV_VAR_2");
        with_temp_env_file("# a comment\n\nTEST_ADAPTERD_ENV_VAR_2=world\n", |path| {
            load_env_file(path);
            assert_eq!(env::var("TEST_ADAPTERD_ENV_VAR_2").unwrap(), "world");
            env::remove_var("TEST_ADAPTERD_ENV_VAR_2");
        });
    }

    #[test]
    fn does_not_override_existing_process_env() {
        env::set_var("TEST_ADAPTERD_ENV_VAR_3", "explicit-value");
        with_temp_env_file("TEST_ADAPTERD_ENV_VAR_3=from-file\n", |path| {
            load_env_file(path);
            assert_eq!(
                env::var("TEST_ADAPTERD_ENV_VAR_3").unwrap(),
                "explicit-value",
                "explicit process env must win over .env file content"
            );
            env::remove_var("TEST_ADAPTERD_ENV_VAR_3");
        });
    }

    #[test]
    fn explicit_adapterd_env_file_wins_over_config_sibling() {
        // Оба источника задают одну переменную с разными значениями —
        // явный ADAPTERD_ENV_FILE должен иметь приоритет.
        env::remove_var("TEST_ADAPTERD_ENV_VAR_4");
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().to_str().unwrap();
        std::fs::write(
            dir.path().join(".env"),
            "TEST_ADAPTERD_ENV_VAR_4=from-sibling\n",
        )
        .unwrap();

        with_temp_env_file("TEST_ADAPTERD_ENV_VAR_4=from-explicit\n", |explicit| {
            env::set_var("ADAPTERD_ENV_FILE", explicit);
            let config_path = format!("{config_dir}/adapter.yaml");
            load_env_files(&config_path);
            assert_eq!(
                env::var("TEST_ADAPTERD_ENV_VAR_4").unwrap(),
                "from-explicit",
                "explicit ADAPTERD_ENV_FILE must take precedence over config-sibling .env"
            );
            env::remove_var("TEST_ADAPTERD_ENV_VAR_4");
            env::remove_var("ADAPTERD_ENV_FILE");
        });
    }

    #[test]
    fn config_sibling_env_loaded_for_manual_run() {
        // Ручной запуск ./adapterd path/to/adapter.yaml без ADAPTERD_ENV_FILE:
        // .env рядом с конфигом должен подхватиться.
        env::remove_var("TEST_ADAPTERD_ENV_VAR_5");
        env::remove_var("ADAPTERD_ENV_FILE");
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().to_str().unwrap();
        std::fs::write(
            dir.path().join(".env"),
            "TEST_ADAPTERD_ENV_VAR_5=from-sibling\n",
        )
        .unwrap();

        let config_path = format!("{config_dir}/adapter.yaml");
        load_env_files(&config_path);
        assert_eq!(
            env::var("TEST_ADAPTERD_ENV_VAR_5").unwrap(),
            "from-sibling",
            "config-sibling .env must be loaded on manual run"
        );
        env::remove_var("TEST_ADAPTERD_ENV_VAR_5");
    }

    #[test]
    fn no_op_when_neither_env_source_present() {
        env::remove_var("ADAPTERD_ENV_FILE");
        // Пустой dir без .env и без явного файла — не должно паниковать
        // или иметь побочные эффекты (обычный Linux/Windows путь).
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("adapter.yaml");
        load_env_files(config_path.to_str().unwrap());
    }
}
