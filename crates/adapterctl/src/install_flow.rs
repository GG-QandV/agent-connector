//! crates/adapterctl/src/install_flow.rs — связывает StorageChoice ->
//! ManagedPostgresPlan/DSN validation -> запись adapter.yaml + .env ->
//! platform service registration -> InstallSummary. Слияние черновиков:
//!   - adapterctl_install_flow.rs (база)
//!   - adapterctl_install_flow_patch.rs (config_template::load + uninstall_flow)
//!   - adapterctl_install_flow_preflight_patch.rs (Docker preflight до cargo build)

use crate::config_template;
use crate::managed_docker::{self, DockerError, ManagedPostgresPlan};
use crate::platform::{PlatformError, PlatformServiceManager, ServiceContext, StorageChoice};
use std::io::Write;
use std::path::{Path, PathBuf};

const DSN_ENV_VAR_NAME: &str = "ADAPTER_CONNECTOR_PG_DSN";

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("build failed: {0}")]
    Build(String),
    #[error("storage setup failed: {0}")]
    Storage(String),
    #[error(transparent)]
    Docker(#[from] DockerError),
    #[error(transparent)]
    Platform(#[from] PlatformError),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no TTY available for interactive prompt — pass --storage explicitly in non-interactive environments")]
    NoTty,
    #[error("DSN validation failed: {0}")]
    DsnValidation(String),
}

pub struct ResolvedStorage {
    pub yaml_fragment: serde_yaml::Value,
    pub env_vars: Vec<(String, String)>,
    pub docker_summary: Option<DockerSummary>,
}

pub struct DockerSummary {
    pub host_port: Option<u16>,
    pub volume_name: String,
    pub container_name: String,
}

/// Шаг 1: определить StorageChoice — из флага, либо интерактивным prompt'ом.
pub fn resolve_storage_choice(flag: Option<StorageChoice>) -> Result<StorageChoice, InstallError> {
    if let Some(choice) = flag {
        return Ok(choice);
    }

    if !atty::is(atty::Stream::Stdin) {
        return Err(InstallError::NoTty);
    }

    println!("Select a storage backend for agent-connector:");
    println!(
        "  1) SQLite            — single file, no external dependencies, good for single-node/dev"
    );
    println!("  2) Existing Postgres — you already have a Postgres instance to connect to");
    println!("  3) Managed Docker Postgres — installer runs an isolated Postgres container for this instance only");
    println!("  4) External managed Postgres — RDS, Neon, Supabase, Cloud SQL, etc. (same as #2, different wording)");
    print!("Choice [1-4]: ");
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    match input.trim() {
        "1" => Ok(StorageChoice::Sqlite),
        "2" => Ok(StorageChoice::ExistingPostgres),
        "3" => Ok(StorageChoice::ManagedDockerPostgres),
        "4" => Ok(StorageChoice::ExternalManagedPostgres),
        other => Err(InstallError::Storage(format!(
            "invalid choice '{other}', expected 1-4"
        ))),
    }
}

/// Шаг 2: по выбору строит ResolvedStorage. Для Postgres-вариантов — РЕАЛЬНО
/// проверяет соединение (`SELECT 1`).
pub async fn resolve_storage(
    choice: StorageChoice,
    data_dir: &Path,
    existing_dsn: Option<&str>,
    confirm_docker: bool,
    docker_network_only: bool,
) -> Result<ResolvedStorage, InstallError> {
    match choice {
        StorageChoice::Sqlite => {
            let db_path = data_dir.join("adapter.db");
            Ok(ResolvedStorage {
                yaml_fragment: serde_yaml::to_value(serde_json::json!({
                    "type": "sqlite",
                    "path": db_path.display().to_string(),
                }))
                .map_err(|e| InstallError::Storage(e.to_string()))?,
                env_vars: vec![],
                docker_summary: None,
            })
        }

        StorageChoice::ExistingPostgres | StorageChoice::ExternalManagedPostgres => {
            let dsn = existing_dsn.ok_or_else(|| {
                InstallError::Storage(
                    "--postgres-dsn is required for existing/external Postgres profiles".into(),
                )
            })?;
            validate_postgres_connection(dsn).await?;
            Ok(ResolvedStorage {
                yaml_fragment: serde_yaml::to_value(serde_json::json!({
                    "type": "postgres",
                    "dsn-env": DSN_ENV_VAR_NAME,
                    "schema": "agent_adapter",
                    "max-connections": 10,
                }))
                .map_err(|e| InstallError::Storage(e.to_string()))?,
                env_vars: vec![(DSN_ENV_VAR_NAME.to_string(), dsn.to_string())],
                docker_summary: None,
            })
        }

        StorageChoice::ManagedDockerPostgres => {
            let plan: ManagedPostgresPlan =
                managed_docker::plan(confirm_docker, docker_network_only, DSN_ENV_VAR_NAME)
                    .map_err(InstallError::Storage)?;

            managed_docker::ensure_running(&plan).await?;
            validate_postgres_connection(&plan.dsn).await?;

            Ok(ResolvedStorage {
                yaml_fragment: serde_yaml::to_value(serde_json::json!({
                    "type": "postgres",
                    "dsn-env": DSN_ENV_VAR_NAME,
                    "schema": "agent_adapter",
                    "max-connections": 10,
                }))
                .map_err(|e| InstallError::Storage(e.to_string()))?,
                env_vars: vec![(DSN_ENV_VAR_NAME.to_string(), plan.dsn.clone())],
                docker_summary: Some(DockerSummary {
                    host_port: plan.host_port,
                    volume_name: "agent-connector-pg-data".to_string(),
                    container_name: "agent-connector-pg".to_string(),
                }),
            })
        }
    }
}

/// Реальная проверка `SELECT 1` через tokio-postgres.
async fn validate_postgres_connection(dsn: &str) -> Result<(), InstallError> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| InstallError::DsnValidation(format!("connection failed: {e}")))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "postgres validation connection closed with error");
        }
    });

    client
        .simple_query("SELECT 1")
        .await
        .map_err(|e| InstallError::DsnValidation(format!("SELECT 1 failed: {e}")))?;

    Ok(())
}

/// Шаг 3: пишет adapter.yaml (без секретов) и .env (с секретами).
pub fn write_config_files(
    prefix: &Path,
    resolved_storage: &ResolvedStorage,
    template_agents_yaml: &serde_yaml::Value,
    platform: &dyn PlatformServiceManager,
) -> Result<(PathBuf, PathBuf), InstallError> {
    let config_path = prefix.join("adapter.yaml");
    let env_path = prefix.join(".env");

    let mut root = serde_yaml::Mapping::new();
    root.insert("mode".into(), "local".into());
    root.insert("storage".into(), resolved_storage.yaml_fragment.clone());
    root.insert("agents".into(), template_agents_yaml.clone());

    let yaml_text = serde_yaml::to_string(&serde_yaml::Value::Mapping(root))
        .map_err(|e| InstallError::Storage(format!("failed to serialize adapter.yaml: {e}")))?;
    std::fs::write(&config_path, yaml_text)?;

    let mut env_text = String::from(
        "# agent-connector runtime environment — generated by adapterctl install.\n\
         # Do NOT commit this file. Regenerate with `adapterctl install --force` if lost\n\
         # (Postgres password will be rotated on regeneration for managed-docker profile).\n\n",
    );
    for (key, value) in &resolved_storage.env_vars {
        env_text.push_str(&format!("{key}={value}\n"));
    }
    std::fs::write(&env_path, env_text)?;

    platform.restrict_file_permissions(&env_path, "adapterd")?;

    Ok((config_path, env_path))
}

/// Полный install-flow, вызываемый из main.rs Command::Install.
#[allow(clippy::too_many_arguments)]
pub async fn run_install(
    prefix: PathBuf,
    service_user: String,
    storage_flag: Option<StorageChoice>,
    postgres_dsn: Option<String>,
    confirm_docker: bool,
    docker_network_only: bool,
    config_template_path: Option<PathBuf>,
    repo_root: &Path,
    skip_build: bool,
    start_now: bool,
    platform: Box<dyn PlatformServiceManager>,
) -> Result<String, InstallError> {
    let choice = resolve_storage_choice(storage_flag)?;

    if choice == StorageChoice::ManagedDockerPostgres {
        if !confirm_docker {
            return Err(InstallError::Storage(
                "ManagedDockerPostgres selected without --confirm-docker".into(),
            ));
        }
        // Preflight ДО cargo build — пользователь узнаёт о недоступности
        // Docker за секунды, не после минут компиляции adapterd.
        managed_docker::preflight_check().await.map_err(|e| {
            InstallError::Storage(format!(
                "Docker preflight check failed before proceeding with managed-docker-postgres: {e}"
            ))
        })?;
    }

    // ШАГ 3: загрузка реального шаблона агентов ДО build/install — fail-fast.
    let agents_template = config_template::load(config_template_path.as_deref(), repo_root)
        .map_err(|e| InstallError::Storage(format!("agents template invalid: {e}")))?;

    let data_dir = prefix.join("data");
    std::fs::create_dir_all(&data_dir)?;

    if !skip_build {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release", "-p", "adapterd"])
            .current_dir(repo_root)
            .status()
            .map_err(|e| InstallError::Build(e.to_string()))?;
        if !status.success() {
            return Err(InstallError::Build(format!(
                "cargo build exited with {status}"
            )));
        }
    }

    platform.ensure_service_user(&service_user)?;

    let resolved = resolve_storage(
        choice,
        &data_dir,
        postgres_dsn.as_deref(),
        confirm_docker,
        docker_network_only,
    )
    .await?;

    let agents_yaml = serde_yaml::to_value(&agents_template.agents)
        .map_err(|e| InstallError::Storage(format!("failed to serialize agents template: {e}")))?;

    let binary_src = repo_root.join("target/release/adapterd");
    let binary_dest = prefix.join("target/release/adapterd");
    std::fs::create_dir_all(binary_dest.parent().unwrap())?;
    std::fs::copy(&binary_src, &binary_dest)?;
    platform.restrict_file_permissions(&binary_dest, &service_user)?;

    let (config_path, env_path) =
        write_config_files(&prefix, &resolved, &agents_yaml, platform.as_ref())?;
    platform.restrict_file_permissions(&config_path, &service_user)?;

    let ctx = ServiceContext {
        binary_path: &binary_dest,
        config_path: &config_path,
        env_file_path: &env_path,
        working_directory: &prefix,
        data_directory: &data_dir,
        user: &service_user,
    };
    platform.register_service(&ctx)?;

    if start_now {
        platform.start_service("adapterd")?;
    }

    Ok(render_summary(
        &prefix,
        &binary_dest,
        &config_path,
        &data_dir,
        &resolved,
        start_now,
    ))
}

fn render_summary(
    prefix: &Path,
    binary_path: &Path,
    config_path: &Path,
    data_dir: &Path,
    resolved: &ResolvedStorage,
    started: bool,
) -> String {
    let mut out = String::new();
    out.push_str("agent-connector installed successfully.\n");
    out.push_str(&format!("  prefix:    {}\n", prefix.display()));
    out.push_str(&format!("  binary:    {}\n", binary_path.display()));
    out.push_str(&format!("  config:    {}\n", config_path.display()));
    out.push_str(&format!("  data:      {}\n", data_dir.display()));

    if let Some(docker) = &resolved.docker_summary {
        if let Some(port) = docker.host_port {
            out.push_str(&format!(
                "  postgres:  127.0.0.1:{port} (container: {})\n",
                docker.container_name
            ));
        } else {
            out.push_str(&format!(
                "  postgres:  internal Docker network only (container: {})\n",
                docker.container_name
            ));
        }
        out.push_str(&format!(
            "  pg volume: {} (NOT removed on uninstall unless --purge-data)\n",
            docker.volume_name
        ));
        out.push_str(&format!(
            "  backup:    docker exec {} pg_dump -U adapter_connector agent_connector > backup.sql\n",
            docker.container_name
        ));
    }

    out.push_str(&format!(
        "  started:   {}\n",
        if started {
            "yes"
        } else {
            "no — run `adapterctl start` or the platform service command"
        }
    ));
    out.push_str(&format!(
        "  uninstall: adapterctl uninstall --prefix {}\n",
        prefix.display()
    ));
    out
}

pub mod uninstall_flow {
    use super::*;

    pub async fn run(prefix: &Path, purge_data: bool) -> Result<(), InstallError> {
        let platform = crate::platform::platform_manager()?;
        platform.unregister_service("adapterd")?;

        let config_path = prefix.join("adapter.yaml");
        let env_path = prefix.join(".env");
        std::fs::remove_file(&config_path).ok();
        std::fs::remove_file(&env_path).ok();
        std::fs::remove_dir_all(prefix.join("target")).ok();

        if purge_data {
            println!("--purge-data set: removing data directory and any managed Docker volume/container.");
            std::fs::remove_dir_all(prefix.join("data")).ok();

            match managed_docker::remove_all_resources(true).await {
                Ok(()) => {}
                Err(DockerError::NotOurResource(name)) => {
                    eprintln!(
                        "warning: found a resource named '{name}' but it lacks the \
                         installer's ownership label — left untouched, not removed"
                    );
                }
                Err(e) => {
                    eprintln!("warning: failed to remove managed Docker resources: {e}");
                }
            }
        } else {
            println!(
                "Data preserved: {} and any managed-docker Postgres volume were NOT removed. \
                 Re-run with --purge-data to delete them permanently.",
                prefix.join("data").display()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_storage_choice_respects_explicit_flag_even_with_tty() {
        let result = resolve_storage_choice(Some(StorageChoice::Sqlite));
        assert!(matches!(result, Ok(StorageChoice::Sqlite)));
    }

    #[test]
    fn no_tty_errors_without_flag() {
        if atty::is(atty::Stream::Stdin) {
            eprintln!("skipping: test requires non-TTY environment to be meaningful");
            return;
        }
        let result = resolve_storage_choice(None);
        assert!(matches!(result, Err(InstallError::NoTty)));
    }
}
