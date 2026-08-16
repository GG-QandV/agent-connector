//! crates/adapterctl/src/main.rs — installer/CLI для agent-connector.
//!
//! Подкоманды (по описаниям из Comment-файлов в docs/design):
//!   install            — полный install-flow (storage choice -> build -> service)
//!   uninstall          — удаление службы и (опционально) данных
//!   start              — запуск службы adapterd
//!   stop               — остановка службы adapterd
//!   backup-postgres    — pg_dump managed-docker Postgres в файл
//!   upgrade-postgres   — смена образа Postgres (обязательный backup перед)
//!
//! Атрибуты-future: macOS-слой и graceful Ctrl+C реализованы как модули;
//! остальное — см. LOCAL_AGENT_ADAPTERCTL_INSTALL_FILES.md.

mod cancel;
mod config_template;
mod install_flow;
mod managed_docker;
mod platform;
mod postgres_lifecycle;

use platform::StorageChoice;
use std::env;
use std::path::PathBuf;

const DEFAULT_PREFIX: &str = "/opt/agent-connector";
const DEFAULT_USER: &str = "adapterd";

#[derive(Debug)]
enum Command {
    Install(InstallArgs),
    Uninstall {
        prefix: PathBuf,
        purge_data: bool,
    },
    Start,
    Stop,
    BackupPostgres {
        output: PathBuf,
    },
    UpgradePostgres {
        target_image_tag: String,
        prefix: PathBuf,
    },
    Help,
}

#[derive(Debug)]
struct InstallArgs {
    prefix: PathBuf,
    user: String,
    storage: Option<StorageChoice>,
    postgres_dsn: Option<String>,
    confirm_docker: bool,
    docker_network_only: bool,
    config: Option<PathBuf>,
    skip_build: bool,
    start_now: bool,
}

fn parse_storage(s: &str) -> Result<StorageChoice, String> {
    match s {
        "sqlite" => Ok(StorageChoice::Sqlite),
        "existing-postgres" | "postgres" => Ok(StorageChoice::ExistingPostgres),
        "managed-docker-postgres" | "docker" => Ok(StorageChoice::ManagedDockerPostgres),
        "external-managed" => Ok(StorageChoice::ExternalManagedPostgres),
        other => Err(format!(
            "unknown storage '{other}' — expected one of: sqlite, existing-postgres, \
             managed-docker-postgres, external-managed"
        )),
    }
}

fn parse_args() -> Result<Command, String> {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        return Ok(Command::Help);
    }
    let sub = args.remove(0);
    match sub.as_str() {
        "install" => {
            let mut a = InstallArgs {
                prefix: PathBuf::from(DEFAULT_PREFIX),
                user: DEFAULT_USER.to_string(),
                storage: None,
                postgres_dsn: None,
                confirm_docker: false,
                docker_network_only: false,
                config: None,
                skip_build: false,
                start_now: false,
            };
            let mut i = 0;
            while i < args.len() {
                match args[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        a.prefix = PathBuf::from(args.get(i).ok_or("--prefix requires a value")?);
                    }
                    "--user" => {
                        i += 1;
                        a.user = args.get(i).ok_or("--user requires a value")?.clone();
                    }
                    "--storage" => {
                        i += 1;
                        a.storage = Some(parse_storage(
                            args.get(i).ok_or("--storage requires a value")?,
                        )?);
                    }
                    "--postgres-dsn" => {
                        i += 1;
                        a.postgres_dsn = Some(
                            args.get(i)
                                .ok_or("--postgres-dsn requires a value")?
                                .clone(),
                        );
                    }
                    "--confirm-docker" => a.confirm_docker = true,
                    "--docker-network-only" => a.docker_network_only = true,
                    "--config" => {
                        i += 1;
                        a.config = Some(PathBuf::from(
                            args.get(i).ok_or("--config requires a value")?,
                        ));
                    }
                    "--skip-build" => a.skip_build = true,
                    "--start-now" => a.start_now = true,
                    other => return Err(format!("unknown install flag: {other}")),
                }
                i += 1;
            }
            Ok(Command::Install(a))
        }
        "uninstall" => {
            let mut prefix = PathBuf::from(DEFAULT_PREFIX);
            let mut purge_data = false;
            let mut i = 0;
            while i < args.len() {
                match args[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        prefix = PathBuf::from(args.get(i).ok_or("--prefix requires a value")?);
                    }
                    "--purge-data" => purge_data = true,
                    other => return Err(format!("unknown uninstall flag: {other}")),
                }
                i += 1;
            }
            Ok(Command::Uninstall { prefix, purge_data })
        }
        "start" => Ok(Command::Start),
        "stop" => Ok(Command::Stop),
        "backup-postgres" => {
            let output = args
                .first()
                .cloned()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("agent-connector-backup.sql"));
            Ok(Command::BackupPostgres { output })
        }
        "upgrade-postgres" => {
            let tag = args
                .first()
                .ok_or("usage: adapterctl upgrade-postgres <image-tag> [--prefix PATH]")?;
            let mut prefix = PathBuf::from(DEFAULT_PREFIX);
            let mut i = 1;
            while i < args.len() {
                match args[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        prefix = PathBuf::from(args.get(i).ok_or("--prefix requires a value")?);
                    }
                    other => return Err(format!("unknown upgrade-postgres flag: {other}")),
                }
                i += 1;
            }
            Ok(Command::UpgradePostgres {
                target_image_tag: tag.clone(),
                prefix,
            })
        }
        "help" | "--help" | "-h" => Ok(Command::Help),
        other => Err(format!("unknown command '{other}' — use 'adapterctl help'")),
    }
}

fn print_help() {
    println!("adapterctl — agent-connector installer / service manager");
    println!();
    println!("USAGE:");
    println!("  adapterctl install [--prefix PATH] [--user NAME] [--storage CHOICE]");
    println!("                   [--postgres-dsn DSN] [--confirm-docker] [--docker-network-only]");
    println!("                   [--config PATH] [--skip-build] [--start-now]");
    println!("  adapterctl uninstall [--prefix PATH] [--purge-data]");
    println!("  adapterctl start | stop");
    println!("  adapterctl backup-postgres [OUTPUT.sql]");
    println!("  adapterctl upgrade-postgres <image-tag> [--prefix PATH]");
    println!();
    println!(
        "STORAGE CHOICES: sqlite | existing-postgres | managed-docker-postgres | external-managed"
    );
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let command = match parse_args() {
        Ok(cmd) => cmd,
        Err(e) => {
            eprintln!("error: {e}");
            print_help();
            return Err(e);
        }
    };

    match command {
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::Install(a) => {
            let repo_root =
                env::current_dir().map_err(|e| format!("cannot determine repo root: {e}"))?;
            let platform = platform::platform_manager().map_err(|e| e.to_string())?;
            match install_flow::run_install(
                a.prefix,
                a.user,
                a.storage,
                a.postgres_dsn,
                a.confirm_docker,
                a.docker_network_only,
                a.config,
                &repo_root,
                a.skip_build,
                a.start_now,
                platform,
            )
            .await
            {
                Ok(summary) => {
                    println!("{summary}");
                    Ok(())
                }
                Err(e) => Err(format!("install failed: {e}")),
            }
        }
        Command::Uninstall { prefix, purge_data } => {
            install_flow::uninstall_flow::run(&prefix, purge_data)
                .await
                .map_err(|e| format!("uninstall failed: {e}"))
        }
        Command::Start => {
            let platform = platform::platform_manager().map_err(|e| e.to_string())?;
            platform
                .start_service("adapterd")
                .map_err(|e| e.to_string())
        }
        Command::Stop => {
            let platform = platform::platform_manager().map_err(|e| e.to_string())?;
            platform
                .unregister_service("adapterd")
                .map_err(|e| e.to_string())
        }
        Command::BackupPostgres { output } => {
            postgres_lifecycle::backup(&output)
                .await
                .map_err(|e| format!("backup failed: {e}"))?;
            println!("Backup written to {}", output.display());
            Ok(())
        }
        Command::UpgradePostgres {
            target_image_tag,
            prefix,
        } => postgres_lifecycle::upgrade(&target_image_tag, &prefix)
            .await
            .map_err(|e| format!("upgrade failed: {e}")),
    }
}
