//! ПРАВКА к install_flow.rs (написан ранее в диалоге) — закрывает шаг 3:
//! заменяет захардкоженный default_agents на config_template::load().
//! Ниже — обновлённая сигнатура и тело run_install(), остальной файл
//! (resolve_storage_choice, resolve_storage, write_config_files,
//! validate_postgres_connection, render_summary) не меняется — вставить
//! этот блок вместо старой версии run_install() в install_flow.rs.

use crate::config_template;
use crate::managed_docker::{self, DockerError, ManagedPostgresPlan};
use crate::{PlatformError, PlatformServiceManager, ServiceContext, StorageChoice};
use std::path::{Path, PathBuf};

// ... (InstallError, ResolvedStorage, DockerSummary, resolve_storage_choice,
//      resolve_storage, validate_postgres_connection, write_config_files,
//      render_summary — без изменений, см. предыдущую версию файла)

/// Обновлённая сигнатура: добавлен `config: Option<PathBuf>` и `repo_root: &Path`
/// — нужны config_template::load() для поиска шаблона (явный путь или
/// репозиторный default).
pub async fn run_install(
    prefix: PathBuf,
    service_user: String,
    storage_flag: Option<StorageChoice>,
    postgres_dsn: Option<String>,
    confirm_docker: bool,
    docker_network_only: bool,
    config_template_path: Option<PathBuf>, // НОВЫЙ параметр (был отсутствие вообще)
    repo_root: &Path,                       // НОВЫЙ параметр
    skip_build: bool,
    start_now: bool,
    platform: Box<dyn PlatformServiceManager>,
) -> Result<String, InstallError> {
    let choice = resolve_storage_choice(storage_flag)?;

    if choice == StorageChoice::ManagedDockerPostgres && !confirm_docker {
        return Err(InstallError::Storage(
            "ManagedDockerPostgres selected without --confirm-docker".into()
        ));
    }

    // ШАГ 3: загрузка реального шаблона агентов ДО build/install — тот же
    // принцип fail-fast, что уже применён к проверке --confirm-docker: не
    // тратим время на cargo build, если сам шаблон агентов невалиден
    // (например, stdio command не резолвится на PATH).
    let agents_template = config_template::load(config_template_path.as_deref(), repo_root)
        .map_err(|e| InstallError::Storage(format!("agents template invalid: {e}")))?;

    let data_dir = prefix.join("data");
    std::fs::create_dir_all(&data_dir)?;

    if !skip_build {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release", "-p", "adapterd"])
            .status()
            .map_err(|e| InstallError::Build(e.to_string()))?;
        if !status.success() {
            return Err(InstallError::Build(format!("cargo build exited with {status}")));
        }
    }

    platform.ensure_service_user(&service_user)?;

    let resolved = resolve_storage(
        choice,
        &data_dir,
        postgres_dsn.as_deref(),
        confirm_docker,
        docker_network_only,
    ).await?;

    // БЫЛО (удалено): захардкоженный default_agents = serde_yaml::to_value(json!([{
    //     "id": "echo", "skills": ["echo"], "driver": "stdio", "command": "echo",
    // }]))
    //
    // СТАЛО: agents_template.agents уже провалидированные AgentConfig из
    // config_template::load() — та же структура, что примет adapterd,
    // сериализуем обратно в YAML для встраивания в итоговый adapter.yaml.
    let agents_yaml = serde_yaml::to_value(&agents_template.agents)
        .map_err(|e| InstallError::Storage(format!("failed to serialize agents template: {e}")))?;

    let binary_src = repo_root.join("target/release/adapterd");
    let binary_dest = prefix.join("target/release/adapterd");
    std::fs::create_dir_all(binary_dest.parent().unwrap())?;
    std::fs::copy(&binary_src, &binary_dest)?;
    platform.restrict_file_permissions(&binary_dest, &service_user)?;

    let (config_path, env_path) = write_config_files(&prefix, &resolved, &agents_yaml, platform.as_ref())?;
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

    Ok(render_summary(&prefix, &binary_dest, &config_path, &data_dir, &resolved, start_now))
}

// ============================================================
// uninstall_flow — также требует правки: подключить
// managed_docker::remove_all_resources() при --purge-data (было
// закомментировано как TODO). Показано здесь как часть той же группы
// правок install_flow.rs, раз мы всё равно редактируем этот файл.
// ============================================================
pub mod uninstall_flow {
    use super::*;

    pub async fn run(prefix: &Path, purge_data: bool) -> Result<(), InstallError> {
        let platform = crate::platform_manager()?;
        platform.unregister_service("adapterd")?;

        let config_path = prefix.join("adapter.yaml");
        let env_path = prefix.join(".env");
        std::fs::remove_file(&config_path).ok();
        std::fs::remove_file(&env_path).ok();
        std::fs::remove_dir_all(prefix.join("target")).ok();

        if purge_data {
            println!("--purge-data set: removing data directory and any managed Docker volume/container.");
            std::fs::remove_dir_all(prefix.join("data")).ok();

            // РАНЬШЕ: только комментарий-заглушка "+ bollard: remove container...".
            // ТЕПЕРЬ: реальный вызов. verify_ownership внутри remove_all_resources
            // защищает от удаления контейнера/volume, не принадлежащего этой
            // инсталляции — если managed-docker профиль не использовался вообще,
            // remove_all_resources просто не найдёт контейнер (Ok(()), не ошибка)
            // и молча пропустит этот шаг.
            match managed_docker::remove_all_resources(true).await {
                Ok(()) => {}
                Err(DockerError::NotOurResource(name)) => {
                    eprintln!(
                        "warning: found a resource named '{name}' but it lacks the \
                         installer's ownership label — left untouched, not removed"
                    );
                }
                Err(e) => {
                    // Не фейлим весь uninstall из-за Docker-ошибки (например,
                    // Docker daemon уже не запущен на момент uninstall) —
                    // файловая часть уже удалена успешно, это best-effort шаг.
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
