//! crates/adapterctl/src/config_template.rs — загрузка/валидация шаблона
//! `agents:` секции из --config. Использует РЕАЛЬНЫЕ типы из adapterd
//! (ре-экспорт config через adapterd::config), не собственный парсер.

use adapterd::config::{AgentConfig, AgentTransportConfig, Config};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("template file not found: {0}")]
    NotFound(PathBuf),
    #[error("failed to read template: {0}")]
    Io(#[from] std::io::Error),
    #[error("template is not valid YAML for adapter.yaml: {0}")]
    InvalidYaml(String),
    #[error("template has no agents defined — at least one agent is required")]
    NoAgents,
    #[error(
        "stdio agent '{agent_id}' references command '{command}' which is not on PATH \
             and is not an absolute existing file — installer cannot verify it will run"
    )]
    UnverifiableStdioCommand { agent_id: String, command: String },
}

/// Результат загрузки шаблона — только список агентов. Секция
/// storage/mode/runtime игнорируется: эти решения принимает install-flow.
pub struct AgentsTemplate {
    pub agents: Vec<AgentConfig>,
}

/// Источник шаблона: явный --config путь, либо встроенный default
/// (config/adapter.example.yaml в repo).
pub fn load(
    explicit_path: Option<&Path>,
    repo_root: &Path,
) -> Result<AgentsTemplate, TemplateError> {
    let path = explicit_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| repo_root.join("config/adapter.example.yaml"));

    if !path.exists() {
        return Err(TemplateError::NotFound(path));
    }

    let text = std::fs::read_to_string(&path)?;

    let full_config: Config =
        serde_yaml::from_str(&text).map_err(|e| TemplateError::InvalidYaml(e.to_string()))?;

    if full_config.agents.is_empty() {
        return Err(TemplateError::NoAgents);
    }

    validate_agents_runnable(&full_config.agents)?;

    Ok(AgentsTemplate {
        agents: full_config.agents,
    })
}

/// Best-effort проверка ДО установки, что stdio-агенты реально смогут
/// запуститься — наличие command на PATH или как абсолютный существующий файл.
fn validate_agents_runnable(agents: &[AgentConfig]) -> Result<(), TemplateError> {
    for agent in agents {
        if let AgentTransportConfig::Stdio { command, .. } = &agent.transport {
            if !command_is_resolvable(command) {
                return Err(TemplateError::UnverifiableStdioCommand {
                    agent_id: agent.id.clone(),
                    command: command.display().to_string(),
                });
            }
        }
    }
    Ok(())
}

fn command_is_resolvable(command: &Path) -> bool {
    if command.is_absolute() {
        return command.exists();
    }
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths).any(|dir| {
                let candidate = dir.join(command);
                candidate.is_file()
            })
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_yaml(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[test]
    fn rejects_template_with_no_agents() {
        let file = write_temp_yaml("mode: local\nagents: []\n");
        let result = load(Some(file.path()), Path::new("."));
        assert!(matches!(result, Err(TemplateError::NoAgents)));
    }

    #[test]
    fn rejects_stdio_agent_with_unresolvable_command() {
        let yaml = r#"
mode: local
agents:
  - id: broken-agent
    driver: stdio
    command: /definitely/does/not/exist/anywhere
"#;
        let file = write_temp_yaml(yaml);
        let result = load(Some(file.path()), Path::new("."));
        assert!(matches!(
            result,
            Err(TemplateError::UnverifiableStdioCommand { .. })
        ));
    }

    #[test]
    fn accepts_stdio_agent_with_command_on_path() {
        if cfg!(windows) {
            eprintln!("skipping: 'echo' as external command is not guaranteed resolvable via PATH lookup on Windows");
            return;
        }
        let yaml = r#"
mode: local
agents:
  - id: echo-agent
    skills: [echo]
    driver: stdio
    command: echo
"#;
        let file = write_temp_yaml(yaml);
        let result = load(Some(file.path()), Path::new("."));
        assert!(
            result.is_ok(),
            "echo should resolve via PATH: {:?}",
            result.err()
        );
    }

    #[test]
    fn missing_explicit_path_errors_clearly() {
        let result = load(
            Some(Path::new("/nonexistent/path/adapter.yaml")),
            Path::new("."),
        );
        assert!(matches!(result, Err(TemplateError::NotFound(_))));
    }
}
