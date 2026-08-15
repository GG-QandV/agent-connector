//! `AdapterCardProducer` — реализация `a2a-server::AgentCardProducer`,
//! строящая Agent Card из агент-регистрации и конфигурации.

use a2a::*;
use a2a_server::AgentCardProducer;
use adapter_core::AgentRegistry;
use std::sync::Arc;

/// Настройки для публичной карточки шлюза.
#[derive(Clone, Debug)]
pub struct AdapterCardConfig {
    pub name: String,
    pub description: String,
    pub version: String,
    pub endpoint_url: String,
}

pub struct AdapterCardProducer {
    registry: Arc<AgentRegistry>,
    config: AdapterCardConfig,
}

impl AdapterCardProducer {
    pub fn new(registry: Arc<AgentRegistry>, config: AdapterCardConfig) -> Self {
        Self { registry, config }
    }

    fn agent_skills(&self) -> Vec<AgentSkill> {
        self.registry
            .agents()
            .into_iter()
            .flat_map(|agent| {
                let agent_id = agent.id.0.clone();
                let skills = agent.skills.clone();
                skills.into_iter().map(move |skill| AgentSkill {
                    id: skill.clone(),
                    name: skill.clone(),
                    description: format!("{agent_id} provides skill `{skill}`"),
                    tags: vec!["adapter".into()],
                    examples: None,
                    input_modes: None,
                    output_modes: None,
                    security_requirements: None,
                })
            })
            .collect()
    }
}

impl AgentCardProducer for AdapterCardProducer {
    fn card(&self) -> AgentCard {
        AgentCard {
            name: self.config.name.clone(),
            description: self.config.description.clone(),
            version: self.config.version.clone(),
            supported_interfaces: vec![AgentInterface::new(
                self.config.endpoint_url.clone(),
                "https",
            )],
            capabilities: AgentCapabilities {
                streaming: Some(true),
                push_notifications: None,
                extensions: None,
                extended_agent_card: None,
            },
            default_input_modes: vec!["text".into()],
            default_output_modes: vec!["text".into()],
            skills: self.agent_skills(),
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: None,
            security_requirements: None,
            signatures: None,
        }
    }
}
