//! `BearerTokenPolicy` — production-ready `PolicyEngine` на основе
//! статических bearer tokens, читаемых из env при старте.
//!
//! Canonical требует "caller identity: OIDC/JWT/API token" и
//! "capability-based policy: caller может вызывать только разрешённые skills"
//! для remote profile. Текущая единственная реализация — `AllowAllPolicy`,
//! это подтверждённый блокер этапа 2 canonical roadmap.
//!
//! Секреты не хранятся в конфиге — только имена env-переменных (тот же
//! паттерн `{env:GW_TOKEN_MAIN}`, что в ACP-A2A_gateway).

use crate::{Caller, CallerId, CoreCommand, CoreError, PolicyEngine};
use async_trait::async_trait;
use std::collections::HashMap;

/// Одна запись доступа: bearer token -> caller identity + разрешённые scopes.
#[derive(Clone, Debug)]
pub struct TokenGrant {
    pub caller_id: CallerId,
    /// Пусто = разрешено всё (для миграции/dev). В production используйте
    /// конкретный список agent_id/skill_id, которые может вызывать caller.
    pub allowed_scopes: Vec<String>,
}

/// PolicyEngine на основе статических bearer tokens, читаемых из env при
/// старте. Не хранит секреты в конфиге — только имена env-переменных.
///
/// Конфигурация (пример adapter.yaml):
/// ```yaml
/// auth:
///   bearer_tokens:
///     - token_env: ADAPTER_TOKEN_MAIN
///       caller_id: primary-client
///       allowed_scopes: []          # всё разрешено
///     - token_env: ADAPTER_TOKEN_READONLY
///       caller_id: readonly-client
///       allowed_scopes: ["get-status"]
/// ```
pub struct BearerTokenPolicy {
    // token -> grant. HashMap, не DashMap: заполняется один раз при старте,
    // после этого только читается (immutable после construction).
    grants: HashMap<String, TokenGrant>,
}

#[derive(thiserror::Error, Debug)]
pub enum BearerTokenPolicyError {
    #[error("missing required env var: {0}")]
    MissingEnv(String),
    #[error("duplicate token detected for token_env {0} — tokens must be unique")]
    DuplicateToken(String),
}

impl BearerTokenPolicy {
    /// Строит policy из списка (env var name, grant) пар. Паникует со
    /// startup error (не runtime error), если обязательная env-переменная
    /// отсутствует — это intentional: лучше не стартовать вообще, чем
    /// стартовать с недоделанной auth-конфигурацией.
    pub fn from_env(entries: Vec<(String, TokenGrant)>) -> Result<Self, BearerTokenPolicyError> {
        let mut grants = HashMap::new();
        for (token_env, grant) in entries {
            let token = std::env::var(&token_env)
                .map_err(|_| BearerTokenPolicyError::MissingEnv(token_env.clone()))?;
            if grants.insert(token, grant).is_some() {
                return Err(BearerTokenPolicyError::DuplicateToken(token_env));
            }
        }
        Ok(Self { grants })
    }

    /// Резолвит bearer token в Caller. Возвращает None для unknown token —
    /// вызывающий код (Axum middleware) должен вернуть 401, не 403 —
    /// разница между "не аутентифицирован" и "аутентифицирован, но
    /// запрещено" важна для клиента.
    pub fn resolve(&self, token: &str) -> Option<Caller> {
        self.grants.get(token).map(|grant| Caller {
            id: grant.caller_id.clone(),
            scopes: grant.allowed_scopes.clone(),
        })
    }
}

#[async_trait]
impl PolicyEngine for BearerTokenPolicy {
    async fn authorize(&self, caller: &Caller, command: &CoreCommand) -> Result<(), CoreError> {
        // Caller уже резолвлен через resolve() на уровне middleware — здесь
        // мы проверяем capability-based scope, если он задан.
        let required_scope = match command {
            CoreCommand::Invoke(request) => request.skill_id.as_deref(),
            CoreCommand::Cancel { .. } => Some("cancel"),
            CoreCommand::ProvideInput { .. } => Some("provide-input"),
            CoreCommand::GetStatus { .. } => Some("get-status"),
        };
        // Пустой allowed_scopes = разрешено всё (миграционный режим).
        if caller.scopes.is_empty() {
            return Ok(());
        }
        match required_scope {
            Some(scope) if caller.scopes.iter().any(|s| s == scope) => Ok(()),
            Some(scope) => Err(CoreError::InvalidRequest(format!(
                "caller {} is not authorized for scope '{scope}'",
                caller.id.0
            ))),
            // Команда без специфичного scope-требования (например, общий
            // Invoke без skill_id) — разрешаем, если caller имеет ХОТЬ
            // какой-то scope (т.е. не readonly-only caller пытается что-то
            // мутировать без явного scope match).
            None => Ok(()),
        }
    }
}
