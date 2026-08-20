//! ANP application profile: `agent-connector.anp-task.v1`.
//!
//! Embedded application profile that runs over the ANP substrate. This
//! crate owns the wire DTOs, validation and the mapping to/from the
//! canonical `adapter-core` command/event model.
//!
//! Profile ID and payload schema are a **compatibility contract** — changes
//! require an ADR update (see `docs/adr/ADR-ANP-001.md`).

use std::fmt;

/// Canonical profile identifier.
pub const PROFILE_ID: &str = "agent-connector.anp-task.v1";
/// Current profile version.
pub const PROFILE_VERSION: u32 = 1;

/// Maximum accepted payload size in bytes (soft cap, enforced by validation).
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

pub mod dto;
pub mod mapper;
pub mod validation;

#[cfg(test)]
mod schema_conformance;

pub use dto::*;

/// Profile-level errors.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("unsupported profile id `{0}` (expected {PROFILE_ID})")]
    UnsupportedProfileId(String),
    #[error("profile version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u32, actual: u32 },
    #[error("payload too large: {actual} bytes > {max} bytes")]
    PayloadTooLarge { actual: usize, max: usize },
    #[error("invalid field `{field}`: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("duplicate message id `{0}`")]
    DuplicateMessageId(String),
    #[error("invalid sequence: {reason}")]
    InvalidSequence { reason: String },
    #[error("terminal event rules violated: {0}")]
    TerminalRuleViolation(String),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type ProfileResult<T> = Result<T, ProfileError>;

impl fmt::Display for dto::TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
