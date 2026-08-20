//! ANP client driver state machine.
//!
//! States per `docs/design/ф-0 (п-3)/anp-p0-execution-handoff.md` §5:
//!
//! ```text
//! Disconnected → Connecting → IdentityVerified → Negotiating → TaskProfileReady
//!                                               ↘ MessagingOnly
//! (any) → Failed
//! ```
//!
//! Rules:
//! - `MessagingOnly` is NOT a full `AgentDriver`; task commands return
//!   `UnsupportedCapability`.
//! - `TaskProfileReady` is entered only after exact profile negotiation of
//!   `agent-connector.anp-task.v1`.
//! - Remote ANP `task_id` is correlation metadata; the local core `TaskId`
//!   stays canonical.
//! - Resume/replay is only promised when the negotiated profile advertises a
//!   cursor/history contract (`supports_resume`).

use std::fmt;

/// ANP client connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnpClientState {
    Disconnected,
    Connecting,
    IdentityVerified,
    Negotiating,
    MessagingOnly,
    TaskProfileReady,
    Failed,
}

impl AnpClientState {
    /// True when task commands (invoke/cancel/provide_input) are allowed.
    pub fn allows_task_commands(self) -> bool {
        matches!(self, AnpClientState::TaskProfileReady)
    }

    /// True when the state is a terminal failure.
    pub fn is_failed(self) -> bool {
        matches!(self, AnpClientState::Failed)
    }
}

impl fmt::Display for AnpClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AnpClientState::Disconnected => "Disconnected",
            AnpClientState::Connecting => "Connecting",
            AnpClientState::IdentityVerified => "IdentityVerified",
            AnpClientState::Negotiating => "Negotiating",
            AnpClientState::MessagingOnly => "MessagingOnly",
            AnpClientState::TaskProfileReady => "TaskProfileReady",
            AnpClientState::Failed => "Failed",
        };
        f.write_str(s)
    }
}

/// Valid state transitions.
///
/// Returns `Err` (leaving the state unchanged) for illegal transitions.
pub fn transition(from: AnpClientState, to: AnpClientState) -> Result<AnpClientState, String> {
    use AnpClientState::*;
    let legal = matches!(
        (from, to),
        (Disconnected, Connecting)
            | (Connecting, IdentityVerified)
            | (Connecting, Failed)
            | (IdentityVerified, Negotiating)
            | (IdentityVerified, MessagingOnly)
            | (IdentityVerified, Failed)
            | (Negotiating, TaskProfileReady)
            | (Negotiating, MessagingOnly)
            | (Negotiating, Failed)
            | (TaskProfileReady, MessagingOnly)
            | (TaskProfileReady, Failed)
            | (MessagingOnly, Failed)
            | (Failed, Disconnected)
    );
    if legal {
        Ok(to)
    } else {
        Err(format!("illegal ANP state transition {from} -> {to}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_commands_only_when_profile_ready() {
        assert!(!AnpClientState::Disconnected.allows_task_commands());
        assert!(!AnpClientState::MessagingOnly.allows_task_commands());
        assert!(AnpClientState::TaskProfileReady.allows_task_commands());
    }

    #[test]
    fn happy_path_transitions() {
        assert_eq!(
            transition(AnpClientState::Disconnected, AnpClientState::Connecting).unwrap(),
            AnpClientState::Connecting
        );
        assert_eq!(
            transition(AnpClientState::Connecting, AnpClientState::IdentityVerified).unwrap(),
            AnpClientState::IdentityVerified
        );
        assert_eq!(
            transition(
                AnpClientState::IdentityVerified,
                AnpClientState::Negotiating
            )
            .unwrap(),
            AnpClientState::Negotiating
        );
        assert_eq!(
            transition(
                AnpClientState::Negotiating,
                AnpClientState::TaskProfileReady
            )
            .unwrap(),
            AnpClientState::TaskProfileReady
        );
    }

    #[test]
    fn messaging_fallback_path() {
        assert_eq!(
            transition(AnpClientState::Negotiating, AnpClientState::MessagingOnly).unwrap(),
            AnpClientState::MessagingOnly
        );
    }

    #[test]
    fn illegal_skip_to_profile_ready_rejected() {
        assert!(transition(
            AnpClientState::Disconnected,
            AnpClientState::TaskProfileReady
        )
        .is_err());
        assert!(transition(
            AnpClientState::IdentityVerified,
            AnpClientState::TaskProfileReady
        )
        .is_err());
    }

    #[test]
    fn failed_recovery_only_via_disconnected() {
        assert_eq!(
            transition(AnpClientState::Failed, AnpClientState::Disconnected).unwrap(),
            AnpClientState::Disconnected
        );
        assert!(transition(AnpClientState::Failed, AnpClientState::TaskProfileReady).is_err());
    }
}
