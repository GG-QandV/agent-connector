//! ANP transport error type.

use std::fmt;

/// ANP transport errors.
#[derive(Debug, Clone)]
pub enum AnpError {
    /// Identity verification failed. No fallback to insecure mode.
    IdentityVerificationFailed(String),
    /// The peer did not expose any capability required by the offer.
    NoCommonProfile {
        /// Capabilities the peer advertises.
        peer_capabilities: Vec<String>,
        /// Profiles the local side offered.
        offered: Vec<String>,
    },
    /// Transport-level failure (connection, timeout, malformed frame).
    Transport(String),
    /// The peer rejected the message (non-2xx / explicit rejection).
    Rejected(String),
    /// A required capability is unsupported by the peer.
    UnsupportedCapability(String),
    /// Invalid/missing payload or schema violation.
    InvalidPayload(String),
    /// The transport has not been connected or the peer identity is stale.
    NotConnected,
    /// Operation timed out.
    Timeout,
}

impl AnpError {
    pub fn is_identity_failure(&self) -> bool {
        matches!(self, AnpError::IdentityVerificationFailed(_))
    }

    pub fn is_no_common_profile(&self) -> bool {
        matches!(self, AnpError::NoCommonProfile { .. })
    }
}

impl fmt::Display for AnpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AnpError::IdentityVerificationFailed(detail) => {
                write!(f, "ANP identity verification failed: {detail}")
            }
            AnpError::NoCommonProfile {
                peer_capabilities,
                offered,
            } => write!(
                f,
                "no common ANP profile: peer offers {peer_capabilities:?}, local offered {offered:?}"
            ),
            AnpError::Transport(detail) => write!(f, "ANP transport failure: {detail}"),
            AnpError::Rejected(detail) => write!(f, "ANP peer rejected message: {detail}"),
            AnpError::UnsupportedCapability(cap) => {
                write!(f, "ANP capability not supported by peer: {cap}")
            }
            AnpError::InvalidPayload(detail) => write!(f, "invalid ANP payload: {detail}"),
            AnpError::NotConnected => write!(f, "ANP transport not connected"),
            AnpError::Timeout => write!(f, "ANP operation timed out"),
        }
    }
}

impl std::error::Error for AnpError {}

pub type AnpResult<T> = Result<T, AnpError>;
