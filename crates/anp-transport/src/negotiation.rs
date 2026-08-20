//! Deterministic ANP profile negotiation.

use crate::capabilities::{AnpCapabilities, AnpCapability};
use crate::error::{AnpError, AnpResult};

/// A profile the local side is willing to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileOffer {
    /// Ordered list of profile IDs by local preference (best first).
    pub profiles: Vec<String>,
}

/// Result of running `anp.negotiate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationStatus {
    /// The peer accepted a profile.
    Accepted,
    /// The peer has no common profile — messaging-only fallback applies.
    NoCommonProfile,
}

/// A successfully negotiated profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedProfile {
    /// Selected profile ID (e.g. `agent-connector.anp-task.v1`).
    pub profile_id: String,
    /// Profile version resolved during negotiation (e.g. `1`).
    pub version: u32,
    /// Whether the peer advertises a durable cursor/history contract.
    /// `false` means resume/replay must not be promised.
    pub supports_resume: bool,
}

/// Outcome of a negotiation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedProfile {
    pub status: NegotiationStatus,
    /// Set when [`NegotiationStatus::Accepted`].
    pub selected: Option<SelectedProfile>,
}

/// Deterministic profile selection policy.
///
/// Prefers the caller's first (best) offer that the peer also advertises.
/// No preference ordering exists beyond the local list; the first match wins.
pub fn profile_selection(
    offer: &ProfileOffer,
    peer_caps: &AnpCapabilities,
) -> AnpResult<Option<SelectedProfile>> {
    if offer.profiles.is_empty() {
        return Err(AnpError::InvalidPayload("empty profile offer".to_string()));
    }
    for profile in &offer.profiles {
        if peer_caps.contains(profile) {
            return Ok(Some(SelectedProfile {
                profile_id: profile.clone(),
                version: 1,
                supports_resume: peer_caps_has_resume(peer_caps, profile),
            }));
        }
    }
    Ok(None)
}

fn peer_caps_has_resume(peer_caps: &AnpCapabilities, profile: &str) -> bool {
    // The resume contract is advertised as a sibling cursor/history marker.
    // Naming: `<profile>#resume` or `<profile>.resume`.
    for cap in &peer_caps.raw {
        let AnpCapability(s) = cap;
        if s.as_str() == format!("{profile}#resume") || s.as_str() == format!("{profile}.resume") {
            return true;
        }
    }
    false
}

/// Runs the full negotiation against a fake/accepted peer response.
pub fn negotiate_deterministic(
    offer: &ProfileOffer,
    peer_caps: &AnpCapabilities,
    peer_accepts: bool,
) -> AnpResult<NegotiatedProfile> {
    if !peer_accepts {
        return Ok(NegotiatedProfile {
            status: NegotiationStatus::NoCommonProfile,
            selected: None,
        });
    }
    match profile_selection(offer, peer_caps)? {
        Some(selected) => Ok(NegotiatedProfile {
            status: NegotiationStatus::Accepted,
            selected: Some(selected),
        }),
        None => Ok(NegotiatedProfile {
            status: NegotiationStatus::NoCommonProfile,
            selected: None,
        }),
    }
}
