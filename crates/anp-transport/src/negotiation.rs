//! Deterministic ANP profile negotiation.
//!
//! Negotiation outcome carries a bounded validity window (`valid_until`),
//! mirroring ANP negotiation semantics (`negotiationId` + `validUntil`). The
//! profile is usable only while `now < valid_until`; after expiry a
//! renegotiation is required before any task operation.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use uuid::Uuid;

use crate::capabilities::{AnpCapabilities, AnpCapability};
use crate::error::{AnpError, AnpResult};

/// A profile the local side is willing to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileOffer {
    /// Ordered list of profile IDs by local preference (best first).
    pub profiles: Vec<String>,
}

/// Capabilities granted by the negotiated profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileCapabilities {
    /// Profile version resolved during negotiation (e.g. `1`).
    pub version: u32,
    /// Whether the peer advertises a durable cursor/history contract.
    /// `false` means resume/replay must not be promised.
    pub supports_resume: bool,
}

/// A successfully negotiated profile with a validity window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedProfile {
    /// Selected profile ID (e.g. `agent-connector.anp-task.v1`).
    pub profile_id: String,
    /// Capabilities granted by the profile.
    pub capabilities: ProfileCapabilities,
    /// Opaque negotiation id returned by the peer (`negotiationId`).
    pub negotiation_id: String,
    /// Hard expiry of the negotiation result. The profile is unusable once
    /// `now >= valid_until` and must be renegotiated.
    pub valid_until: DateTime<Utc>,
}

impl NegotiatedProfile {
    /// Whether the profile is still usable at `now`.
    pub fn is_valid(&self, now: DateTime<Utc>) -> bool {
        now < self.valid_until
    }

    /// Whether the profile has expired at `now` and must be renegotiated.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        !self.is_valid(now)
    }
}

/// Deterministic profile selection policy.
///
/// Prefers the caller's first (best) offer that the peer also advertises.
/// No preference ordering exists beyond the local list; the first match wins.
/// Returns `None` when no offered profile is advertised by the peer.
pub fn profile_selection(
    offer: &ProfileOffer,
    peer_caps: &AnpCapabilities,
) -> AnpResult<Option<(String, ProfileCapabilities)>> {
    if offer.profiles.is_empty() {
        return Err(AnpError::InvalidPayload("empty profile offer".to_string()));
    }
    for profile in &offer.profiles {
        if peer_caps.contains(profile) {
            return Ok(Some((
                profile.clone(),
                ProfileCapabilities {
                    version: 1,
                    supports_resume: peer_caps_has_resume(peer_caps, profile),
                },
            )));
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

/// Builds a negotiated profile for the given selection, valid for `validity`
/// starting at `now`. Deterministic: the caller supplies the clock so tests
/// never rely on wall-clock sleeps.
pub fn build_negotiated(
    profile_id: String,
    capabilities: ProfileCapabilities,
    negotiation_id: String,
    now: DateTime<Utc>,
    validity: ChronoDuration,
) -> NegotiatedProfile {
    NegotiatedProfile {
        profile_id,
        capabilities,
        negotiation_id,
        valid_until: now + validity,
    }
}

/// Runs deterministic negotiation against a peer response.
///
/// Returns a usable [`NegotiatedProfile`] when a common profile exists and
/// the peer accepts it; otherwise returns [`AnpError::NoCommonProfile`].
pub fn negotiate_deterministic(
    offer: &ProfileOffer,
    peer_caps: &AnpCapabilities,
    peer_accepts: bool,
    now: DateTime<Utc>,
    validity: ChronoDuration,
) -> AnpResult<NegotiatedProfile> {
    if !peer_accepts {
        return Err(no_common_profile(offer, peer_caps));
    }
    match profile_selection(offer, peer_caps)? {
        Some((profile_id, caps)) => Ok(build_negotiated(
            profile_id,
            caps,
            Uuid::new_v4().to_string(),
            now,
            validity,
        )),
        None => Err(no_common_profile(offer, peer_caps)),
    }
}

fn no_common_profile(offer: &ProfileOffer, peer_caps: &AnpCapabilities) -> AnpError {
    let peer_caps: Vec<String> = peer_caps
        .raw
        .iter()
        .map(|AnpCapability(s)| s.clone())
        .collect();
    AnpError::NoCommonProfile {
        peer_capabilities: peer_caps,
        offered: offer.profiles.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn caps(with_resume: bool) -> AnpCapabilities {
        let mut raw = vec!["agent-connector.anp-task.v1".to_string()];
        if with_resume {
            raw.push("agent-connector.anp-task.v1#resume".to_string());
        }
        AnpCapabilities::parse(raw)
    }

    const PROFILE: &str = "agent-connector.anp-task.v1";

    fn offer() -> ProfileOffer {
        ProfileOffer {
            profiles: vec![PROFILE.to_string()],
        }
    }

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    #[test]
    fn valid_until_in_future_usable() {
        let n = negotiate_deterministic(
            &offer(),
            &caps(true),
            true,
            t0(),
            ChronoDuration::minutes(5),
        )
        .unwrap();
        assert!(n.is_valid(t0() + ChronoDuration::minutes(4)));
        assert!(!n.is_expired(t0() + ChronoDuration::minutes(4)));
    }

    #[test]
    fn expired_at_valid_until_boundary() {
        let n = negotiate_deterministic(
            &offer(),
            &caps(true),
            true,
            t0(),
            ChronoDuration::minutes(5),
        )
        .unwrap();
        // Boundary: now == valid_until is expired.
        assert!(n.is_expired(t0() + ChronoDuration::minutes(5)));
        assert!(!n.is_valid(t0() + ChronoDuration::minutes(5)));
    }

    #[test]
    fn past_valid_until_expired() {
        let n = negotiate_deterministic(
            &offer(),
            &caps(true),
            true,
            t0(),
            ChronoDuration::minutes(5),
        )
        .unwrap();
        assert!(n.is_expired(t0() + ChronoDuration::hours(1)));
        assert!(!n.is_valid(t0() + ChronoDuration::hours(1)));
    }

    #[test]
    fn expired_requires_renegotiation() {
        let n1 = negotiate_deterministic(
            &offer(),
            &caps(true),
            true,
            t0(),
            ChronoDuration::minutes(5),
        )
        .unwrap();
        let later = t0() + ChronoDuration::minutes(6);
        assert!(n1.is_expired(later));

        // Renegotiation at `later` yields a fresh validity window.
        let n2 = negotiate_deterministic(
            &offer(),
            &caps(true),
            true,
            later,
            ChronoDuration::minutes(5),
        )
        .unwrap();
        assert!(n2.is_valid(later));
        assert!(n2.negotiation_id != n1.negotiation_id);
    }

    #[test]
    fn no_common_profile_is_error() {
        let r = negotiate_deterministic(
            &offer(),
            &AnpCapabilities::parse(["direct.send".to_string()]),
            true,
            t0(),
            ChronoDuration::minutes(5),
        );
        assert!(r.is_err() && r.unwrap_err().is_no_common_profile());
    }

    #[test]
    fn peer_that_does_not_accept_offer_is_no_common_profile() {
        let r = negotiate_deterministic(
            &offer(),
            &caps(true),
            false,
            t0(),
            ChronoDuration::minutes(5),
        );
        assert!(r.is_err() && r.unwrap_err().is_no_common_profile());
    }

    #[test]
    fn resume_capability_reflected() {
        let with_resume = negotiate_deterministic(
            &offer(),
            &caps(true),
            true,
            t0(),
            ChronoDuration::minutes(5),
        )
        .unwrap();
        assert!(with_resume.capabilities.supports_resume);

        let without = negotiate_deterministic(
            &offer(),
            &caps(false),
            true,
            t0(),
            ChronoDuration::minutes(5),
        )
        .unwrap();
        assert!(!without.capabilities.supports_resume);
    }
}
