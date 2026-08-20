//! Peer identity state.

use std::fmt;

/// A raw peer reference before identity verification.
///
/// Contains whatever discovery gave us (DID, endpoint, key fingerprint),
/// but carries **no trust**. Identity must be verified via
/// [`crate::AnpTransport::connect`] before any message is sent.
#[derive(Debug, Clone)]
pub struct PeerRef {
    /// Endpoint / channel address (e.g. `did:anp:...`, or a localhost fixture URL).
    pub endpoint: String,
    /// Optional pinned DID string for trust binding.
    pub expected_did: Option<String>,
    /// Optional pinned key fingerprint (base64/hex, as published by peer).
    pub expected_key_fingerprint: Option<String>,
}

/// How much trust a peer identity has been granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustLevel {
    /// Production trust policy: pinned DID/key fingerprint, verified.
    Verified,
    /// Localhost test fixture only — never for production.
    InsecureDev,
}

/// A peer whose identity has been verified through a transport policy.
#[derive(Debug, Clone)]
pub struct VerifiedAnpPeer {
    /// Verified identity claims.
    pub identity: PeerIdentity,
    /// Trust level under which the peer was verified.
    pub trust: TrustLevel,
}

/// Verified identity claims for an ANP peer.
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// DID string the peer presented and was verified against.
    pub did: String,
    /// Verified key fingerprint.
    pub key_fingerprint: String,
}

impl PeerIdentity {
    /// Whether this identity is fully empty (nothing configured).
    pub fn is_empty(&self) -> bool {
        self.did.trim().is_empty() && self.key_fingerprint.trim().is_empty()
    }

    /// Whether the identity matches against a configured expected identity.
    ///
    /// The expected identity is considered configured if it carries at least
    /// one non-empty claim (DID or key fingerprint). Only the claims that are
    /// explicitly configured are enforced; an empty expected identity matches
    /// nothing (strict).
    pub fn matches_expected(&self, expected: &PeerIdentity) -> bool {
        if expected.is_empty() {
            return false;
        }
        if !expected.did.trim().is_empty() && expected.did != self.did {
            return false;
        }
        if !expected.key_fingerprint.trim().is_empty()
            && expected.key_fingerprint != self.key_fingerprint
        {
            return false;
        }
        true
    }
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (key {})",
            self.did,
            &self.key_fingerprint[..self.key_fingerprint.len().min(12)]
        )
    }
}
