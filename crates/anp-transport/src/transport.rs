//! ANP transport trait.

use async_trait::async_trait;

use crate::capabilities::AnpCapabilities;
use crate::error::AnpResult;
use crate::message::{AnpAccepted, AnpMessage};
use crate::negotiation::{NegotiatedProfile, ProfileOffer};
use crate::peer::{PeerRef, VerifiedAnpPeer};

/// Profile-aware ANP transport boundary.
///
/// A `send` success means the substrate **accepted the message** — it does
/// NOT mean a task was executed. Task semantics live in the profile layer.
#[async_trait]
pub trait AnpTransport: Send + Sync {
    /// Establish a connection and verify the peer's identity.
    ///
    /// On identity failure returns [`crate::AnpError::IdentityVerificationFailed`];
    /// there is NO fallback to insecure mode in production. The fake
    /// transport uses [`crate::TrustLevel::InsecureDev`] for localhost
    /// fixtures only.
    async fn connect(&self, peer: PeerRef) -> AnpResult<VerifiedAnpPeer>;

    /// Fetch the peer's advertised capabilities.
    async fn capabilities(&self, peer: &VerifiedAnpPeer) -> AnpResult<AnpCapabilities>;

    /// Negotiate a profile from the local offer.
    ///
    /// Returns a usable [`NegotiatedProfile`] when the peer accepts a common
    /// profile, or [`crate::AnpError::NoCommonProfile`] otherwise — the
    /// caller then falls back to messaging-only.
    async fn negotiate(
        &self,
        peer: &VerifiedAnpPeer,
        offer: ProfileOffer,
    ) -> AnpResult<NegotiatedProfile>;

    /// Send a message via the substrate (`direct.send`).
    ///
    /// An [`AnpAccepted`] confirms delivery/acceptance at the substrate
    /// level only, not task completion.
    async fn send(&self, peer: &VerifiedAnpPeer, message: AnpMessage) -> AnpResult<AnpAccepted>;
}
