//! ANP transport boundary.
//!
//! This crate defines the profile-aware transport abstraction that sits
//! between `agent-connector` and an ANP substrate (DID/WNS, signatures,
//! E2EE, `direct.send`, `anp.get_capabilities`, `anp.negotiate`).
//!
//! Per `docs/design/ф-0 (п-3)/anp-p0-execution-handoff.md` §5 this layer is
//! intentionally trait-only plus a fake transport for test fixtures. It does
//! NOT couple to the upstream `anp` SDK yet: that adapter is added once a
//! real interop peer exists and the transport implementation starts.

mod capabilities;
mod error;
mod fake;
mod message;
mod negotiation;
mod peer;
mod transport;

pub use capabilities::{AnpCapabilities, AnpCapability};
pub use error::{AnpError, AnpResult};
pub use fake::{FakeAnpTransport, FakePeerSpec};
pub use message::{AnpAccepted, AnpMessage, AnpMessageBody, AnpMessageId};
pub use negotiation::{
    profile_selection, NegotiatedProfile, NegotiationStatus, ProfileOffer, SelectedProfile,
};
pub use peer::{PeerIdentity, PeerRef, TrustLevel, VerifiedAnpPeer};
pub use transport::AnpTransport;
