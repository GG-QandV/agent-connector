//! Fake transport for test fixtures.
//!
//! **Not for production.** Implements [`AnpTransport`] over an in-memory
//! peer catalog. Identity verification succeeds only for `localhost`
//! endpoints (insecure-dev fixture), matching the handoff rule that
//! no-auth/insecure mode exists solely for test fixtures.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::HashMap;
use std::sync::Arc;

use crate::capabilities::AnpCapabilities;
use crate::error::{AnpError, AnpResult};
use crate::message::{AnpAccepted, AnpMessage};
use crate::negotiation::{negotiate_deterministic, NegotiatedProfile, ProfileOffer};
use crate::peer::{PeerIdentity, PeerRef, TrustLevel, VerifiedAnpPeer};
use crate::transport::AnpTransport;

/// A fake peer entry.
#[derive(Debug, Clone)]
pub struct FakePeerSpec {
    /// Endpoint the peer is reachable at (localhost fixture).
    pub endpoint: String,
    /// DID the peer presents.
    pub did: String,
    /// Advertised capabilities.
    pub capabilities: AnpCapabilities,
    /// Whether the peer accepts a negotiation offer.
    pub accepts_offer: bool,
    /// Validity window granted for each negotiation result.
    pub negotiation_validity: ChronoDuration,
}

impl FakePeerSpec {
    pub fn new(endpoint: impl Into<String>, did: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            did: did.into(),
            capabilities: AnpCapabilities::default(),
            accepts_offer: false,
            negotiation_validity: ChronoDuration::minutes(5),
        }
    }

    pub fn with_capabilities<'a>(mut self, caps: impl IntoIterator<Item = &'a str>) -> Self {
        self.capabilities = AnpCapabilities::parse(caps.into_iter().map(str::to_owned));
        self
    }

    pub fn with_accepts_offer(mut self, accepts: bool) -> Self {
        self.accepts_offer = accepts;
        self
    }

    pub fn with_negotiation_validity(mut self, validity: ChronoDuration) -> Self {
        self.negotiation_validity = validity;
        self
    }
}

/// Injectable clock so tests exercise expiry deterministically without
/// wall-clock sleeps.
type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// In-memory fake transport backed by a peer catalog.
pub struct FakeAnpTransport {
    peers: Arc<HashMap<String, FakePeerSpec>>,
    clock: Clock,
    /// Expected peer identity, enforced even for `InsecureDev`.
    /// Per security policy §1, `InsecureDev` still requires an explicitly
    /// configured expected identity (DID or key) — it only relaxes the
    /// localhost/production restriction, not the identity requirement.
    expected_identity: PeerIdentity,
}

impl std::fmt::Debug for FakeAnpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeAnpTransport")
            .field("peers", &self.peers)
            .field("clock", &"<fn>")
            .field("expected_identity", &self.expected_identity)
            .finish()
    }
}

impl Default for FakeAnpTransport {
    fn default() -> Self {
        Self::new_with_clock(
            std::iter::empty(),
            Arc::new(Utc::now),
            PeerIdentity {
                did: String::new(),
                key_fingerprint: String::new(),
            },
        )
    }
}

impl FakeAnpTransport {
    pub fn new(
        peers: impl IntoIterator<Item = FakePeerSpec>,
        expected_identity: PeerIdentity,
    ) -> Self {
        Self::new_with_clock(peers, Arc::new(Utc::now), expected_identity)
    }

    pub fn new_with_clock(
        peers: impl IntoIterator<Item = FakePeerSpec>,
        clock: Clock,
        expected_identity: PeerIdentity,
    ) -> Self {
        let mut map = HashMap::new();
        for p in peers {
            map.insert(p.endpoint.clone(), p);
        }
        Self {
            peers: Arc::new(map),
            clock,
            expected_identity,
        }
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }
}

fn fingerprint(did: &str) -> String {
    format!("fake-key:{}", did)
}

/// Identity a peer in the catalog presents for the given spec.
fn presented_identity(spec: &FakePeerSpec) -> PeerIdentity {
    PeerIdentity {
        did: spec.did.clone(),
        key_fingerprint: fingerprint(&spec.did),
    }
}

#[async_trait]
impl AnpTransport for FakeAnpTransport {
    async fn connect(&self, peer: PeerRef) -> AnpResult<VerifiedAnpPeer> {
        let spec = self
            .peers
            .get(&peer.endpoint)
            .ok_or_else(|| AnpError::Transport(format!("unknown peer {}", peer.endpoint)))?;

        // Insecure-dev fixture: only localhost endpoints are allowed, and
        // only when no production trust pins were supplied.
        let is_localhost = peer.endpoint.starts_with("http://127.0.0.1")
            || peer.endpoint.starts_with("http://localhost")
            || peer.endpoint.starts_with("did:anp:local:");
        if !is_localhost {
            return Err(AnpError::IdentityVerificationFailed(format!(
                "fake transport refuses non-localhost endpoint {}",
                peer.endpoint
            )));
        }
        if peer.expected_did.is_some() || peer.expected_key_fingerprint.is_some() {
            return Err(AnpError::IdentityVerificationFailed(
                "production trust pins are not supported by the fake transport".to_string(),
            ));
        }

        // Security policy §1: even InsecureDev requires an explicitly
        // configured expected identity. The presented identity must match;
        // an unconfigured/empty expected identity is a hard failure.
        let presented = presented_identity(spec);
        if !presented.matches_expected(&self.expected_identity) {
            return Err(AnpError::IdentityVerificationFailed(format!(
                "presented identity {} does not match configured expected identity {}",
                presented.did, self.expected_identity.did
            )));
        }

        Ok(VerifiedAnpPeer {
            identity: presented,
            trust: TrustLevel::InsecureDev,
            did_document: None,
        })
    }

    async fn capabilities(&self, peer: &VerifiedAnpPeer) -> AnpResult<AnpCapabilities> {
        self.peers
            .values()
            .find(|s| s.did == peer.identity.did)
            .map(|s| s.capabilities.clone())
            .ok_or_else(|| AnpError::NotConnected)
    }

    async fn negotiate(
        &self,
        peer: &VerifiedAnpPeer,
        offer: ProfileOffer,
    ) -> AnpResult<NegotiatedProfile> {
        let spec = self
            .peers
            .values()
            .find(|s| s.did == peer.identity.did)
            .ok_or_else(|| AnpError::NotConnected)?;
        negotiate_deterministic(
            &offer,
            &spec.capabilities,
            spec.accepts_offer,
            self.now(),
            spec.negotiation_validity,
        )
    }

    async fn send(&self, peer: &VerifiedAnpPeer, message: AnpMessage) -> AnpResult<AnpAccepted> {
        let spec = self
            .peers
            .values()
            .find(|s| s.did == peer.identity.did)
            .ok_or_else(|| AnpError::NotConnected)?;
        // The fake substrate accepts any message from a verified peer.
        let _ = spec;
        Ok(AnpAccepted {
            message_id: message.message_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::AnpMessageId;

    fn task_peer() -> FakePeerSpec {
        FakePeerSpec::new("http://127.0.0.1:9901", "did:anp:local:peer-a")
            .with_capabilities([
                "agent-connector.anp-task.v1",
                "agent-connector.anp-task.v1#resume",
            ])
            .with_accepts_offer(true)
    }

    fn messaging_peer() -> FakePeerSpec {
        FakePeerSpec::new("http://127.0.0.1:9902", "did:anp:local:peer-b")
            .with_capabilities(["direct.send"])
            .with_accepts_offer(true)
    }

    fn remote_peer() -> FakePeerSpec {
        FakePeerSpec::new("https://anp.example.com/peer", "did:anp:peer-c")
    }

    /// Expected identity for `task_peer` (matches its did/key).
    fn task_expected() -> PeerIdentity {
        PeerIdentity {
            did: "did:anp:local:peer-a".into(),
            key_fingerprint: fingerprint("did:anp:local:peer-a"),
        }
    }

    /// Expected identity for `messaging_peer`.
    fn messaging_expected() -> PeerIdentity {
        PeerIdentity {
            did: "did:anp:local:peer-b".into(),
            key_fingerprint: fingerprint("did:anp:local:peer-b"),
        }
    }

    fn peer_ref(endpoint: &str) -> PeerRef {
        PeerRef {
            endpoint: endpoint.into(),
            expected_did: None,
            expected_key_fingerprint: None,
        }
    }

    #[tokio::test]
    async fn connect_localhost_succeeds_insecure_dev() {
        let t = FakeAnpTransport::new([task_peer()], task_expected());
        let p = t.connect(peer_ref("http://127.0.0.1:9901")).await.unwrap();
        assert_eq!(p.identity.did, "did:anp:local:peer-a");
        assert_eq!(p.trust, TrustLevel::InsecureDev);
    }

    #[tokio::test]
    async fn connect_remote_refuses_non_localhost() {
        let t = FakeAnpTransport::new([remote_peer()], task_expected());
        let r = t.connect(peer_ref("https://anp.example.com/peer")).await;
        assert!(r.is_err() && r.unwrap_err().is_identity_failure());
    }

    #[tokio::test]
    async fn connect_with_production_pin_fails_no_fallback() {
        let t = FakeAnpTransport::new([task_peer()], task_expected());
        let r = t
            .connect(PeerRef {
                endpoint: "http://127.0.0.1:9901".into(),
                expected_did: Some("did:anp:real".into()),
                expected_key_fingerprint: None,
            })
            .await;
        assert!(r.is_err() && r.unwrap_err().is_identity_failure());
    }

    #[tokio::test]
    async fn insecure_dev_without_expected_identity_rejected() {
        // Empty expected identity = nothing configured = hard failure.
        let t = FakeAnpTransport::new(
            [task_peer()],
            PeerIdentity {
                did: String::new(),
                key_fingerprint: String::new(),
            },
        );
        let r = t.connect(peer_ref("http://127.0.0.1:9901")).await;
        assert!(r.is_err() && r.unwrap_err().is_identity_failure());
    }

    #[tokio::test]
    async fn insecure_dev_mismatched_identity_rejected() {
        // Expected identity for peer-b, but connecting to peer-a on localhost.
        let t = FakeAnpTransport::new([task_peer()], messaging_expected());
        let r = t.connect(peer_ref("http://127.0.0.1:9901")).await;
        assert!(r.is_err() && r.unwrap_err().is_identity_failure());
    }

    #[tokio::test]
    async fn insecure_dev_matched_identity_localhost_accepted() {
        let t = FakeAnpTransport::new([task_peer()], task_expected());
        let p = t.connect(peer_ref("http://127.0.0.1:9901")).await.unwrap();
        assert_eq!(p.identity.did, "did:anp:local:peer-a");
        assert_eq!(p.trust, TrustLevel::InsecureDev);
    }

    #[tokio::test]
    async fn insecure_dev_matched_did_alone_accepted() {
        // Expected identity configured by DID only; key not configured -> only
        // DID is enforced.
        let t = FakeAnpTransport::new(
            [task_peer()],
            PeerIdentity {
                did: "did:anp:local:peer-a".into(),
                key_fingerprint: String::new(),
            },
        );
        let p = t.connect(peer_ref("http://127.0.0.1:9901")).await.unwrap();
        assert_eq!(p.identity.did, "did:anp:local:peer-a");
    }

    #[tokio::test]
    async fn insecure_dev_mismatched_did_rejected_even_with_matching_key() {
        // Key matches but DID does not -> reject.
        let t = FakeAnpTransport::new(
            [task_peer()],
            PeerIdentity {
                did: "did:anp:local:other".into(),
                key_fingerprint: fingerprint("did:anp:local:peer-a"),
            },
        );
        let r = t.connect(peer_ref("http://127.0.0.1:9901")).await;
        assert!(r.is_err() && r.unwrap_err().is_identity_failure());
    }

    #[tokio::test]
    async fn negotiate_selects_task_profile() {
        let t = FakeAnpTransport::new([task_peer()], task_expected());
        let peer = t.connect(peer_ref("http://127.0.0.1:9901")).await.unwrap();
        let caps = t.capabilities(&peer).await.unwrap();
        assert!(caps.contains("agent-connector.anp-task.v1"));

        let n = t
            .negotiate(
                &peer,
                ProfileOffer {
                    profiles: vec!["agent-connector.anp-task.v1".into()],
                },
            )
            .await
            .unwrap();
        assert_eq!(n.profile_id, "agent-connector.anp-task.v1");
        assert!(n.capabilities.supports_resume);
        assert!(!n.negotiation_id.is_empty());
        assert!(n.is_valid(chrono::Utc::now()));
    }

    #[tokio::test]
    async fn negotiation_result_expires() {
        use chrono::TimeZone;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let t =
            FakeAnpTransport::new_with_clock([task_peer()], Arc::new(move || t0), task_expected());
        let peer = t.connect(peer_ref("http://127.0.0.1:9901")).await.unwrap();
        let n = t
            .negotiate(
                &peer,
                ProfileOffer {
                    profiles: vec!["agent-connector.anp-task.v1".into()],
                },
            )
            .await
            .unwrap();
        assert!(n.is_valid(t0));
        assert!(n.is_expired(t0 + chrono::Duration::minutes(6)));
    }

    #[tokio::test]
    async fn no_common_profile_falls_back_messaging_only() {
        let t = FakeAnpTransport::new([messaging_peer()], messaging_expected());
        let peer = t.connect(peer_ref("http://127.0.0.1:9902")).await.unwrap();
        let r = t
            .negotiate(
                &peer,
                ProfileOffer {
                    profiles: vec!["agent-connector.anp-task.v1".into()],
                },
            )
            .await;
        assert!(r.is_err() && r.unwrap_err().is_no_common_profile());
    }

    #[tokio::test]
    async fn send_accepts_from_verified_peer() {
        let t = FakeAnpTransport::new([task_peer()], task_expected());
        let peer = t.connect(peer_ref("http://127.0.0.1:9901")).await.unwrap();
        let acc = t
            .send(
                &peer,
                AnpMessage {
                    message_id: AnpMessageId("m-1".into()),
                    kind: "task.invoke".into(),
                    body: crate::AnpMessageBody::Json("{}".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(acc.message_id.0, "m-1");
    }
}
