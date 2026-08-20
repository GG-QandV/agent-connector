//! Fake transport for test fixtures.
//!
//! **Not for production.** Implements [`AnpTransport`] over an in-memory
//! peer catalog. Identity verification succeeds only for `localhost`
//! endpoints (insecure-dev fixture), matching the handoff rule that
//! no-auth/insecure mode exists solely for test fixtures.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use crate::capabilities::{AnpCapabilities, AnpCapability};
use crate::error::{AnpError, AnpResult};
use crate::message::{AnpAccepted, AnpMessage};
use crate::negotiation::{
    negotiate_deterministic, NegotiatedProfile, NegotiationStatus, ProfileOffer,
};
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
}

impl FakePeerSpec {
    pub fn new(endpoint: impl Into<String>, did: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            did: did.into(),
            capabilities: AnpCapabilities::default(),
            accepts_offer: false,
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
}

/// In-memory fake transport backed by a peer catalog.
#[derive(Debug, Clone, Default)]
pub struct FakeAnpTransport {
    peers: Arc<HashMap<String, FakePeerSpec>>,
}

impl FakeAnpTransport {
    pub fn new(peers: impl IntoIterator<Item = FakePeerSpec>) -> Self {
        let mut map = HashMap::new();
        for p in peers {
            map.insert(p.endpoint.clone(), p);
        }
        Self {
            peers: Arc::new(map),
        }
    }
}

fn fingerprint(did: &str) -> String {
    format!("fake-key:{}", did)
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

        Ok(VerifiedAnpPeer {
            identity: PeerIdentity {
                did: spec.did.clone(),
                key_fingerprint: fingerprint(&spec.did),
            },
            trust: TrustLevel::InsecureDev,
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
        let negotiated = negotiate_deterministic(&offer, &spec.capabilities, spec.accepts_offer)?;
        if negotiated.status == NegotiationStatus::NoCommonProfile {
            let peer_caps: Vec<String> = spec
                .capabilities
                .raw
                .iter()
                .map(|AnpCapability(s)| s.clone())
                .collect();
            return Err(AnpError::NoCommonProfile {
                peer_capabilities: peer_caps,
                offered: offer.profiles,
            });
        }
        Ok(negotiated)
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

    #[tokio::test]
    async fn connect_localhost_succeeds_insecure_dev() {
        let t = FakeAnpTransport::new([task_peer()]);
        let p = t
            .connect(PeerRef {
                endpoint: "http://127.0.0.1:9901".into(),
                expected_did: None,
                expected_key_fingerprint: None,
            })
            .await
            .unwrap();
        assert_eq!(p.identity.did, "did:anp:local:peer-a");
        assert_eq!(p.trust, TrustLevel::InsecureDev);
    }

    #[tokio::test]
    async fn connect_remote_refuses_non_localhost() {
        let t = FakeAnpTransport::new([remote_peer()]);
        let r = t
            .connect(PeerRef {
                endpoint: "https://anp.example.com/peer".into(),
                expected_did: None,
                expected_key_fingerprint: None,
            })
            .await;
        assert!(r.is_err() && r.unwrap_err().is_identity_failure());
    }

    #[tokio::test]
    async fn connect_with_production_pin_fails_no_fallback() {
        let t = FakeAnpTransport::new([task_peer()]);
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
    async fn negotiate_selects_task_profile() {
        let t = FakeAnpTransport::new([task_peer()]);
        let peer = t
            .connect(PeerRef {
                endpoint: "http://127.0.0.1:9901".into(),
                expected_did: None,
                expected_key_fingerprint: None,
            })
            .await
            .unwrap();
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
        assert_eq!(n.status, NegotiationStatus::Accepted);
        let sel = n.selected.unwrap();
        assert_eq!(sel.profile_id, "agent-connector.anp-task.v1");
        assert!(sel.supports_resume);
    }

    #[tokio::test]
    async fn no_common_profile_falls_back_messaging_only() {
        let t = FakeAnpTransport::new([messaging_peer()]);
        let peer = t
            .connect(PeerRef {
                endpoint: "http://127.0.0.1:9902".into(),
                expected_did: None,
                expected_key_fingerprint: None,
            })
            .await
            .unwrap();
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
        let t = FakeAnpTransport::new([task_peer()]);
        let peer = t
            .connect(PeerRef {
                endpoint: "http://127.0.0.1:9901".into(),
                expected_did: None,
                expected_key_fingerprint: None,
            })
            .await
            .unwrap();
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
