//! ANP message envelope.

use std::fmt;

/// Message ID — must be unique per sender for dedup/idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnpMessageId(pub String);

impl fmt::Display for AnpMessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Body of an ANP message. Kept opaque at the transport layer; typed
/// interpretation happens in the profile layer (`protocol-anp-profile`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnpMessageBody {
    /// Typed JSON payload (the profile schema) serialized to a string.
    Json(String),
    /// Opaque bytes (e.g. E2EE-encrypted payload).
    Binary(Vec<u8>),
}

/// A message sent over ANP `direct.send`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnpMessage {
    pub message_id: AnpMessageId,
    pub kind: String,
    pub body: AnpMessageBody,
}

/// Positive acknowledgement from the substrate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnpAccepted {
    pub message_id: AnpMessageId,
}

impl fmt::Display for AnpMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "anp:{}:{}", self.kind, self.message_id)
    }
}
