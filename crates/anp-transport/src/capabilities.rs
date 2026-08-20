//! ANP capability model and parsing.

/// A capability advertised by a peer (e.g. `agent-connector.anp-task.v1`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AnpCapability(pub String);

impl AsRef<str> for AnpCapability {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Parsed set of capabilities a peer advertises.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnpCapabilities {
    /// Raw capability list from `anp.get_capabilities`.
    pub raw: Vec<AnpCapability>,
}

impl AnpCapabilities {
    /// Parses a raw capability list, trimming and skipping empty entries.
    pub fn parse(raw: impl IntoIterator<Item = String>) -> Self {
        let mut caps: Vec<AnpCapability> = raw
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(AnpCapability)
            .collect();
        caps.sort();
        caps.dedup();
        Self { raw: caps }
    }

    /// Returns true if the peer advertises the given capability.
    pub fn contains(&self, capability: &str) -> bool {
        self.raw.iter().any(|c| c.as_ref() == capability)
    }

    /// Returns the subset of `wanted` that the peer advertises.
    pub fn intersection<'a>(&self, wanted: &[&'a str]) -> Vec<&'a str> {
        wanted
            .iter()
            .copied()
            .filter(|w| self.contains(w))
            .collect()
    }
}
