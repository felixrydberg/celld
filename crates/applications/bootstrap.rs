//! Private host-selected bootstrap shared by language adapters. Resolved values
//! are sensitive: this type deliberately has no Debug implementation.

use crate::{ApplicationId, Handler, Name};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const VERSION: u16 = 1;
pub const DISK_BYTES: usize = 65536;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub version: u16,
    pub application: ApplicationId,
    pub deployment: Name,
    pub manifest_sha256: String,
    pub handler: Handler,
}

impl Bootstrap {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.version == VERSION, "unsupported guest bootstrap");
        ensure!(
            self.manifest_sha256.len() == 64
                && self
                    .manifest_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid bootstrap manifest digest"
        );
        self.handler.validate()?;
        ensure!(
            self.handler.secrets.is_empty(),
            "unresolved bootstrap secrets"
        );
        Ok(())
    }

    pub fn encode_disk(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = serde_json::to_vec(self)?;
        ensure!(
            bytes.len() < DISK_BYTES,
            "guest bootstrap exceeds disk limit"
        );
        bytes.resize(DISK_BYTES, 0);
        Ok(bytes)
    }

    pub fn decode_disk(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == DISK_BYTES, "invalid bootstrap disk size");
        let end = bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
        let bootstrap: Self = serde_json::from_slice(&bytes[..end])
            .map_err(|_| anyhow::anyhow!("invalid guest bootstrap JSON"))?;
        bootstrap.validate()?;
        Ok(bootstrap)
    }
}

#[cfg(test)]
#[path = "unit_tests/bootstrap/tests.rs"]
mod tests;
