use base64::Engine;
use rand::RngCore;
use std::{fmt, str::FromStr, sync::Arc};

use crate::{anyhow, Error};

pub type IdentityToken = SecretToken<24>;
pub type SessionToken = SecretToken<16>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct SecretToken<const N: usize>(Arc<[u8; N]>);
impl<const N: usize> SecretToken<N> {
    pub fn new() -> Self {
        let mut buf = [0; N];
        rand::thread_rng().fill_bytes(&mut buf);
        Self(Arc::new(buf))
    }

    pub fn as_bytes(&self) -> &[u8; N] {
        &self.0
    }
}
impl<const N: usize> fmt::Display for SecretToken<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.0.as_slice()))
    }
}

impl<const N: usize> FromStr for SecretToken<N> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let buf = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .ok()
            .and_then(|it| it.try_into().ok())
            .map(Arc::new)
            .ok_or_else(|| anyhow!(@InvalidArgument "invalid token"))?;
        Ok(Self(buf))
    }
}
