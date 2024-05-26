use chrono::{DateTime, Utc};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use serde_json::json;
use std::{collections::HashSet, sync::Arc};

use crate::{
    anyhow, bail,
    novi::Novi,
    token::IdentityToken,
    user::{User, UserRef},
    Result,
};

pub(crate) static IDENTITIES: Lazy<DashMap<IdentityToken, Arc<Identity>>> =
    Lazy::new(Default::default);

pub struct Identity {
    pub user: UserRef,
    guest_user: Arc<User>,
    permission_subset: Option<HashSet<String>>,
    expire_at: Option<DateTime<Utc>>,
}

impl Identity {
    pub fn new_user(
        user: UserRef,
        guest_user: Arc<User>,
        expire_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            user,
            guest_user,
            permission_subset: None,
            expire_at,
        }
    }

    pub fn is_admin(&self) -> bool {
        self.user.load().is_admin() && self.permission_subset.is_none()
    }

    pub fn permissions(&self) -> Vec<String> {
        if let Some(subset) = &self.permission_subset {
            subset.iter().cloned().collect()
        } else {
            self.user
                .load()
                .perms
                .iter()
                .chain(self.guest_user.perms.iter())
                .cloned()
                .collect()
        }
    }

    pub fn has_perm(&self, perm: &str) -> bool {
        if let Some(subset) = &self.permission_subset {
            subset.contains(perm)
        } else {
            self.guest_user.has_perm(perm) || self.user.load().has_perm(perm)
        }
    }

    pub fn check_perm(&self, perm: &str) -> Result<()> {
        if !self.has_perm(perm) {
            bail!(@PermissionDenied ("permission" => perm.to_owned()) "permission denied");
        }
        Ok(())
    }

    pub fn redis_key(token: &IdentityToken) -> Vec<u8> {
        let mut buf = b"novi:identity:".to_vec();
        buf.extend_from_slice(token.as_bytes());
        buf
    }

    pub async fn save_to_db(&self, novi: &Novi, token: &IdentityToken) -> Result<()> {
        let data = self.encode().to_string();
        novi.redis_pool
            .get()
            .await?
            .set(Self::redis_key(token), data)
            .await?;
        Ok(())
    }

    pub fn encode(&self) -> serde_json::Value {
        json!({
            "user": self.user.load().id.map(|it| it.to_string()),
            "subset": self.permission_subset,
            "expire_at": self.expire_at,
        })
    }

    pub async fn decode(novi: &Novi, value: serde_json::Value) -> Result<Self> {
        let user_id = value["user"]
            .as_str()
            .map(|it| it.parse())
            .transpose()
            .map_err(|_| anyhow!("invalid identity"))?;
        let user = if let Some(user_id) = user_id {
            novi.get_user(user_id).await?
        } else {
            Arc::new(novi.guest_user.clone().into())
        };
        let expire_at = value["expire_at"]
            .as_str()
            .map(|it| it.parse())
            .transpose()
            .map_err(|_| anyhow!("invalid identity"))?;
        if expire_at.map_or(false, |it| it < Utc::now()) {
            bail!(@IdentityExpired "identity expired");
        }
        Ok(Self {
            user,
            guest_user: novi.guest_user.clone(),
            permission_subset: value["subset"]
                .as_array()
                .map(|it| {
                    it.iter()
                        .map(|it| {
                            it.as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| anyhow!("invalid identity"))
                        })
                        .collect::<Result<HashSet<_>>>()
                })
                .transpose()?,
            expire_at,
        })
    }
}
