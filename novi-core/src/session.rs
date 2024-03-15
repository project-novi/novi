use crate::{
    anyhow, bail,
    user::{self, GUEST_USER},
    Novi, Result, User,
};
use aes_gcm::{
    aead::{Aead, OsRng},
    AeadCore, Aes256Gcm, KeyInit,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dashmap::DashSet;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};
use tokio::task_local;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct FlattenedSession {
    user: Option<Uuid>,
    granted: Vec<String>,
}

pub struct Session {
    user: Arc<User>,
    granted: DashSet<String>,
}
impl Session {
    pub fn new(user: Arc<User>) -> Arc<Self> {
        Arc::new(Self {
            user,
            granted: DashSet::new(),
        })
    }

    pub(crate) fn grant(&self, perm: impl Into<String>) {
        self.granted.insert(perm.into());
    }

    pub async fn enter<R>(self: Arc<Self>, f: impl Future<Output = R>) -> R {
        SESSION.scope(self, f).await
    }

    fn flatten(&self) -> FlattenedSession {
        FlattenedSession {
            user: self.user.id,
            granted: self.granted.iter().map(|it| it.clone()).collect(),
        }
    }

    pub fn gen_token(&self, novi: &Novi) -> String {
        let bytes = postcard::to_allocvec(&self.flatten()).unwrap();
        let cipher = Aes256Gcm::new(&novi.session_key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let mut ciphertext = cipher.encrypt(&nonce, bytes.as_slice()).unwrap();
        ciphertext.extend_from_slice(nonce.as_slice());
        STANDARD.encode(ciphertext)
    }

    pub async fn from_token(novi: &Novi, token: &str) -> Result<Arc<Self>> {
        async fn inner(novi: &Novi, token: &str) -> Option<Arc<Session>> {
            let ciphertext = STANDARD.decode(token.as_bytes()).ok()?;
            let (ciphertext, nonce) = ciphertext.split_at(ciphertext.len() - 12);
            let cipher = Aes256Gcm::new(&novi.session_key);
            let bytes = cipher.decrypt(nonce.try_into().ok()?, ciphertext).ok()?;
            let flat: FlattenedSession = postcard::from_bytes(&bytes).ok()?;
            let user = match flat.user {
                Some(id) => novi.get_user(id).await,
                None => GUEST_USER.clone(),
            };
            Some(Arc::new(Session {
                user,
                granted: flat.granted.into_iter().collect(),
            }))
        }
        inner(novi, token)
            .await
            .ok_or_else(|| anyhow!(@InvalidToken))
    }
}

task_local! {
    static SESSION: Arc<Session>;
}

pub static GUEST_SESSION: Lazy<Arc<Session>> = Lazy::new(|| Session::new(user::GUEST_USER.clone()));
pub static INTERNAL_SESSION: Lazy<Arc<Session>> =
    Lazy::new(|| Session::new(user::INTERNAL_USER.clone()));

pub fn get() -> Arc<Session> {
    SESSION
        .try_with(|it| Arc::clone(&it))
        .unwrap_or_else(|_| GUEST_SESSION.clone())
}
pub fn get_user() -> Arc<User> {
    SESSION
        .try_with(|it| Arc::clone(&it.user))
        .unwrap_or_else(|_| GUEST_USER.clone())
}
pub fn user_id() -> Option<Uuid> {
    SESSION.try_with(|it| it.user.id).ok().flatten()
}

pub fn has_perm(mut perm: &str) -> bool {
    let session = get();
    if session.user.is_internal() {
        return true;
    }

    loop {
        if session.granted.contains(perm) {
            return true;
        }

        if let Some(idx) = perm.rfind(['.', ':']) {
            perm = &perm[..idx];
        } else {
            return false;
        }
    }
}

pub fn check_perm(perm: &str) -> Result<()> {
    if !has_perm(perm) {
        bail!(@PermissionDenied "missing permission {perm}");
    }
    Ok(())
}

#[inline]
pub async fn internal_scope<R>(f: impl Future<Output = R>) -> R {
    INTERNAL_SESSION.clone().enter(f).await
}
