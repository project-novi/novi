use crate::{
    bail,
    user::{self, GUEST_USER},
    Result, User,
};
use dashmap::DashSet;
use once_cell::sync::Lazy;
use std::{future::Future, sync::Arc};
use tokio::task_local;
use uuid::Uuid;

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
}

task_local! {
    static SESSION: Arc<Session>;
}

pub static GUEST_SESSION: Lazy<Arc<Session>> =
    Lazy::new(|| Session::new(user::GUEST_USER.clone()));
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
