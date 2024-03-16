use crate::{
    anyhow, bail,
    user::{GUEST_USER, INTERNAL_USER},
    Result, User,
};
use std::{future::Future, sync::Arc};
use tokio::task_local;
use uuid::Uuid;

pub struct Session {
    parent: Option<Arc<Session>>,
    user: Arc<User>,
}
impl Session {
    pub fn new(user: Arc<User>) -> Arc<Self> {
        Arc::new(Self { parent: None, user })
    }

    pub fn inherit(user: Arc<User>) -> Arc<Self> {
        Arc::new(Self {
            parent: SESSION.try_with(|it| it.clone()).ok(),
            user,
        })
    }
}

pub async fn enter<R>(user: Arc<User>, f: impl Future<Output = R>) -> R {
    SESSION.scope(Session::inherit(user), f).await
}

#[inline]
pub async fn scope<R>(session: Arc<Session>, f: impl Future<Output = R>) -> R {
    SESSION.scope(session, f).await
}

task_local! {
    static SESSION: Arc<Session>;
}

pub fn current() -> Arc<Session> {
    SESSION
        .try_with(|it| it.clone())
        .unwrap_or_else(|_| Session::new(GUEST_USER.clone()))
}
pub fn user() -> Arc<User> {
    SESSION
        .try_with(|it| it.user.clone())
        .unwrap_or_else(|_| GUEST_USER.clone())
}
pub fn user_id() -> Option<Uuid> {
    SESSION.try_with(|it| it.user.id).unwrap_or(None)
}

pub fn has_perm(mut perm: &str) -> bool {
    SESSION.with(|session| loop {
        let mut cur = Some(session.as_ref());
        while let Some(it) = cur {
            if it.user.has_perm(perm) {
                return true;
            }
            cur = it.parent.as_deref();
        }

        if let Some(idx) = perm.rfind(['.', ':']) {
            perm = &perm[..idx];
        } else {
            return false;
        }
    })
}

pub fn check_perm(perm: &str) -> Result<()> {
    if !has_perm(perm) {
        bail!(@PermissionDenied "missing permission {perm}");
    }
    Ok(())
}

#[inline]
pub async fn internal_scope<R>(f: impl Future<Output = R>) -> R {
    enter(INTERNAL_USER.clone(), f).await
}
