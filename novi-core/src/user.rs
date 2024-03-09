use crate::{anyhow, bail, Error, Model, Object, Result};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use once_cell::sync::Lazy;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    future::Future,
    sync::Arc,
    sync::Mutex,
};
use tokio::task_local;
use uuid::Uuid;

task_local! {
    pub static USER: Arc<User>;

    static GRANT_PERMS: Mutex<BTreeSet<String>>;
}

static INTERNAL_USER: Lazy<Arc<User>> = Lazy::new(|| {
    Arc::new(User {
        id: None,
        name: String::new(),
        password: String::new(),

        roles: std::iter::once("admin".to_owned()).collect(),
        perms: BTreeSet::new(),

        tags: BTreeMap::new(),
    })
});

pub async fn internal_scope<R>(f: impl Future<Output = R>) -> R {
    USER.scope(INTERNAL_USER.clone(), f).await
}

pub fn blocking_internal_scope<R>(f: impl FnOnce() -> R) -> R {
    USER.sync_scope(INTERNAL_USER.clone(), f)
}

pub async fn with_grant<R>(perm: &str, f: impl Future<Output = R>) -> R {
    match GRANT_PERMS.try_with(|it| it.lock().unwrap().insert(perm.to_owned())) {
        Err(_) => {
            GRANT_PERMS
                .scope(Mutex::default(), async move {
                    GRANT_PERMS.with(|it| {
                        it.lock().unwrap().insert(perm.to_owned());
                    });
                    let r = f.await;
                    GRANT_PERMS.with(|it| {
                        it.lock().unwrap().remove(perm);
                    });
                    r
                })
                .await
        }
        Ok(new) => {
            let r = f.await;
            if new {
                GRANT_PERMS.with(|it| {
                    it.lock().unwrap().remove(perm);
                });
            }
            r
        }
    }
}

static GUEST_USER: Lazy<Arc<User>> = Lazy::new(|| {
    Arc::new(User {
        id: None,
        name: String::new(),
        password: String::new(),

        roles: BTreeSet::new(),
        perms: BTreeSet::new(),

        tags: BTreeMap::new(),
    })
});

pub async fn guest_scope<R>(f: impl Future<Output = R>) -> R {
    USER.scope(GUEST_USER.clone(), f).await
}

pub struct User {
    pub id: Option<Uuid>,
    pub name: String,
    pub password: String,

    pub roles: BTreeSet<String>,
    pub perms: BTreeSet<String>,

    pub tags: BTreeMap<String, Option<String>>,
}

impl User {
    pub fn new(name: String, password: String) -> Self {
        Self {
            id: None,
            name,
            password,

            roles: BTreeSet::new(),
            perms: BTreeSet::new(),

            tags: BTreeMap::new(),
        }
    }

    pub fn temp_view(perms: BTreeSet<String>) -> Self {
        Self {
            id: None,
            name: String::new(),
            password: String::new(),

            roles: BTreeSet::new(),
            perms,

            tags: BTreeMap::new(),
        }
    }

    pub fn is_internal(&self) -> bool {
        self.roles.contains("admin")
    }

    pub fn verify(&self, password: &str) -> Result<()> {
        if Argon2::default()
            .verify_password(
                password.as_bytes(),
                &PasswordHash::new(&self.password).unwrap(),
            )
            .is_err()
        {
            bail!(@InvalidCredentials "password incorrect")
        } else {
            Ok(())
        }
    }
}

impl Model for User {
    fn id(&self) -> Uuid {
        self.id.unwrap()
    }

    fn to_tags(&self) -> BTreeMap<String, Option<String>> {
        let mut tags = self.tags.clone();
        tags.insert("@user".to_owned(), None);
        tags.insert("@user.name".to_owned(), Some(self.name.clone()));
        tags.insert("@user.password".to_owned(), Some(self.password.clone()));
        for role in &self.roles {
            tags.insert(format!("@user.role:{role}"), None);
        }
        for perm in &self.perms {
            tags.insert(format!("@user.perm:{perm}"), None);
        }

        tags
    }
}

impl TryFrom<Object> for User {
    type Error = Error;

    fn try_from(value: Object) -> Result<Self> {
        fn inner(mut value: Object) -> Option<User> {
            let id = Some(value.id);

            value.remove_tag("@user")?;
            let name = value.remove_tag("@user.name")?.value?;
            let password = value.remove_tag("@user.password")?.value?;

            let mut roles = BTreeSet::new();
            let mut perms = BTreeSet::new();

            let tags = value
                .into_pairs()
                .filter(|(tag, _)| {
                    if let Some(role) = tag.strip_prefix("@user.role:") {
                        roles.insert(role.to_owned());
                        false
                    } else if let Some(perm) = tag.strip_prefix("@user.perm:") {
                        perms.insert(perm.to_owned());
                        false
                    } else {
                        true
                    }
                })
                .collect();

            Some(User {
                id,
                name,
                password,

                roles,
                perms,

                tags,
            })
        }

        let id = value.id;
        inner(value).ok_or_else(|| anyhow!(@InvalidObject "object {id} is not a user"))
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum AccessKind {
    View = 0,
    Edit,
    Delete,
}
impl Display for AccessKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use AccessKind::*;
        let s = match self {
            View => "view",
            Edit => "edit",
            Delete => "delete",
        };
        f.write_str(s)
    }
}

pub fn id() -> Option<Uuid> {
    USER.with(|it| it.id)
}

pub fn has_perm(mut perm: &str) -> bool {
    USER.with(|it| {
        if it.is_internal() {
            return true;
        }

        loop {
            if it.perms.contains(perm)
                || GRANT_PERMS
                    .try_with(|it| it.lock().unwrap().contains(perm))
                    .map_or(false, |it| it)
            {
                return true;
            }

            if let Some(idx) = perm.rfind(['.', ':']) {
                perm = &perm[..idx];
            } else {
                return false;
            }
        }
    })
}

pub fn check_perm(perm: &str) -> Result<()> {
    if !has_perm(perm) {
        bail!(@PermissionDenied "missing permission {perm}");
    }
    Ok(())
}
