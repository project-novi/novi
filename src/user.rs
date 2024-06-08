use arc_swap::ArcSwap;
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use once_cell::sync::Lazy;
use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

use crate::{anyhow, bail, object::Object, Error, Result};

pub type UserRef = Arc<ArcSwap<User>>;

pub static INTERNAL_USER: Lazy<UserRef> = Lazy::new(|| {
    Arc::new(ArcSwap::from_pointee(User {
        id: None,
        name: "internal".to_owned(),
        password: None,
        roles: std::iter::once("admin".to_owned()).collect(),
        perms: HashSet::new(),
    }))
});

pub struct User {
    pub id: Option<Uuid>,
    pub name: String,
    pub password: Option<String>,

    pub roles: HashSet<String>,
    pub perms: HashSet<String>,
}

impl User {
    pub fn is_admin(&self) -> bool {
        self.roles.contains("admin")
    }

    pub fn verify(&self, password: &str) -> Result<()> {
        let Some(password_hash) = &self.password else {
            bail!(@InvalidCredentials "password incorrect")
        };
        if Argon2::default()
            .verify_password(
                password.as_bytes(),
                &PasswordHash::new(password_hash).unwrap(),
            )
            .is_err()
        {
            bail!(@InvalidCredentials "password incorrect")
        } else {
            Ok(())
        }
    }

    pub fn validate(&self) -> Result<()> {
        if !self.roles.contains("plugin") && !(4..=20).contains(&self.name.len()) {
            bail!(@InvalidArgument "name must be 4-20 characters long");
        }
        if !self
            .name
            .chars()
            .all(|it| it.is_alphanumeric() || "-_ .".contains(it))
        {
            bail!(@InvalidArgument "name can only contain alphanumeric characters, space, and -_.");
        }

        Ok(())
    }

    pub fn validate_password(password: &str) -> Result<()> {
        if !(8..=32).contains(&password.len()) {
            bail!(@InvalidArgument "password must be 8-32 characters long");
        }
        Ok(())
    }

    pub fn has_perm(&self, mut perm: &str) -> bool {
        if self.is_admin() {
            return true;
        }
        // TODO: Optimize using trie
        loop {
            if self.perms.contains(perm) {
                return true;
            }
            if let Some(pos) = perm.rfind([':', '.']) {
                perm = &perm[..pos];
            } else {
                return false;
            }
        }
    }
}

impl TryFrom<Object> for User {
    type Error = Error;

    fn try_from(value: Object) -> Result<Self> {
        fn inner(mut value: Object) -> Option<User> {
            let id = Some(value.id);

            value.remove_tag("@user")?;
            let name = value.remove_tag("@user.name")?.value?;
            let password = value.remove_tag("@user.password")?.value;

            let roles = value.subtags("@user.role").map(|it| it.0.to_owned()).collect();
            let perms = value.subtags("@user.perm").map(|it| it.0.to_owned()).collect();

            Some(User {
                id,
                name,
                password,

                roles,
                perms,
            })
        }

        let id = value.id;
        inner(value).ok_or_else(|| anyhow!(@InvalidObject ("id" => id.to_string()) "not a user"))
    }
}
