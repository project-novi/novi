use crate::{anyhow, bail, Error, Model, Novi, Object, Result};
use aes_gcm::{
    aead::{Aead, OsRng},
    AeadCore, Aes256Gcm, KeyInit,
};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use base64::{engine::general_purpose::STANDARD, Engine};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    sync::Arc,
};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
enum FlattenedUser {
    Internal,
    User(Uuid),
    Phony(BTreeSet<String>),
}

pub(crate) static INTERNAL_USER: Lazy<Arc<User>> = Lazy::new(|| {
    Arc::new(User {
        id: None,
        name: "internal".to_owned(),
        password: String::new(),
        roles: std::iter::once("admin".to_owned()).collect(),
        perms: BTreeSet::new(),
        tags: BTreeMap::new(),
    })
});
pub(crate) static GUEST_USER: Lazy<Arc<User>> =
    Lazy::new(|| Arc::new(User::phony(BTreeSet::new())));

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

    pub fn phony(perms: BTreeSet<String>) -> Self {
        Self {
            id: None,
            name: String::new(),
            password: String::new(),

            roles: BTreeSet::new(),
            perms,

            tags: BTreeMap::new(),
        }
    }

    fn flatten(&self) -> FlattenedUser {
        if let Some(id) = self.id {
            FlattenedUser::User(id)
        } else {
            if self.is_admin() {
                FlattenedUser::Internal
            } else {
                FlattenedUser::Phony(self.perms.clone())
            }
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
        async fn inner(novi: &Novi, token: &str) -> Option<Arc<User>> {
            let ciphertext = STANDARD.decode(token.as_bytes()).ok()?;
            let (ciphertext, nonce) = ciphertext.split_at(ciphertext.len() - 12);
            let cipher = Aes256Gcm::new(&novi.session_key);
            let bytes = cipher.decrypt(nonce.try_into().ok()?, ciphertext).ok()?;
            let flat: FlattenedUser = postcard::from_bytes(&bytes).ok()?;
            Some(match flat {
                FlattenedUser::Internal => INTERNAL_USER.clone(),
                FlattenedUser::User(id) => novi.get_user(id).await,
                FlattenedUser::Phony(perms) => Arc::new(User::phony(perms)),
            })
        }
        inner(novi, token)
            .await
            .ok_or_else(|| anyhow!(@InvalidToken))
    }

    pub fn is_admin(&self) -> bool {
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

    pub fn has_perm(&self, perm: &str) -> bool {
        self.is_admin() || self.perms.contains(perm)
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
