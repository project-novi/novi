use crate::{user::INTERNAL_USER, User};
use serde::Deserialize;
use std::{process, sync::Arc};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub authors: Vec<String>,
    pub license: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default)]
    pub itags: Vec<String>,

    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub disabled: bool,
}
impl PluginInfo {
    pub fn new_user(&self) -> Arc<User> {
        if self.internal {
            return INTERNAL_USER.clone();
        }

        let perms = self
            .permissions
            .iter()
            .cloned()
            .chain(self.itags.iter().map(|itag| format!("itag:{itag}")))
            .collect();

        Arc::new(User::phony(perms))
    }
}

pub(crate) struct PluginState {
    pub secret_key: Uuid,
    pub info: PluginInfo,
    pub process: process::Child,
}
