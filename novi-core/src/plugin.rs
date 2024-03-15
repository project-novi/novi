use crate::{session::INTERNAL_SESSION, user::GUEST_USER, Session};
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
    pub fn new_session(&self) -> Arc<Session> {
        if self.internal {
            return INTERNAL_SESSION.clone();
        }

        let session = Session::new(GUEST_USER.clone());
        for perm in &self.permissions {
            session.grant(perm);
        }
        for itag in &self.itags {
            session.grant(format!("itag:{itag}"));
        }
        session
    }
}

pub(crate) struct PluginState {
    pub secret_key: Uuid,
    pub info: PluginInfo,
    pub process: process::Child,
}
