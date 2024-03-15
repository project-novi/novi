use crate::{user::GUEST_USER, Session};
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
    pub perms: Vec<String>,

    #[serde(default)]
    pub disabled: bool,
}
impl PluginInfo {
    pub fn new_session(&self) -> Arc<Session> {
        let session = Session::new(GUEST_USER.clone());
        session.grant("rpc.register");
        session.grant("subscribe");
        for perm in &self.perms {
            session.grant(perm);
        }
        session
    }
}

pub(crate) struct PluginState {
    pub secret_key: Uuid,
    pub info: PluginInfo,
    pub process: process::Child,
}
