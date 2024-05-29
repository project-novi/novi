use std::str::FromStr;
use crate::{anyhow, misc::BoxFuture, rpc::SessionStore, session::Session, Result};
use serde_json::Map;

pub type Arguments = Map<String, serde_json::Value>;

pub type Function = Box<
    dyn for<'a> Fn(
            (&'a mut Session, SessionStore),
            Arguments,
        ) -> BoxFuture<'a, Result<serde_json::Value>>
        + Send
        + Sync,
>;

pub fn parse_arguments(json: String) -> Result<Arguments> {
    serde_json::Value::from_str(&json)
        .ok()
        .and_then(|it| match it {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        })
        .ok_or_else(|| anyhow!(@InvalidArgument "invalid JSON object"))
}

pub fn parse_json(json: String) -> Result<serde_json::Value> {
    serde_json::Value::from_str(&json)
        .map_err(|_| anyhow!(@InvalidArgument "invalid JSON object"))
}
