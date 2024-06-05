use crate::{anyhow, bail, misc::BoxFuture, rpc::SessionStore, session::Session, Result};
use serde_json::Map;
use std::{fmt, str::FromStr, sync::Arc};
use uuid::Uuid;

pub struct JsonMap(Map<String, serde_json::Value>);
impl JsonMap {
    pub fn new(map: Map<String, serde_json::Value>) -> Self {
        Self(map)
    }

    pub fn get(&self, arg: &str) -> Result<&serde_json::Value> {
        self.0
            .get(arg)
            .ok_or_else(|| anyhow!(@InvalidArgument ("argument" => arg) "missing argument"))
    }

    pub fn get_id(&self, arg: &str) -> Result<Uuid> {
        self.get(arg)?
            .as_str()
            .and_then(|id| Uuid::from_str(id).ok())
            .ok_or_else(|| anyhow!(@InvalidArgument ("argument" => arg) "argument is not a UUID"))
    }

    pub fn get_str(&self, arg: &str) -> Result<&str> {
        self.get(arg)?
            .as_str()
            .ok_or_else(|| anyhow!(@InvalidArgument ("argument" => arg) "argument is not a string"))
    }

    pub fn get_u64(&self, arg: &str) -> Result<u64> {
        self.get(arg)?
            .as_u64()
            .ok_or_else(|| anyhow!(@InvalidArgument ("argument" => arg) "argument is not a u64"))
    }

    pub fn get_bool(&self, arg: &str) -> Result<bool> {
        self.get(arg)?
            .as_bool()
            .ok_or_else(|| anyhow!(@InvalidArgument ("argument" => arg) "argument is not a boolean"))
    }
}
impl fmt::Display for JsonMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&serde_json::to_string(&self.0).unwrap())
    }
}
impl FromIterator<(String, serde_json::Value)> for JsonMap {
    fn from_iter<T: IntoIterator<Item = (String, serde_json::Value)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

pub type Function = Arc<
    dyn for<'a> Fn(
            (&'a mut Session, &'a SessionStore),
            &'a JsonMap,
        ) -> BoxFuture<'a, Result<JsonMap>>
        + Send
        + Sync,
>;

pub fn parse_arguments(json: String) -> Result<JsonMap> {
    serde_json::Value::from_str(&json)
        .ok()
        .and_then(|it| match it {
            serde_json::Value::Object(map) => Some(JsonMap::new(map)),
            _ => None,
        })
        .ok_or_else(|| anyhow!(@InvalidArgument "invalid JSON object"))
}

pub fn parse_json_map(json: String) -> Result<JsonMap> {
    match serde_json::Value::from_str(&json) {
        Ok(serde_json::Value::Object(args)) => Ok(JsonMap::new(args)),
        _ => bail!(@InvalidArgument "expected JSON map"),
    }
}
