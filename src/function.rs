use crate::{anyhow, bail, misc::BoxFuture, proto::ObjectLock, session::Session, Result};
use serde_json::{Map, Value};
use std::{fmt, str::FromStr, sync::Arc};
use uuid::Uuid;

#[derive(Default, Debug)]
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

    pub fn get_opt(&self, arg: &str) -> Option<&serde_json::Value> {
        self.0.get(arg)
    }

    pub fn map<'a, T>(
        &'a self,
        arg: &str,
        f: impl FnOnce(&'a serde_json::Value) -> Option<T>,
        what: &'static str,
    ) -> Result<T> {
        f(self.get(arg)?)
            .ok_or_else(|| anyhow!(@InvalidArgument ("argument" => arg) "argument is not {what}"))
    }

    pub fn map_opt<'a, T>(
        &'a self,
        arg: &str,
        f: impl FnOnce(&'a serde_json::Value) -> Option<T>,
        what: &'static str,
    ) -> Result<Option<T>> {
        self.get_opt(arg)
            .map(|it| {
                f(it).ok_or_else(
                    || anyhow!(@InvalidArgument ("argument" => arg) "argument is not {what}"),
                )
            })
            .transpose()
    }

    pub fn get_id(&self, arg: &str) -> Result<Uuid> {
        self.map(
            arg,
            |it| it.as_str().and_then(|id| Uuid::from_str(id).ok()),
            "a UUID",
        )
    }

    pub fn get_str(&self, arg: &str) -> Result<&str> {
        self.map(arg, Value::as_str, "a string")
    }
    pub fn get_str_opt(&self, arg: &str) -> Result<Option<&str>> {
        self.map_opt(arg, Value::as_str, "a string")
    }

    pub fn get_u64_opt(&self, arg: &str) -> Result<Option<u64>> {
        self.map_opt(arg, Value::as_u64, "a u64")
    }

    pub fn get_bool_opt(&self, arg: &str) -> Result<Option<bool>> {
        self.map_opt(arg, Value::as_bool, "a boolean")
    }

    pub fn get_lock_opt(&self, arg: &str) -> Result<Option<ObjectLock>> {
        self.map_opt(
            arg,
            |it| {
                it.as_str().and_then(|it| match it {
                    "none" => Some(ObjectLock::LockNone),
                    "exclusive" => Some(ObjectLock::LockExclusive),
                    "share" => Some(ObjectLock::LockShare),
                    _ => None,
                })
            },
            "a valid lock mode",
        )
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
    dyn for<'a> Fn(&'a mut Session, &'a JsonMap) -> BoxFuture<'a, Result<JsonMap>> + Send + Sync,
>;

pub fn parse_json_map(json: String) -> Result<JsonMap> {
    match serde_json::Value::from_str(&json) {
        Ok(serde_json::Value::Object(args)) => Ok(JsonMap::new(args)),
        _ => bail!(@InvalidArgument "expected JSON map"),
    }
}
