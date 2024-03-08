use crate::{Object, Result, RpcArgs};
use serde::Serialize;
use std::{fmt::Display, future::Future, pin::Pin, sync::Arc};

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Created,
    Updated,
    Deleted,
}
impl Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EventKind::Created => "created",
            EventKind::Updated => "updated",
            EventKind::Deleted => "deleted",
        };
        write!(f, "{s}")
    }
}

pub type Subscriber = Box<dyn FnMut(&Arc<Object>, EventKind) + Send + Sync>;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type RpcProvider =
    Arc<dyn Fn(&str, RpcArgs) -> BoxFuture<Result<serde_json::Value>> + Send + Sync>;
