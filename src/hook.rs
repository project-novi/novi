use chrono::{DateTime, Utc};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    bail,
    function::{parse_json, Arguments},
    misc::BoxFuture,
    object::Object,
    proto::{self, required, tags_from_pb},
    rpc::SessionStore,
    session::Session,
    tag::{TagValue, Tags},
    Result,
};

pub const HOOK_POINT_COUNT: usize = 7;

pub type CoreHookCallback =
    Box<dyn for<'a> Fn(CoreHookArgs<'a>) -> BoxFuture<'a, Result<ObjectEdits>> + Send + Sync>;

pub type HookCallback =
    Arc<dyn for<'a> Fn(HookArgs<'a>) -> BoxFuture<'a, Result<HookAction>> + Send + Sync>;

#[derive(Default)]
pub enum HookAction {
    #[default]
    None,
    UpdateResult(serde_json::Value),
    UpdateArgs(Arguments),
}
impl HookAction {
    pub fn from_pb(pb: proto::HookAction) -> Result<Self> {
        if let Some(result_or_args) = pb.result_or_args {
            let result_or_args = parse_json(result_or_args)?;
            if pb.update_args {
                match result_or_args {
                    serde_json::Value::Object(args) => Ok(Self::UpdateArgs(args)),
                    _ => bail!(@InvalidArgument "invalid update_args value"),
                }
            } else {
                Ok(Self::UpdateResult(result_or_args))
            }
        } else {
            Ok(Self::None)
        }
    }
}

#[non_exhaustive]
pub struct CoreHookArgs<'a> {
    pub object: &'a Object,
    pub old_object: Option<&'a Object>,

    // This can only be None if called from non-session context. The only
    // non-session context for now should be subscriber callback (BeforeView),
    // so it's safe to assume the presence of session for non-BeforeView hooks.
    pub session: Option<(&'a mut Session, &'a SessionStore)>,
}
impl<'a> CoreHookArgs<'a> {
    pub fn to_pb(&self) -> proto::RegCoreHookReply {
        // TODO: Transmiting session should be optional in order to save
        // bandwidth
        proto::RegCoreHookReply {
            call_id: 0,
            object: Some(self.object.clone().into()),
            old_object: self.old_object.cloned().map(Into::into),
            session: self.session.as_ref().map(|it| it.0.token().to_string()),
        }
    }
}

pub struct HookArgs<'a> {
    pub arguments: &'a Arguments,
    // None if the hook is a before-hook
    pub original_result: Option<&'a serde_json::Value>,
    pub session: (&'a mut Session, &'a SessionStore),
}
impl<'a> HookArgs<'a> {
    pub fn to_pb(&self) -> proto::RegHookReply {
        proto::RegHookReply {
            call_id: 0,
            arguments: serde_json::to_string(&self.arguments).unwrap(),
            original_result: self.original_result.as_ref().map(|it| it.to_string()),
            session: self.session.0.token().to_string(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ObjectEdits {
    pub deletes: HashSet<String>,
    pub update: Tags,
    pub clear: bool,
}
impl ObjectEdits {
    pub fn from_pb(pb: proto::ObjectEdits) -> Result<Self> {
        Ok(Self {
            deletes: pb.deletes.into_iter().collect(),
            update: tags_from_pb(required(pb.update)?),
            clear: pb.clear,
        })
    }

    pub fn new() -> Self {
        Self {
            deletes: HashSet::new(),
            update: HashMap::new(),
            clear: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.update.is_empty() && !self.clear
    }

    pub fn set(&mut self, tag: String, value: Option<String>) {
        self.deletes.remove(&tag);
        self.update.insert(tag, value);
    }

    pub fn delete(&mut self, tag: String) {
        self.update.remove(&tag);
        if !self.clear {
            self.deletes.insert(tag);
        }
    }

    pub fn clear(&mut self) {
        self.deletes.clear();
        self.update.clear();
        self.clear = true;
    }

    pub fn extend(&mut self, other: ObjectEdits) {
        if other.clear {
            self.clear();
        }
        for tag in other.deletes {
            self.delete(tag);
        }
        self.update.extend(other.update);
    }

    pub fn apply(self, object: &mut Object, time: DateTime<Utc>) {
        if self.clear {
            object.tags.clear();
        }
        for tag in self.deletes {
            object.tags.remove(&tag);
        }
        object.tags.extend(
            self.update
                .into_iter()
                .map(|(k, v)| (k, TagValue::new(v, time))),
        );
    }
}
