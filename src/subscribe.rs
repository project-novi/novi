use chrono::{DateTime, Utc};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::broadcast;
use tracing::error;

use crate::{
    bail,
    filter::Filter,
    hook::CoreHookArgs,
    identity::Identity,
    misc::BoxFuture,
    novi::Novi,
    object::Object,
    proto::{reg_core_hook_request::HookPoint, EventKind},
    session::AccessKind,
    Result,
};

pub type SubscribeCallback =
    Box<dyn for<'a> FnMut(SubscribeArgs<'a>) -> BoxFuture<'a, ()> + Send + Sync>;

pub struct SubscribeArgs<'a> {
    pub object: &'a Object,
    pub kind: EventKind,
}

pub struct SubscribeOptions {
    pub checkpoint: Option<DateTime<Utc>>,
    pub accept_kinds: Vec<EventKind>,
}
impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            checkpoint: None,
            accept_kinds: vec![EventKind::Create, EventKind::Update, EventKind::Delete],
        }
    }
}

#[derive(Clone)]
pub(crate) struct Event {
    pub kind: EventKind,
    pub object: Object,
    pub deleted_tags: BTreeSet<String>,
}

pub(crate) struct Subscriber {
    pub alive: Arc<AtomicBool>,
    pub filter: Filter,
    pub identity: Arc<Identity>,
    pub accept_kinds: u8,
    pub callback: SubscribeCallback,
}

pub async fn subscriber_task(
    novi: Novi,
    mut sub: Subscriber,
    objects: Vec<Object>,
    ckpt: DateTime<Utc>,
    mut rx: broadcast::Receiver<Event>,
) {
    for object in objects {
        let kind = if object.created >= ckpt {
            EventKind::Create
        } else {
            EventKind::Update
        };
        if sub.accept_kinds & (1 << kind as u8) == 0 {
            continue;
        }
        (sub.callback)(SubscribeArgs {
            object: &object,
            kind,
        })
        .await;
    }

    while let Ok(Event {
        kind,
        object,
        deleted_tags,
    }) = rx.recv().await
    {
        if !sub.alive.load(Ordering::Relaxed) {
            break;
        }

        if sub.accept_kinds & (1 << kind as u8) == 0
            || !sub.filter.matches(&object, &deleted_tags)
            || sub
                .identity
                .check_access(&object, AccessKind::View)
                .is_err()
        {
            continue;
        }

        // Run the BeforeView hooks manually since we're not in a session
        let hooks = novi.core_hooks.read().await;
        for (filter, f) in &hooks[HookPoint::BeforeView as usize] {
            if !filter.matches(&object, &Default::default()) {
                continue;
            }

            let result: Result<()> = async {
                let edits = f(CoreHookArgs {
                    object: &object,
                    old_object: None,
                    session: Err(&sub.identity),
                })
                .await?;
                if !edits.is_empty() {
                    bail!(@InvalidArgument "hook must not modify object");
                }
                Ok(())
            }
            .await;
            if let Err(err) = result {
                error!(?err, "failed to run hook");
                continue;
            }
        }
        drop(hooks);

        (sub.callback)(SubscribeArgs {
            object: &object,
            kind,
        })
        .await;
    }
}
