use chrono::{DateTime, Utc};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{
    bail,
    filter::Filter,
    hook::HookArgs,
    misc::BoxFuture,
    novi::Novi,
    object::Object,
    proto::{reg_hook_request::HookPoint, EventKind},
    Result,
};

pub type SubscribeCallback =
    Box<dyn for<'a> FnMut(&'a Object, EventKind) -> BoxFuture<'a, ()> + Send + Sync>;

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

pub(crate) enum DispatchWorkerCommand {
    Event {
        kind: EventKind,
        object: Object,
        deleted_tags: HashSet<String>,
    },
    NewSub {
        alive: Arc<AtomicBool>,
        filter: Filter,
        accept_kinds: u8,
        callback: SubscribeCallback,
    },
}

pub(crate) async fn dispatch_worker(novi: Novi, mut rx: mpsc::Receiver<DispatchWorkerCommand>) {
    struct Subscriber {
        alive: Arc<AtomicBool>,
        filter: Filter,
        accept_kinds: u8,
        callback: SubscribeCallback,
    }
    let mut subscribers = Vec::new();
    while let Some(obj) = rx.recv().await {
        match obj {
            DispatchWorkerCommand::NewSub {
                alive,
                filter,
                accept_kinds,
                callback,
            } => {
                subscribers.push(Subscriber {
                    alive,
                    filter,
                    accept_kinds,
                    callback,
                });
            }
            DispatchWorkerCommand::Event {
                kind,
                object,
                deleted_tags,
            } => {
                debug!(object = %object.id, ?kind, "dispatch event");
                let mut i = 0;
                while i < subscribers.len() {
                    let sub = &mut subscribers[i];
                    if !sub.alive.load(Ordering::Relaxed) {
                        subscribers.swap_remove(i);
                        continue;
                    }
                    i += 1;
                    if sub.accept_kinds & (1 << kind as u8) != 0
                        && sub.filter.matches(&object, &deleted_tags)
                    {
                        // Run the BeforeView hooks manually since we're not in a session
                        let hooks = novi.hooks.read().await;
                        for (filter, f) in &hooks[HookPoint::BeforeView as usize] {
                            if !filter.matches(&object, &HashSet::new()) {
                                continue;
                            }

                            let result: Result<()> = async {
                                let edits = f(HookArgs {
                                    object: &object,
                                    old_object: None,
                                    session: None,
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

                        (sub.callback)(&object, kind).await;
                    }
                }
            }
        }
    }
}
