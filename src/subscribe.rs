use chrono::{DateTime, Utc};
use futures::stream::FuturesUnordered;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, error};

use crate::{
    bail,
    filter::Filter,
    hook::CoreHookArgs,
    identity::Identity,
    misc::BoxFuture,
    novi::Novi,
    object::Object,
    proto::{reg_core_hook_request::HookPoint, EventKind},
    session::{AccessKind, Session},
    Result,
};

pub type SubscribeCallback =
    Box<dyn for<'a> FnMut(SubscribeArgs<'a>) -> BoxFuture<'a, ()> + Send + Sync>;

pub struct SubscribeArgs<'a> {
    pub object: &'a Object,
    pub kind: EventKind,
    pub session: Option<&'a mut Session>,
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

pub(crate) enum DispatchWorkerCommand {
    Event(Event),
    NewSub(Subscriber),
}

pub(crate) async fn dispatch_worker(novi: Novi, mut rx: mpsc::Receiver<DispatchWorkerCommand>) {
    let mut subscribers = Vec::new();
    while let Some(obj) = rx.recv().await {
        match obj {
            DispatchWorkerCommand::NewSub(sub) => {
                subscribers.push(sub);
            }
            DispatchWorkerCommand::Event(Event {
                kind,
                object,
                deleted_tags,
            }) => {
                let id = object.id;
                debug!(object = %id, ?kind, "dispatch event");
                let mut i = 0;
                while i < subscribers.len() {
                    let sub = &mut subscribers[i];
                    if !sub.alive.load(Ordering::Relaxed) {
                        subscribers.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }

                let mut futs = FuturesUnordered::new();
                for sub in &mut subscribers {
                    if sub.accept_kinds & (1 << kind as u8) == 0
                        || !sub.filter.matches(&object, &deleted_tags)
                        || sub.identity.check_access(&object, AccessKind::View).is_err()
                    {
                        continue;
                    }

                    futs.push(async {
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
                            session: None,
                        })
                        .await;
                    });
                }

                while let Some(_) = futs.next().await {}
            }
        }
    }
}
