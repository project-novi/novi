use chrono::{DateTime, Utc};
use std::{
    collections::BTreeSet,
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
    hook::CoreHookArgs,
    identity::Identity,
    misc::BoxFuture,
    novi::Novi,
    object::Object,
    proto::{reg_core_hook_request::HookPoint, EventKind, SessionMode},
    session::Session,
    ErrorKind, Result,
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
    pub session: Option<SessionMode>,
    pub latest: bool,
    pub recheck: bool,
}
impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            checkpoint: None,
            accept_kinds: vec![EventKind::Create, EventKind::Update, EventKind::Delete],
            session: None,
            latest: true,
            recheck: true,
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
    pub options: SubscribeOptions,
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
                mut object,
                deleted_tags,
            }) => {
                let id = object.id;
                debug!(object = %id, ?kind, "dispatch event");
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
                        if let Some(mode) = sub.options.session {
                            let Ok(mut session) = novi.guest_session(mode).await else {
                                error!("failed to create session for subscriber");
                                continue;
                            };
                            if sub.options.latest {
                                match session.get_object(id, true).await {
                                    Ok(new_object) => {
                                        object = new_object;
                                        if sub.options.recheck
                                            && !sub.filter.matches(&object, &deleted_tags)
                                        {
                                            continue;
                                        }
                                    }
                                    Err(err) if err.kind == ErrorKind::ObjectNotFound => {
                                        continue;
                                    }
                                    Err(err) => {
                                        error!(?err, "failed to get object");
                                        continue;
                                    }
                                }
                            }
                            if let Err(err) = session.run_before_view(&mut object).await {
                                error!(?err, "failed to return object");
                                continue;
                            }
                            (sub.callback)(SubscribeArgs {
                                object: &object,
                                kind,
                                session: Some(&mut session),
                            })
                            .await;
                        } else {
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
                        }
                    }
                }
            }
        }
    }
}
