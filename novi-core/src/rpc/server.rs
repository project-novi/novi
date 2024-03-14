use super::{client, Close, Execute, FlattenedObject, IpcSocket};
use crate::{
    anyhow, session::INTERNAL_SESSION, Error, Novi, Object, Result, Session, Tags, TimeRange,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::{DashMap, DashSet};
use interprocess::local_socket::tokio as ipc;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{runtime::Handle, sync::Notify};
use tracing::warn;
use uuid::Uuid;

static PEERS_TERMINATED: Lazy<DashMap<u32, Notify>> = Lazy::new(DashMap::new);

pub async fn wait_terminate(pid: u32) {
    if let Some(notify) = PEERS_TERMINATED.get(&pid) {
        notify.notified().await;
    }
}

pub fn new_socket(novi: Arc<Novi>, stream: ipc::LocalSocketStream) -> Arc<IpcSocket<Command>> {
    let sessions = DashMap::new();
    let internal_session = Uuid::new_v4();
    sessions.insert(internal_session, INTERNAL_SESSION.clone());

    let context = ServerContext {
        novi,
        pid: stream.peer_pid().unwrap(),
        subs: DashSet::new(),
        rpcs: DashSet::new(),
        sessions,
        internal_session,
    };
    IpcSocket::new(stream, context, 128, "server".to_owned())
}

#[derive(Serialize, Deserialize)]
pub struct Command {
    pub session: Option<Uuid>,
    pub command: RawCommand,
}
#[derive(Serialize, Deserialize)]
pub enum RawCommand {
    AddObject(Tags),
    GetObject(Uuid),
    SetObjectTags {
        id: Uuid,
        tags: Tags,
        force_update: bool,
    },
    DeleteObject(Uuid),
    Query {
        filter: String,
        checkpoint: Option<DateTime<Utc>>,
        updated_range: TimeRange,
        created_range: TimeRange,
        order: String,
        limit: Option<u32>,
    },
    Subscribe {
        filter: String,
        callback: u64,
        checkpoint: Option<DateTime<Utc>>,
        with_history: bool,
        exclude_unrelated: bool,
    },
    Unsubscribe(Uuid),
    Call {
        name: String,
        args: String, // json string
        timeout: Option<f64>,
    },
    RegisterRpc {
        name: String,
        callback: u64,
    },
    UnregisterRpc(String),
    GetInternalSession,
    Login {
        name: String,
        password: String,
    },
}

pub struct ServerContext {
    novi: Arc<Novi>,
    pid: u32,
    subs: DashSet<Uuid>,
    rpcs: DashSet<String>,
    sessions: DashMap<Uuid, Arc<Session>>,
    internal_session: Uuid,
}
#[async_trait]
impl Close for ServerContext {
    async fn close(&self) {
        for id in self.subs.iter() {
            if let Err(err) = self.novi.unsubscribe(*id) {
                warn!(?err, "failed to unsubscribe");
            }
        }
        for name in self.rpcs.iter() {
            if let Err(err) = self.novi.unregister_rpc(name.as_str()) {
                warn!(?err, "failed to unregister rpc");
            }
        }
        if let Some((_, notify)) = PEERS_TERMINATED.remove(&self.pid) {
            notify.notify_one();
        }
    }
}

impl RawCommand {
    async fn execute(self, socket: &Arc<IpcSocket<Command>>) -> Result<Vec<u8>> {
        let context = &socket.context;
        let novi = &context.novi;
        fn wrap<T: Serialize>(value: T) -> Vec<u8> {
            postcard::to_allocvec(&value).unwrap()
        }
        Ok(match self {
            RawCommand::AddObject(tags) => {
                wrap(novi.add_object(tags).await.map(FlattenedObject::from)?)
            }
            RawCommand::GetObject(id) => {
                wrap(novi.get_object(id).await.map(FlattenedObject::from)?)
            }
            RawCommand::SetObjectTags {
                id,
                tags,
                force_update,
            } => wrap(
                novi.set_object_tags(id, tags, force_update)
                    .await
                    .map(FlattenedObject::from)?,
            ),
            RawCommand::DeleteObject(id) => wrap(novi.delete_object(id).await?),
            RawCommand::Query {
                filter,
                checkpoint,
                updated_range,
                created_range,
                order,
                limit,
            } => wrap(
                novi.query(
                    filter.parse()?,
                    checkpoint,
                    updated_range,
                    created_range,
                    order.parse()?,
                    limit,
                )
                .await
                .map(|objs| {
                    objs.into_iter()
                        .map(FlattenedObject::from)
                        .collect::<Vec<_>>()
                })?,
            ),
            RawCommand::Subscribe {
                filter,
                callback,
                checkpoint,
                with_history,
                exclude_unrelated,
            } => {
                let socket = Arc::clone(socket);
                let id = novi
                    .subscribe(
                        filter.parse()?,
                        checkpoint,
                        with_history,
                        exclude_unrelated,
                        Box::new(move |object, kind| {
                            if let Err(err) = tokio::task::block_in_place(|| {
                                Handle::current().block_on(async {
                                    socket
                                        .invoke::<(), _>(client::Command::CallSubscribe {
                                            callback,
                                            object: Object::clone(&object).into(),
                                            kind: kind.to_string(),
                                        })
                                        .await
                                })
                            }) {
                                warn!(?err, "failed to call subscribe");
                            }
                        }),
                    )
                    .await?;
                context.subs.insert(id);
                wrap(id)
            }
            RawCommand::Unsubscribe(id) => {
                novi.unsubscribe(id)?;
                socket.context.subs.remove(&id);
                wrap(())
            }
            RawCommand::Call {
                name,
                args,
                timeout,
            } => wrap(
                novi.call(
                    &name,
                    serde_json::from_str(&args).unwrap(),
                    timeout.map(Duration::from_secs_f64),
                )
                .await
                .map(|it| it.to_string())?,
            ),
            RawCommand::RegisterRpc { name, callback } => {
                let socket = Arc::clone(socket);
                novi.register_rpc(
                    &name,
                    Arc::new(move |name, args| {
                        let name = name.to_owned();
                        let socket = Arc::clone(&socket);
                        Box::pin(async move {
                            let args = serde_json::to_string(&args).unwrap();
                            let result: String = socket
                                .invoke(client::Command::CallRpc {
                                    callback,
                                    name: name.to_owned(),
                                    args,
                                })
                                .await?;
                            Ok(serde_json::from_str(&result).unwrap())
                        })
                    }),
                )?;
                context.rpcs.insert(name);
                wrap(())
            }
            RawCommand::UnregisterRpc(name) => {
                novi.unregister_rpc(&name)?;
                context.rpcs.remove(&name);
                wrap(())
            }
            RawCommand::GetInternalSession => wrap(context.internal_session),
            RawCommand::Login { name, password } => {
                let session = novi.login(&name, &password).await?;
                let id = Uuid::new_v4();
                context.sessions.insert(id, session.clone());
                wrap(id)
            }
        })
    }
}

#[async_trait]
impl Execute for Command {
    type Context = ServerContext;
    type Error = Error;

    fn format_error(err: &Self::Error) -> String {
        format!("{err}")
    }

    async fn execute(self, socket: &Arc<IpcSocket<Self>>) -> Result<Vec<u8>, Self::Error> {
        match self.session {
            Some(id) => {
                let session = socket
                    .context
                    .sessions
                    .get(&id)
                    .map(|it| it.clone())
                    .ok_or_else(|| anyhow!("invalid session"))?;

                session.enter(self.command.execute(socket)).await
            }
            None => self.command.execute(socket).await,
        }
    }
}
