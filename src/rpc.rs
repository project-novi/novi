use slab::Slab;
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::{
    anyhow, bail,
    filter::{Filter, QueryOptions, TimeRange},
    function::{parse_arguments, parse_json_map, JsonMap},
    hook::{CoreHookArgs, HookAction, HookArgs, ObjectEdits},
    identity::IDENTITIES,
    misc::utc_from_timestamp,
    proto::{
        self, query_request::Order, required, tags_from_pb, EventKind, ObjectLock, SessionMode,
    },
    session::{Session, SessionCommand},
    subscribe::SubscribeOptions,
    token::{IdentityToken, SessionToken},
    Error, Novi, Result,
};

pub fn interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    if let Some(identity) = req.metadata().get("identity") {
        let Ok(identity) = identity.to_str() else {
            bail!(@InvalidArgument "invalid identity");
        };
        let token = IdentityToken::from_str(identity)?;
        req.extensions_mut().insert(token);
    }
    if let Some(session) = req.metadata().get("session") {
        let Ok(session) = session.to_str() else {
            bail!(@InvalidArgument "invalid session");
        };
        let token = SessionToken::from_str(session)?;
        req.extensions_mut().insert(token);
    }

    Ok(req)
}

pub(crate) type RpcResult<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct RpcFacade(Novi);
impl RpcFacade {
    pub fn new(novi: Novi) -> Self {
        Self(novi)
    }
}

#[tonic::async_trait]
impl proto::novi_server::Novi for RpcFacade {
    async fn login(&self, req: Request<proto::LoginRequest>) -> RpcResult<proto::LoginReply> {
        let req = req.into_inner();
        let identity = self.0.login(&req.username, &req.password).await?;
        let token = IdentityToken::new();
        identity.save_to_db(&self.0, &token).await?;
        Ok(Response::new(proto::LoginReply {
            identity: token.to_string(),
        }))
    }

    async fn login_as(
        &self,
        req: Request<proto::LoginAsRequest>,
    ) -> RpcResult<proto::LoginAsReply> {
        let (_, ext, req) = req.into_parts();
        if !self.0.extract_identity(&ext).await?.is_admin() {
            bail!(@PermissionDenied "only admin can login as other users");
        }
        let user = self.0.get_user(required(req.user)?.into()).await?;
        let identity = self.0.login_as(user);
        let token = if req.temporary {
            identity.cache_token()
        } else {
            let token = IdentityToken::new();
            identity.save_to_db(&self.0, &token).await?;
            token
        };
        Ok(Response::new(proto::LoginAsReply {
            identity: token.to_string(),
        }))
    }

    async fn use_master_key(
        &self,
        req: Request<proto::UseMasterKeyRequest>,
    ) -> RpcResult<proto::UseMasterKeyReply> {
        if self.0.config.master_key != Some(req.into_inner().key) {
            bail!(@InvalidCredentials "invalid master key");
        }
        let token = IdentityToken::new();
        IDENTITIES.insert(token.clone(), self.0.internal_identity.clone());
        Ok(Response::new(proto::UseMasterKeyReply {
            identity: token.to_string(),
        }))
    }

    type NewSessionStream = ReceiverStream<Result<proto::NewSessionReply, Status>>;

    async fn new_session(
        &self,
        req: Request<proto::NewSessionRequest>,
    ) -> RpcResult<Self::NewSessionStream> {
        let req = req.into_inner();
        let mode = SessionMode::try_from(req.mode)
            .map_err(|_| anyhow!(@InvalidArgument "invalid session mode"))?;
        info!(?mode, "new session");

        let (token, _) = self.0.new_session(mode).await?;

        let (tx, rx) = mpsc::channel::<Result<proto::NewSessionReply, Status>>(1);
        tx.send(Ok(proto::NewSessionReply {
            token: token.to_string(),
        }))
        .await
        .unwrap();

        // spawns a task to end the session if the client disconnects
        tokio::spawn({
            let novi = self.0.clone();
            let token = token.clone();
            async move {
                tx.closed().await;
                warn!("client disconnected, ending session");
                novi.session_store.remove(&token);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn end_session(
        &self,
        req: Request<proto::EndSessionRequest>,
    ) -> RpcResult<proto::EndSessionReply> {
        let (_, mut ext, req) = req.into_parts();
        let Some(token) = ext.remove::<SessionToken>() else {
            bail!(@InvalidCredentials "unauthenticated");
        };
        let Some((_, tx)) = self.0.session_store.remove(&token) else {
            bail!(@InvalidCredentials "invalid session");
        };
        let (end_tx, end_rx) = oneshot::channel::<()>();
        tx.send(SessionCommand::End {
            commit: req.commit,
            notify: Some(end_tx),
        })
        .await
        .map_err(|_| anyhow!("failed to end session"))?;
        end_rx.await.map_err(|_| anyhow!("failed to end session"))?;
        info!(%token, "end session");
        Ok(Response::new(proto::EndSessionReply {}))
    }

    async fn create_object(
        &self,
        req: Request<proto::CreateObjectRequest>,
    ) -> RpcResult<proto::CreateObjectReply> {
        let (_, ext, req) = req.into_parts();
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let object = session
                        .create_object(tags_from_pb(required(req.tags)?))
                        .await?;
                    Ok(proto::CreateObjectReply {
                        object: Some(object.into()),
                    })
                })
            })
            .await
    }

    async fn get_object(
        &self,
        req: Request<proto::GetObjectRequest>,
    ) -> RpcResult<proto::GetObjectReply> {
        let (_, ext, req) = req.into_parts();
        let lock = ObjectLock::try_from(req.lock).unwrap_or(ObjectLock::LockShare);
        let precondition: Option<Filter> = req.precondition.map(|it| it.parse()).transpose()?;
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    info!(token = %session.token, ?lock, "pre get object");
                    let object = session.get_object(required(req.id)?.into(), lock).await?;
                    info!(token = %session.token, id = %object.id, ?lock, "get object");
                    if let Some(precondition) = precondition {
                        if !precondition.matches(&object, &Default::default()) {
                            bail!(@PreconditionFailed);
                        }
                    }
                    Ok(proto::GetObjectReply {
                        object: Some(object.into()),
                    })
                })
            })
            .await
    }

    async fn update_object(
        &self,
        req: Request<proto::UpdateObjectRequest>,
    ) -> RpcResult<proto::UpdateObjectReply> {
        let (_, ext, req) = req.into_parts();
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let object = session
                        .update_object(
                            required(req.id)?.into(),
                            tags_from_pb(required(req.tags)?),
                            req.force,
                        )
                        .await?;
                    Ok(proto::UpdateObjectReply {
                        object: Some(object.into()),
                    })
                })
            })
            .await
    }

    async fn replace_object(
        &self,
        req: Request<proto::ReplaceObjectRequest>,
    ) -> RpcResult<proto::ReplaceObjectReply> {
        let (_, ext, req) = req.into_parts();
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let object = session
                        .replace_object(
                            required(req.id)?.into(),
                            tags_from_pb(required(req.tags)?),
                            req.scopes.map(|it| it.scopes.into_iter().collect()),
                            req.force,
                        )
                        .await?;
                    Ok(proto::ReplaceObjectReply {
                        object: Some(object.into()),
                    })
                })
            })
            .await
    }

    async fn delete_object_tags(
        &self,
        req: Request<proto::DeleteObjectTagsRequest>,
    ) -> RpcResult<proto::DeleteObjectTagsReply> {
        let (_, ext, req) = req.into_parts();
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let object = session
                        .delete_object_tags(required(req.id)?.into(), req.tags)
                        .await?;
                    Ok(proto::DeleteObjectTagsReply {
                        object: Some(object.into()),
                    })
                })
            })
            .await
    }

    async fn delete_object(
        &self,
        req: Request<proto::DeleteObjectRequest>,
    ) -> RpcResult<proto::DeleteObjectReply> {
        let (_, ext, req) = req.into_parts();
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    session.delete_object(required(req.id)?.into()).await?;
                    Ok(proto::DeleteObjectReply {})
                })
            })
            .await
    }

    async fn query(&self, req: Request<proto::QueryRequest>) -> RpcResult<proto::QueryReply> {
        let (_, ext, req) = req.into_parts();
        let lock = ObjectLock::try_from(req.lock).unwrap_or(ObjectLock::LockShare);
        let filter: Filter = req.filter.parse()?;
        fn time_range(after: Option<i64>, before: Option<i64>) -> Result<TimeRange> {
            let after = after.map(utc_from_timestamp).transpose()?;
            let before = before.map(utc_from_timestamp).transpose()?;
            Ok((after, before))
        }
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let objects = session
                        .query(
                            filter,
                            QueryOptions {
                                checkpoint: req.checkpoint.map(utc_from_timestamp).transpose()?,
                                created_range: time_range(req.created_after, req.created_before)?,
                                updated_range: time_range(req.updated_after, req.updated_before)?,
                                order: Order::try_from(req.order)
                                    .map_err(|_| anyhow!(@InvalidArgument "invalid order"))?,
                                limit: req.limit,
                                lock,
                            },
                        )
                        .await?;
                    Ok(proto::QueryReply {
                        objects: objects.into_iter().map(Into::into).collect(),
                    })
                })
            })
            .await
    }

    type SubscribeStream = ReceiverStream<Result<proto::SubscribeReply, Status>>;

    async fn subscribe(
        &self,
        req: Request<proto::SubscribeRequest>,
    ) -> RpcResult<Self::SubscribeStream> {
        let (_, ext, req) = req.into_parts();
        let (tx, rx) = mpsc::channel::<Result<proto::SubscribeReply, Status>>(8);

        let filter: Filter = req.filter.parse()?;
        let checkpoint = req.checkpoint.map(utc_from_timestamp).transpose()?;
        let accept_kinds = req
            .accept_kinds
            .into_iter()
            .filter_map(|it| EventKind::try_from(it).ok())
            .collect();
        let options = SubscribeOptions {
            checkpoint,
            accept_kinds,
        };

        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let alive = Arc::new(AtomicBool::new(true));
                    session
                        .subscribe(
                            filter,
                            options,
                            alive.clone(),
                            Box::new(move |args| {
                                let tx = tx.clone();
                                let alive = alive.clone();
                                Box::pin(async move {
                                    let pb = proto::SubscribeReply {
                                        object: Some(args.object.clone().into()),
                                        kind: args.kind.into(),
                                    };
                                    if tx.send(Ok(pb)).await.is_err() {
                                        debug!("subscriber disconnected");
                                        alive.store(false, Ordering::Relaxed);
                                    }
                                })
                            }),
                        )
                        .await
                })
            })
            .await?;

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // TODO: reduce duplication (register_core_hook and register_hook)

    type RegisterCoreHookStream = ReceiverStream<Result<proto::RegCoreHookReply, Status>>;

    async fn register_core_hook(
        &self,
        req: Request<Streaming<proto::RegCoreHookRequest>>,
    ) -> RpcResult<Self::RegisterCoreHookStream> {
        use proto::{reg_core_hook_request::*, RegCoreHookRequest as Req};

        let (_, ext, mut req) = req.into_parts();
        self.0
            .extract_identity(&ext)
            .await?
            .check_perm("core_hook.register")?;

        let Some(Ok(Req {
            message: Some(Message::Initiate(init)),
        })) = req.next().await
        else {
            bail!(@InvalidArgument "expected initiate");
        };
        let Ok(point) = HookPoint::try_from(init.point) else {
            bail!(@InvalidArgument "invalid hook point");
        };
        let filter: Filter = init.filter.parse()?;

        info!(?point, "register core hook");

        let (stream_tx, stream_rx) = mpsc::channel::<Result<proto::RegCoreHookReply, Status>>(8);
        let (call_tx, mut call_rx) = mpsc::channel::<(
            proto::RegCoreHookReply,
            oneshot::Sender<Result<ObjectEdits>>,
        )>(32);
        let removed = Arc::new(AtomicBool::default());
        tokio::spawn({
            let stream_tx = stream_tx.clone();
            let removed = removed.clone();
            async move {
                let mut result_txs = Slab::new();
                loop {
                    tokio::select! {
                        call = call_rx.recv() => {
                            let Some((mut resp, tx)) = call else {
                                break;
                            };
                            resp.call_id = result_txs.insert(tx) as u64;
                            if stream_tx.send(Ok(resp)).await.is_err() {
                                break;
                            }
                        }
                        reply = req.next() => {
                            let Some(Ok(reply)) = reply else {
                                break;
                            };
                            let Req { message: Some(Message::Result(result)) } = reply else {
                                warn!("invalid reply from client");
                                continue;
                            };
                            if let Some(tx) = result_txs.try_remove(result.call_id as usize) {
                                let result = match result.result {
                                    Some(call_result::Result::Response(resp)) => ObjectEdits::from_pb(resp),
                                    Some(call_result::Result::Error(err)) => Err(Error::from_pb(err)),
                                    _ => Err(anyhow!(@InvalidArgument "invalid response from client")),
                                };
                                let _ = tx.send(result);
                            } else {
                                warn!(call_id = result.call_id, "invalid call id from core hook client");
                            }
                        }
                        else => break,
                    }
                }
                warn!("core hook client disconnected");
                removed.store(true, Ordering::Relaxed);
            }
        });

        self.0
            .register_core_hook(
                point,
                filter,
                Box::new(move |args: CoreHookArgs| {
                    if removed.load(Ordering::Relaxed) {
                        return Box::pin(async move { Ok(ObjectEdits::default()) });
                    }
                    let removed = removed.clone();
                    let call_tx = call_tx.clone();
                    let pb = args.to_pb();
                    let fut = async move {
                        let (result_tx, result_rx) = oneshot::channel::<Result<ObjectEdits>>();
                        if call_tx.send((pb, result_tx)).await.is_err() {
                            warn!("core hook client disconnected, removing hook");
                            removed.store(true, Ordering::Relaxed);
                            return Ok(ObjectEdits::default());
                        }
                        result_rx
                            .await
                            .map_err(|_| anyhow!(@IOError "core hook client disconnected"))?
                    };
                    if let Ok(session) = args.session {
                        Box::pin(session.yield_self(fut))
                    } else {
                        Box::pin(fut)
                    }
                }),
            )
            .await;

        // Notify the client that the hook has been registered
        let _ = stream_tx
            .send(Ok(proto::RegCoreHookReply {
                call_id: 0,
                object: None,
                old_object: None,
                session: None,
                identity: String::new(),
            }))
            .await;

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    type RegisterHookStream = ReceiverStream<Result<proto::RegHookReply, Status>>;

    async fn register_hook(
        &self,
        req: Request<Streaming<proto::RegHookRequest>>,
    ) -> RpcResult<Self::RegisterHookStream> {
        use proto::{reg_hook_request::*, RegHookRequest as Req};

        let (_, ext, mut req) = req.into_parts();

        let Some(Ok(Req {
            message: Some(Message::Initiate(init)),
        })) = req.next().await
        else {
            bail!(@InvalidArgument "expected initiate");
        };
        let function = init.function;
        let before = init.before;

        self.0
            .extract_identity(&ext)
            .await?
            .check_perm(&format!("hook.register:{function}"))?;

        info!(function, before, "register hook");

        let (stream_tx, stream_rx) = mpsc::channel::<Result<proto::RegHookReply, Status>>(8);
        let (call_tx, mut call_rx) =
            mpsc::channel::<(proto::RegHookReply, oneshot::Sender<Result<HookAction>>)>(32);
        let removed = Arc::new(AtomicBool::default());
        tokio::spawn({
            let stream_tx = stream_tx.clone();
            let removed = removed.clone();
            async move {
                let mut result_txs = Slab::new();
                loop {
                    tokio::select! {
                        call = call_rx.recv() => {
                            let Some((mut resp, tx)) = call else {
                                break;
                            };
                            resp.call_id = result_txs.insert(tx) as u64;
                            if stream_tx.send(Ok(resp)).await.is_err() {
                                break;
                            }
                        }
                        reply = req.next() => {
                            let Some(Ok(reply)) = reply else {
                                break;
                            };
                            let Req { message: Some(Message::Result(result)) } = reply else {
                                warn!("invalid reply from client");
                                continue;
                            };
                            if let Some(tx) = result_txs.try_remove(result.call_id as usize) {
                                let result = match result.result {
                                    Some(call_result::Result::Response(resp)) => HookAction::from_pb(resp),
                                    Some(call_result::Result::Error(err)) => Err(Error::from_pb(err)),
                                    _ => Err(anyhow!(@InvalidArgument "invalid response from client")),
                                };
                                let _ = tx.send(result);
                            } else {
                                warn!(call_id = result.call_id, "invalid call id from hook client");
                            }
                        }
                        else => break,
                    }
                }
                warn!("core hook client disconnected");
                removed.store(true, Ordering::Relaxed);
            }
        });

        self.0
            .register_hook(
                &function,
                before,
                Arc::new(move |args: HookArgs| {
                    if removed.load(Ordering::Relaxed) {
                        return Box::pin(async move { Ok(HookAction::default()) });
                    }
                    let removed = removed.clone();
                    let call_tx = call_tx.clone();
                    let pb = args.to_pb();
                    let fut = async move {
                        let (result_tx, result_rx) = oneshot::channel::<Result<HookAction>>();
                        if call_tx.send((pb, result_tx)).await.is_err() {
                            warn!("hook client disconnected, removing hook");
                            removed.store(true, Ordering::Relaxed);
                            return Ok(HookAction::default());
                        }
                        result_rx
                            .await
                            .map_err(|_| anyhow!(@IOError "hook client disconnected"))?
                    };
                    Box::pin(args.session.yield_self(fut))
                }),
            )
            .await?;

        // Notify the client that the hook has been registered
        let _ = stream_tx
            .send(Ok(proto::RegHookReply {
                call_id: 0,
                arguments: String::new(),
                original_result: None,
                session: String::new(),
                identity: String::new(),
            }))
            .await;

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    type RegisterFunctionStream = ReceiverStream<Result<proto::RegFunctionReply, Status>>;

    async fn register_function(
        &self,
        req: Request<Streaming<proto::RegFunctionRequest>>,
    ) -> RpcResult<Self::RegisterFunctionStream> {
        use proto::{reg_function_request::*, RegFunctionRequest as Req};

        let (_, ext, mut req) = req.into_parts();
        let Some(Ok(Req {
            message: Some(Message::Initiate(init)),
        })) = req.next().await
        else {
            bail!(@InvalidArgument "expected initiate");
        };
        let name = init.name;
        self.0
            .extract_identity(&ext)
            .await?
            .check_perm(&format!("function.register:{name}"))?;
        info!(name, hookable = init.hookable, "register function");

        let (stream_tx, stream_rx) = mpsc::channel::<Result<proto::RegFunctionReply, Status>>(8);
        let (call_tx, mut call_rx) =
            mpsc::channel::<(proto::RegFunctionReply, oneshot::Sender<Result<JsonMap>>)>(32);
        tokio::spawn({
            let stream_tx = stream_tx.clone();
            let novi = self.0.clone();
            let name = name.clone();
            async move {
                let mut result_txs = Slab::new();
                loop {
                    tokio::select! {
                        call = call_rx.recv() => {
                            let Some((mut resp, tx)) = call else {
                                break;
                            };
                            resp.call_id = result_txs.insert(tx) as u64;
                            if stream_tx.send(Ok(resp)).await.is_err() {
                                break;
                            }
                        }
                        reply = req.next() => {
                            let Some(Ok(reply)) = reply else {
                                break;
                            };
                            let Req { message: Some(Message::Result(result)) } = reply else {
                                warn!("invalid reply from client");
                                continue;
                            };
                            if let Some(tx) = result_txs.try_remove(result.call_id as usize) {
                                let result = match result.result {
                                    Some(call_result::Result::Response(resp)) => Ok(resp),
                                    Some(call_result::Result::Error(err)) => Err(Error::from_pb(err)),
                                    _ => Err(anyhow!(@InvalidArgument "invalid response from client")),
                                };
                                let result = result.and_then(parse_json_map);
                                let _ = tx.send(result);
                            } else {
                                warn!(call_id = result.call_id, "invalid call id from hook client");
                            }
                        }
                        else => break,
                    }
                }
                warn!(name, "function provider disconnected, removing function");
                novi.functions.remove(&name);
            }
        });

        self.0
            .register_function(
                name,
                Arc::new(move |session: &mut Session, arguments: &JsonMap| {
                    let token = session.token().to_string();
                    let identity = session.identity.clone();
                    let call_tx = call_tx.clone();
                    Box::pin(session.yield_self(async move {
                        let (result_tx, result_rx) = oneshot::channel::<Result<JsonMap>>();
                        let reply = proto::RegFunctionReply {
                            call_id: 0,
                            arguments: arguments.to_string(),
                            session: token,
                            identity: identity.cache_token().to_string(),
                        };
                        if call_tx.send((reply, result_tx)).await.is_err() {
                            bail!(@IOError "function provider disconnected");
                        }
                        result_rx
                            .await
                            .map_err(|_| anyhow!(@IOError "function provider disconnected"))?
                    }))
                }),
                init.hookable,
            )
            .await?;

        // Notify the client that the hook has been registered
        let _ = stream_tx
            .send(Ok(proto::RegFunctionReply {
                call_id: 0,
                arguments: String::new(),
                session: String::new(),
                identity: String::new(),
            }))
            .await;

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    async fn call_function(
        &self,
        req: Request<proto::CallFunctionRequest>,
    ) -> RpcResult<proto::CallFunctionReply> {
        let (_, ext, req) = req.into_parts();
        self.0
            .submit(ext, move |session| {
                Box::pin(async move {
                    let result = session
                        .call_function(&req.name, &parse_arguments(req.arguments)?)
                        .await?;
                    Ok(proto::CallFunctionReply {
                        result: result.to_string(),
                    })
                })
            })
            .await
    }

    async fn has_permission(
        &self,
        req: Request<proto::HasPermissionRequest>,
    ) -> RpcResult<proto::HasPermissionReply> {
        let (_, ext, req) = req.into_parts();
        let identity = self.0.extract_identity(&ext).await?;
        Ok(Response::new(proto::HasPermissionReply {
            ok: req.permissions.iter().all(|perm| identity.has_perm(perm)),
        }))
    }
}
