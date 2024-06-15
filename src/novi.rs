use arc_swap::ArcSwap;
use chrono::Utc;
use dashmap::DashMap;
use deadpool::managed::Pool;
use redis::AsyncCommands;
use std::{collections::HashSet, ops::Deref, str::FromStr, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task::JoinHandle,
};
use tokio_postgres::{types::Type, NoTls};
use tracing::warn;
use uuid::Uuid;

use crate::{
    anyhow, bail,
    filter::Filter,
    function::Function,
    hook::{CoreHookCallback, HookAction, HookArgs, HookCallback, HOOK_POINT_COUNT},
    identity::Identity,
    misc::BoxFuture,
    plugins,
    proto::{reg_core_hook_request::HookPoint, SessionMode},
    rpc::RpcResult,
    session::{Session, SessionCommand, SessionStore},
    subscribe::{DispatchWorkerCommand, Event},
    token::{IdentityToken, SessionToken},
    user::{User, UserRef, INTERNAL_USER},
    Config, Result,
};

pub(crate) struct FunctionRegistry {
    pub function: Function,
    pub hookable: bool,
}

pub struct Inner {
    pub config: Config,
    pub session_store: SessionStore,
    pub core_hooks: RwLock<[Vec<(Filter, CoreHookCallback)>; HOOK_POINT_COUNT]>,
    pub functions: DashMap<String, FunctionRegistry>,
    pub dispatch_tx: mpsc::Sender<DispatchWorkerCommand>,
    pub guest_user: Arc<User>,
    pub guest_identity: Arc<Identity>,
    pub internal_identity: Arc<Identity>,

    pub pg_pool: Pool<deadpool_postgres::Manager>,
    pub redis_pool: Pool<deadpool_redis::Manager, deadpool_redis::Connection>,
}
impl Inner {
    pub async fn register_core_hook(&self, point: HookPoint, filter: Filter, f: CoreHookCallback) {
        self.core_hooks.write().await[point as usize].push((filter, f));
    }

    pub async fn register_hook(&self, function: &str, before: bool, f: HookCallback) -> Result<()> {
        let mut cant_hook = false;
        let mut success = false;
        self.functions.alter(function, |_, reg| {
            if !reg.hookable {
                cant_hook = true;
                return reg;
            }
            success = true;
            let orig_f = reg.function;
            FunctionRegistry {
                function: if before {
                    Arc::new(move |session, args| {
                        let orig_f = Arc::clone(&orig_f);
                        let f = Arc::clone(&f);
                        Box::pin(async move {
                            let action = f(HookArgs {
                                arguments: args,
                                original_result: None,
                                session: session,
                            })
                            .await?;
                            match action {
                                HookAction::None => orig_f(session, args).await,
                                HookAction::UpdateResult(result) => Ok(result),
                                HookAction::UpdateArgs(new_args) => {
                                    orig_f(session, &new_args).await
                                }
                            }
                        })
                    })
                } else {
                    Arc::new(move |session, args| {
                        let orig_f = Arc::clone(&orig_f);
                        let f = Arc::clone(&f);
                        Box::pin(async move {
                            let result = orig_f(session, args).await?;
                            let action = f(HookArgs {
                                arguments: args,
                                original_result: Some(&result),
                                session: session,
                            })
                            .await?;
                            match action {
                                HookAction::None => Ok(result),
                                HookAction::UpdateResult(new_result) => Ok(new_result),
                                HookAction::UpdateArgs(_) => {
                                    bail!(@InvalidArgument "UpdateArgs is not allowed in after-hook")
                                }
                            }
                        })
                    })
                },
                hookable: true,
            }
        });

        if cant_hook {
            bail!(@PermissionDenied "Function is not hookable. Try hooking `func.impl` instead");
        }
        if !success {
            bail!(@FunctionNotFound "function not found");
        }

        Ok(())
    }

    pub async fn register_function(
        &self,
        name: String,
        function: Function,
        hookable: bool,
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;
        match self.functions.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(FunctionRegistry { function, hookable });
            }
            Entry::Occupied(_) => {
                bail!(@InvalidArgument "function already exists");
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Novi(Arc<Inner>);
impl Novi {
    pub async fn new(mut config: Config) -> Result<Self> {
        config.validate()?;

        let pg_pool = config
            .database
            .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)?;
        let redis_pool = config
            .redis
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

        let guest_user = Arc::new(User {
            id: None,
            name: "guest".to_owned(),
            password: None,

            roles: HashSet::new(),
            perms: config.guest_permissions.iter().cloned().collect(),
        });
        let guest_identity = Arc::new(Identity::new_user(
            UserRef::new(guest_user.clone().into()),
            guest_user.clone(),
            None,
        ));
        let internal_identity = Arc::new(Identity::new_user(
            INTERNAL_USER.clone(),
            guest_user.clone(),
            None,
        ));

        let (dispatch_tx, dispatch_rx) = mpsc::channel(1024);
        let inner = Arc::new(Inner {
            config,
            session_store: SessionStore::default(),
            core_hooks: Default::default(),
            functions: DashMap::new(),
            dispatch_tx,
            guest_user,
            guest_identity,
            internal_identity,

            pg_pool,
            redis_pool,
        });
        let result = Self(inner);

        tokio::spawn(crate::subscribe::dispatch_worker(
            result.clone(),
            dispatch_rx,
        ));

        plugins::files::init(&result).await?;
        plugins::group::init(&result).await?;
        plugins::implies::init(&result).await?;
        plugins::users::init(&result).await?;

        Ok(result)
    }

    pub async fn extract_identity(&self, ext: &tonic::Extensions) -> Result<Arc<Identity>> {
        match ext.get::<IdentityToken>() {
            Some(identity) => self.identity_from_token(identity).await,
            None => Ok(self.guest_identity.clone()),
        }
    }

    pub(crate) async fn submit<R: Send + 'static>(
        &self,
        mut ext: tonic::Extensions,
        action: impl for<'a> FnOnce(&'a mut Session) -> BoxFuture<'a, Result<R>> + Send + 'static,
    ) -> RpcResult<R> {
        let identity = self.extract_identity(&ext).await?;
        let (token, handle) = match ext.remove::<SessionToken>() {
            Some(token) => (token, None),
            None => {
                // run in a temporary new session
                // TODO: Is it possible that the this break the consistency?
                let (token, handle) = self.new_session(SessionMode::Auto).await?;
                (token, Some(handle))
            }
        };
        let (tx, rx) = oneshot::channel();
        let Some(sender) = self.session_store.get(&token).map(|it| it.clone()) else {
            bail!(@InvalidCredentials "invalid session");
        };
        let result = sender
            .send(SessionCommand::Action(Box::new(move |session| {
                session.identity = identity;
                let fut = action(session);
                Box::pin(async move {
                    let _ = tx.send(fut.await);
                })
            })))
            .await;

        if result.is_err() {
            bail!(@IOError "failed to submit");
        }
        let result = match rx.await {
            Ok(result) => {
                if handle.is_some() {
                    // release temporary session
                    let _ = sender.send(SessionCommand::End { commit: result.is_ok() }).await;
                }
                tonic::Response::new(result?)
            }
            Err(_) => bail!(@IOError "session closed"),
        };

        if let Some(handle) = handle {
            // wait for the temporary session to terminate
            let _ = handle.await;
        }
        Ok(result)
    }

    pub(crate) async fn guest_session(&self, mode: SessionMode) -> Result<Session> {
        Session::new(self.clone(), self.guest_identity.clone(), mode).await
    }

    pub(crate) async fn internal_session(&self, mode: SessionMode) -> Result<Session> {
        Session::new(self.clone(), self.internal_identity.clone(), mode).await
    }

    pub async fn new_session(&self, mode: SessionMode) -> Result<(SessionToken, JoinHandle<()>)> {
        let mut session = self.guest_session(mode).await?;
        let (tx, mut rx) = mpsc::channel::<SessionCommand>(8);
        let token = session.token().clone();
        self.session_store.insert(token.clone(), tx);
        let handle = tokio::spawn(async move {
            while let Some(action) = rx.recv().await {
                match action {
                    SessionCommand::Action(action) => action(&mut session).await,
                    SessionCommand::End { commit } => {
                        if let Err(err) = session.end(commit).await {
                            warn!(?err, "failed to end session");
                        }
                        break;
                    }
                }
            }
        });
        Ok((token, handle))
    }

    pub async fn get_user(&self, id: Uuid) -> Result<UserRef> {
        use plugins::users::USERS;

        if let Some(user) = USERS.read().await.get(&id) {
            return Ok(UserRef::clone(user));
        }

        let mut users = USERS.write().await;
        if let Some(user) = users.get(&id) {
            return Ok(UserRef::clone(user));
        }
        let mut session = self.internal_session(SessionMode::Immediate).await?;
        let user = User::try_from(session.get_object_inner(id, true).await?)?;
        let user = Arc::new(ArcSwap::from_pointee(user));

        users.insert(id, user.clone());
        Ok(user)
    }

    pub async fn identity_from_token(&self, token: &IdentityToken) -> Result<Arc<Identity>> {
        use crate::identity::IDENTITIES;

        if let Some(identity) = IDENTITIES.get(token) {
            return Ok(identity.clone());
        }

        // This uses a different strategy from get_user since this would be
        // called more frequently. If the write lock is held for a long time, it
        // will block all other readers. Instead, we use DashMap and fetch the
        // identity from database before putting it into the map.
        let data: Option<String> = self
            .redis_pool
            .get()
            .await?
            .get(Identity::redis_key(token))
            .await?;
        let Some(data) = data.and_then(|it| serde_json::Value::from_str(&it).ok()) else {
            bail!(@InvalidCredentials "invalid identity")
        };
        let identity = Arc::new(Identity::decode(self, data).await?);

        Ok(IDENTITIES
            .entry(token.to_owned())
            .or_insert(identity)
            .clone())
    }

    pub async fn login(&self, name: &str, password: &str) -> Result<Arc<Identity>> {
        User::validate_password(password)?;

        let connection = self.pg_pool.get().await?;
        let sql = "select id from object where tags->'@user.name'->>'v' = $1";
        let sql = connection.prepare_typed_cached(sql, &[Type::TEXT]).await?;
        let Some(row) = connection.query_opt(&sql, &[&name]).await? else {
            bail!(@InvalidCredentials "invalid username or password")
        };
        let id: Uuid = row.get(0);
        drop(connection);

        let user = self.get_user(id).await?;
        user.load().verify(password)?;

        Ok(self.login_as(user))
    }

    pub fn login_as(&self, user: UserRef) -> Arc<Identity> {
        Arc::new(Identity::new_user(
            user,
            self.guest_user.clone(),
            Some(Utc::now() + chrono::Duration::days(7)),
        ))
    }

    pub(crate) async fn dispatch_event(&self, event: Event) {
        if let Err(err) = self
            .dispatch_tx
            .send(DispatchWorkerCommand::Event(event))
            .await
        {
            warn!(?err, "dispatcher disconnected");
        }
    }
}

impl Deref for Novi {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
