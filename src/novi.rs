use arc_swap::ArcSwap;
use chrono::Utc;
use dashmap::DashMap;
use deadpool::managed::Pool;
use redis::AsyncCommands;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::{mpsc, RwLock};
use tokio_postgres::{types::Type, NoTls};
use tracing::warn;
use uuid::Uuid;

use crate::{
    bail,
    filter::Filter,
    function::Function,
    hook::{CoreHookCallback, HookAction, HookArgs, HookCallback, HOOK_POINT_COUNT},
    identity::Identity,
    plugins,
    proto::reg_core_hook_request::HookPoint,
    session::Session,
    subscribe::{DispatchWorkerCommand, Event},
    token::IdentityToken,
    user::{User, UserRef, INTERNAL_USER},
    Config, Result,
};

pub struct Inner {
    pub config: Config,
    pub core_hooks: RwLock<[Vec<(Filter, CoreHookCallback)>; HOOK_POINT_COUNT]>,
    pub functions: DashMap<String, Function>,
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

    pub async fn register_hook(&self, function: &str, before: bool, f: HookCallback) {
        self.functions.alter(function, |_, orig_f| {
            if before {
                Arc::new(move |(session, store), args| {
                    let orig_f = Arc::clone(&orig_f);
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let action = f(HookArgs {
                            arguments: args,
                            original_result: None,
                            session: (session, store),
                        })
                        .await?;
                        match action {
                            HookAction::None => orig_f((session, store), args).await,
                            HookAction::UpdateResult(result) => Ok(result),
                            HookAction::UpdateArgs(new_args) => {
                                orig_f((session, store), &new_args).await
                            }
                        }
                    })
                })
            } else {
                Arc::new(move |(session, store), args| {
                    let orig_f = Arc::clone(&orig_f);
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let result = orig_f((session, store), args).await?;
                        let action = f(HookArgs {
                            arguments: args,
                            original_result: Some(&result),
                            session: (session, store),
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
            }
        });
    }

    pub async fn register_function(&self, name: String, f: Function) -> Result<()> {
        use dashmap::mapref::entry::Entry;
        match self.functions.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(f);
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
    pub async fn new(config: Config) -> Result<Self> {
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

            tags: HashMap::new(),
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

    pub(crate) async fn guest_session(&self, lock: Option<bool>) -> Result<Session> {
        Session::transaction(self.clone(), self.guest_identity.clone(), lock).await
    }

    pub(crate) async fn internal_session(&self, lock: Option<bool>) -> Result<Session> {
        Session::transaction(self.clone(), self.internal_identity.clone(), lock).await
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
        let mut session = self.internal_session(None).await?;
        let user = User::try_from(session.get_object_inner(id).await?)?;
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
