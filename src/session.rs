use deadpool_postgres::Transaction;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::pin,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::types::{Json, Type};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    anyhow, bail,
    filter::{Filter, QueryOptions},
    hook::{HookArgs, HOOK_POINT_COUNT},
    identity::Identity,
    novi::Novi,
    object::Object,
    proto::{query_request::Order, reg_hook_request::HookPoint, EventKind},
    query::args_to_ref,
    rpc::{Command, SessionStore},
    subscribe::{DispatchWorkerCommand, SubscribeCallback, SubscribeOptions},
    tag::{to_tag_dict, validate_tag_name, validate_tag_value, Tags},
    token::SessionToken,
    Result,
};

type PgConnection = deadpool::managed::Object<deadpool_postgres::Manager>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccessKind {
    View = 0,
    Edit,
    Delete,
}

pub struct Session {
    pub(crate) novi: Novi,
    token: SessionToken,
    txn: Option<Transaction<'static>>,
    pub identity: Arc<Identity>,
    pub(crate) connection: Box<PgConnection>,
    lock: Option<bool>,

    // Keep track of currently running hooks to avoid reentrancy.
    running_hooks: [bool; HOOK_POINT_COUNT],
}
impl Session {
    pub(crate) async fn transaction(
        novi: Novi,
        identity: Arc<Identity>,
        lock: Option<bool>,
    ) -> Result<Self> {
        let mut connection = Box::new(novi.pg_pool.get().await?);
        let txn = match lock {
            Some(lock) => {
                let txn = connection.transaction().await?;
                if lock {
                    txn.execute("lock object in exclusive mode", &[]).await?;
                }
                Some(txn)
            }
            _ => None,
        };
        Ok(Self {
            novi,
            token: SessionToken::new(),
            txn: unsafe { std::mem::transmute(txn) },
            identity,
            connection,
            lock,

            running_hooks: Default::default(),
        })
    }

    pub fn token(&self) -> &SessionToken {
        &self.token
    }

    pub async fn end(self, commit: bool) -> Result<(), tokio_postgres::Error> {
        if let Some(txn) = self.txn {
            if commit {
                txn.commit().await
            } else {
                txn.rollback().await
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn require_locked(&self) -> Result<()> {
        if !matches!(self.lock, None | Some(true)) {
            bail!(@InvalidArgument "session must be locked")
        }
        Ok(())
    }

    /// Puts self into the SessionStore, allowing the future to re-enter this
    /// session.
    pub(crate) async fn yield_self<'a, R: Send + 'static>(
        &'a mut self,
        store: SessionStore,
        fut: impl Future<Output = Result<R>> + Send + 'a,
    ) -> Result<R> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(1);
        let (result_tx, result_rx) = oneshot::channel();
        let old = store.senders.insert(self.token.clone(), cmd_tx);
        let fut = {
            let store = store.clone();
            let token = self.token.clone();
            async move {
                let result = fut.await;
                let Some(sender) = store.senders.get(&token) else {
                    warn!("session closed unexpectedly");
                    return;
                };
                let Ok(_) = sender.send(Command::End { commit: false }).await else {
                    warn!("session closed unexpectedly");
                    return;
                };
                drop(sender);
                let _ = result_tx.send(result);
            }
        };
        let mut fut = pin!(fut);

        loop {
            tokio::select! {
                _ = &mut fut => break,
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        Command::Action(action) => action(self).await,
                        Command::End { .. } => break,
                    }
                },
            }
        }

        if let Some(old) = old {
            store.senders.insert(self.token.clone(), old);
        } else {
            store.senders.remove(&self.token);
        }
        match result_rx.await {
            Ok(result) => result,
            Err(_) => bail!(@IOError "session closed"),
        }
    }

    async fn run_hook_inner(
        &mut self,
        store: SessionStore,
        point: HookPoint,
        freeze: bool,
        object: &mut Object,
        old_object: Option<&Object>,
        deleted_tags: &HashSet<String>,
    ) -> Result<()> {
        // On editing this, also edit subscribe::dispatch_worker
        let shared = self.novi.clone();
        let hooks = shared.hooks.read().await;
        for (filter, f) in hooks[point as usize].iter().rev() {
            if !filter.matches(object, deleted_tags) {
                continue;
            }
            let edits = f(HookArgs {
                object,
                old_object,
                session: Some((self, &store)),
            })
            .await?;
            if freeze {
                if !edits.is_empty() {
                    bail!(@InvalidArgument "hook must not modify object");
                }
            } else {
                edits.apply(object, object.updated);
            }
        }
        Ok(())
    }

    async fn run_hook(
        &mut self,
        store: Option<SessionStore>,
        point: HookPoint,
        freeze: bool,
        object: &mut Object,
        old_object: Option<&Object>,
        deleted_tags: &HashSet<String>,
    ) -> Result<()> {
        // Check reentrancy
        if self.running_hooks[point as usize] {
            return Ok(());
        }
        self.running_hooks[point as usize] = true;
        // If no SessionStore is provided, hooks are disabled
        let Some(store) = store else {
            return Ok(());
        };

        let result = self
            .run_hook_inner(store, point, freeze, object, old_object, deleted_tags)
            .await;

        self.running_hooks[point as usize] = false;
        result
    }

    async fn return_object(
        &mut self,
        store: Option<SessionStore>,
        mut object: Object,
    ) -> Result<Object> {
        self.run_hook(
            store,
            HookPoint::BeforeView,
            false,
            &mut object,
            None,
            &HashSet::new(),
        )
        .await?;
        Ok(object)
    }

    async fn object_event(&self, kind: EventKind, object: Object, deleted_tags: HashSet<String>) {
        if let Err(err) = self
            .novi
            .dispatch_tx
            .send(DispatchWorkerCommand::Event {
                kind,
                object,
                deleted_tags,
            })
            .await
        {
            warn!(?err, "dispatcher disconnected");
        }
    }

    /// Save an object to the database.
    pub(crate) async fn save_object(&self, object: &Object) -> Result<()> {
        let sql = "
        update object
        set tags = $2, updated = $3
        where id = $1";
        let sql = self
            .prepare_stmt(sql, &[Type::UUID, Type::JSONB, Type::TIMESTAMPTZ])
            .await?;
        self.connection
            .execute(&sql, &[&object.id, &Json(&object.tags), &object.updated])
            .await?;
        Ok(())
    }

    async fn submit_change(
        &mut self,
        store: Option<SessionStore>,
        object: &mut Object,
        old_object: Object,
        deleted_tags: HashSet<String>,
    ) -> Result<()> {
        self.run_hook(
            store.clone(),
            HookPoint::BeforeUpdate,
            false,
            object,
            Some(&old_object),
            &deleted_tags,
        )
        .await?;

        self.save_object(object).await?;

        self.run_hook(
            store,
            HookPoint::AfterUpdate,
            true,
            object,
            Some(&old_object),
            &deleted_tags,
        )
        .await?;
        self.object_event(EventKind::Update, object.clone(), deleted_tags)
            .await;

        Ok(())
    }

    fn check_access(&self, object: &Object, access: AccessKind) -> Result<()> {
        if object.creator.is_some() && object.creator == self.identity.user.load().id {
            return Ok(()); // creator has all permissions
        }

        if access != AccessKind::View {
            for (req, _) in object.subtags("@access.view") {
                self.identity.check_perm(req).ok();
                if !self.identity.has_perm(req) {
                    bail!(@PermissionDenied "access denied");
                }
            }
        }

        let prefix = match access {
            AccessKind::View => "@access.view",
            AccessKind::Edit => "@access.edit",
            AccessKind::Delete => "@access.delete",
        };
        for (req, _) in object.subtags(prefix) {
            if !self.identity.has_perm(req) {
                bail!(@PermissionDenied "access denied");
            }
        }

        Ok(())
    }

    async fn prepare_stmt(
        &self,
        sql: &str,
        types: &[Type],
    ) -> Result<tokio_postgres::Statement, tokio_postgres::Error> {
        if let Some(txn) = &self.txn {
            txn.prepare_typed_cached(sql, types).await
        } else {
            self.connection.prepare_typed_cached(sql, types).await
        }
    }

    fn check_property_perm<'a>(&self, tags: impl Iterator<Item = &'a str>) -> Result<()> {
        if !self.identity.has_perm("prop") {
            for tag in tags {
                if tag == "@" {
                    self.identity.check_perm("prop.content")?;
                } else if let Some(prop) = tag.strip_prefix('@') {
                    self.identity.check_perm(&format!("prop:{prop}"))?;
                }
            }
        }
        Ok(())
    }

    fn validate_tags(&self, tags: &Tags) -> Result<()> {
        for (tag, value) in tags {
            validate_tag_name(tag)?;
            validate_tag_value(tag, value.as_deref())?;
        }
        self.check_property_perm(tags.keys().map(|it| it.as_str()))?;
        Ok(())
    }
}

impl Session {
    pub async fn create_object(
        &mut self,
        store: Option<SessionStore>,
        tags: Tags,
    ) -> Result<Object> {
        self.require_locked()?;
        self.identity.check_perm("object.create")?;
        self.validate_tags(&tags)?;

        let (time, tag_dict) = to_tag_dict(tags);
        let mut object = Object {
            id: Uuid::new_v4(),
            tags: tag_dict,
            creator: self.identity.user.load().id,
            created: time,
            updated: time,
        };
        self.run_hook(
            store.clone(),
            HookPoint::BeforeCreate,
            false,
            &mut object,
            None,
            &HashSet::new(),
        )
        .await?;

        let sql = "
        insert into object (id, tags, creator, created, updated)
        values ($1, $2, $3, $4, $4)";
        let sql = self
            .prepare_stmt(
                sql,
                &[Type::UUID, Type::JSONB, Type::UUID, Type::TIMESTAMPTZ],
            )
            .await?;
        self.connection
            .execute(
                &sql,
                &[&object.id, &Json(&object.tags), &object.creator, &time],
            )
            .await?;

        self.run_hook(
            store.clone(),
            HookPoint::AfterCreate,
            true,
            &mut object,
            None,
            &HashSet::new(),
        )
        .await?;
        self.object_event(EventKind::Create, object.clone(), HashSet::new())
            .await;

        self.return_object(store, object).await
    }

    pub async fn get_object_inner(&mut self, id: Uuid) -> Result<Object> {
        let sql = "select * from object where id = $1";
        let sql = self.prepare_stmt(sql, &[Type::UUID]).await?;
        let row = self.connection.query_opt(&sql, &[&id]).await?;
        let row = match row {
            Some(row) => row,
            None => bail!(@ObjectNotFound ("id" => id.to_string()) "object {id} not found"),
        };

        let object = Object::from_row(row)?;
        self.check_access(&object, AccessKind::View)?;
        Ok(object)
    }

    pub async fn get_object(&mut self, store: Option<SessionStore>, id: Uuid) -> Result<Object> {
        self.identity.check_perm("object.get")?;
        let object = self.get_object_inner(id).await?;
        self.return_object(store, object).await
    }

    pub async fn update_object(
        &mut self,
        store: Option<SessionStore>,
        id: Uuid,
        tags: Tags,
        force: bool,
    ) -> Result<Object> {
        self.require_locked()?;
        self.identity.check_perm("object.edit")?;
        self.validate_tags(&tags)?;

        debug!(%id, "update object");

        let mut object = self.get_object_inner(id).await?;
        self.check_access(&object, AccessKind::Edit)?;
        let old_object = object.clone();

        if !object.update(tags, force) {
            return self.return_object(store, object).await;
        }

        self.submit_change(store.clone(), &mut object, old_object, HashSet::new())
            .await?;
        self.return_object(store, object).await
    }

    pub async fn replace_object(
        &mut self,
        store: Option<SessionStore>,
        id: Uuid,
        tags: Tags,
        scopes: Option<HashSet<String>>,
        force: bool,
    ) -> Result<Object> {
        self.require_locked()?;
        self.identity.check_perm("object.edit")?;
        self.validate_tags(&tags)?;

        debug!(%id, "replace object");

        let mut object = self.get_object_inner(id).await?;
        self.check_access(&object, AccessKind::Edit)?;
        let old_object = object.clone();

        let Some(deleted_tags) = object.replace(tags, scopes, force) else {
            return self.return_object(store, object).await;
        };

        self.submit_change(store.clone(), &mut object, old_object, deleted_tags)
            .await?;
        self.return_object(store, object).await
    }

    pub async fn delete_object_tags(
        &mut self,
        store: Option<SessionStore>,
        id: Uuid,
        tags: Vec<String>,
    ) -> Result<Object> {
        self.require_locked()?;
        self.identity.check_perm("object.edit")?;
        for tag in &tags {
            validate_tag_name(tag)?;
        }
        self.check_property_perm(tags.iter().map(|it| it.as_str()))?;

        debug!(%id, "delete object tags");

        let mut object = self.get_object_inner(id).await?;
        self.check_access(&object, AccessKind::Edit)?;
        let old_object = object.clone();

        let deleted_tags = object.delete_tags(tags);
        if deleted_tags.is_empty() {
            return self.return_object(store, object).await;
        }

        self.submit_change(store.clone(), &mut object, old_object, deleted_tags)
            .await?;
        self.return_object(store, object).await
    }

    pub async fn delete_object(&mut self, store: Option<SessionStore>, id: Uuid) -> Result<()> {
        self.require_locked()?;
        self.identity.check_perm("object.delete")?;

        debug!(%id, "delete object");

        let mut object = self.get_object_inner(id).await?;
        self.check_access(&object, AccessKind::Delete)?;
        self.run_hook(
            store.clone(),
            HookPoint::BeforeDelete,
            true,
            &mut object,
            None,
            &HashSet::new(),
        )
        .await?;

        let sql = "delete from object where id = $1";
        let sql = self.prepare_stmt(sql, &[Type::UUID]).await?;
        self.connection.execute(&sql, &[&id]).await?;

        self.run_hook(
            store,
            HookPoint::AfterDelete,
            true,
            &mut object,
            None,
            &HashSet::new(),
        )
        .await?;
        self.object_event(EventKind::Delete, object, HashSet::new())
            .await;

        Ok(())
    }

    pub async fn query(
        &mut self,
        store: Option<SessionStore>,
        filter: Filter,
        options: QueryOptions,
    ) -> Result<Vec<Object>> {
        self.identity.check_perm("object.query")?;

        let (sql, args, types) = filter.query(&self.identity, options);

        let stmt = self.prepare_stmt(&sql, &types).await?;
        let rows = self.connection.query(&stmt, &args_to_ref(&args)).await?;
        let mut objects = Vec::with_capacity(rows.len());

        for row in rows {
            // TODO: parallel?
            objects.push(
                self.return_object(store.clone(), Object::from_row(row)?)
                    .await?,
            );
        }

        Ok(objects)
    }

    pub async fn subscribe(
        &mut self,
        store: Option<SessionStore>,
        filter: Filter,
        options: SubscribeOptions,
        alive: Arc<AtomicBool>,
        mut callback: SubscribeCallback,
    ) -> Result<()> {
        self.identity.check_perm("subscribe")?;

        info!("new subscriber");
        if let Some(ckpt) = options.checkpoint {
            let objects = self
                .query(
                    store,
                    filter.clone(),
                    QueryOptions {
                        checkpoint: Some(ckpt),
                        order: Order::CreatedAsc,
                        ..Default::default()
                    },
                )
                .await?;
            for object in objects {
                callback(
                    &object,
                    if object.created >= ckpt {
                        EventKind::Create
                    } else {
                        EventKind::Update
                    },
                )
                .await;
            }
        }
        let mut accept_kinds = 0u8;
        for kind in options.accept_kinds {
            accept_kinds |= 1 << kind as u8;
        }
        self.novi
            .dispatch_tx
            .send(DispatchWorkerCommand::NewSub {
                alive: alive.clone(),
                filter,
                accept_kinds,
                callback: Box::new(callback),
            })
            .await
            .map_err(|_| anyhow!(@IOError "dispatcher disconnected"))?;
        Ok(())
    }

    pub async fn call_function(
        &mut self,
        store: SessionStore,
        name: &str,
        arguments: HashMap<String, Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let novi = self.novi.clone();
        let Some(function) = novi.functions.get(name) else {
            bail!(@FunctionNotFound "function not found")
        };

        function((self, store), arguments).await
    }
}
