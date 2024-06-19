use chrono::Utc;
use dashmap::DashMap;
use deadpool_postgres::Transaction;
use std::{
    collections::{BTreeSet, HashSet},
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
    function::JsonMap,
    hook::{CoreHookArgs, HOOK_POINT_COUNT},
    identity::Identity,
    misc::BoxFuture,
    novi::Novi,
    object::Object,
    proto::{
        query_request::Order, reg_core_hook_request::HookPoint, EventKind, ObjectLock, SessionMode,
    },
    query::args_to_ref,
    subscribe::{subscriber_task, Event, SubscribeCallback, SubscribeOptions, Subscriber},
    tag::{to_tag_dict, validate_tag_name, validate_tag_value, Tags},
    token::SessionToken,
    Result,
};

pub type SessionAction = Box<dyn for<'a> FnOnce(&'a mut Session) -> BoxFuture<'a, ()> + Send>;

pub(crate) enum SessionCommand {
    Action(SessionAction),
    End {
        commit: bool,
        notify: Option<oneshot::Sender<()>>,
    },
}
pub type SessionStore = DashMap<SessionToken, mpsc::Sender<SessionCommand>>;

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
    pub token: SessionToken,

    // This actually references `connection``, and should be dropped before it.
    // So this MUST be placed before `connection`.
    txn: Option<Transaction<'static>>,
    pub identity: Arc<Identity>,
    pub(crate) connection: Box<PgConnection>,
    mode: SessionMode,

    // Keep track of currently running hooks to avoid reentrancy
    running_hooks: [bool; HOOK_POINT_COUNT],

    // For transactions, events should be dispatched only after the transaction
    // is committed
    pending_events: Vec<Event>,
}
impl Session {
    pub(crate) async fn new(
        novi: Novi,
        identity: Arc<Identity>,
        mode: SessionMode,
    ) -> Result<Self> {
        let mut connection = Box::new(novi.pg_pool.get().await?);
        let txn = match mode {
            SessionMode::SessionAuto
            | SessionMode::SessionReadOnly
            | SessionMode::SessionReadWrite => {
                let txn = connection
                    .build_transaction()
                    .read_only(mode == SessionMode::SessionReadOnly)
                    .start()
                    .await?;
                if mode == SessionMode::SessionReadWrite {
                    txn.execute("lock object in exclusive mode", &[]).await?;
                }
                Some(txn)
            }
            SessionMode::SessionImmediate => None,
        };
        Ok(Self {
            novi,
            token: SessionToken::new(),
            txn: unsafe { std::mem::transmute(txn) },
            identity,
            connection,
            mode,

            running_hooks: Default::default(),

            pending_events: Vec::new(),
        })
    }

    pub fn token(&self) -> &SessionToken {
        &self.token
    }

    pub async fn end(self, commit: bool) -> Result<(), tokio_postgres::Error> {
        if let Some(txn) = self.txn {
            let result = if commit {
                let novi = self.novi.clone();
                let result = txn.commit().await;
                if result.is_ok() {
                    tokio::spawn(async move {
                        for event in self.pending_events {
                            novi.dispatch_event(event).await;
                        }
                    });
                }
                result
            } else {
                txn.rollback().await
            };
            result
        } else {
            Ok(())
        }
    }

    pub(crate) fn require_mutable(&self) -> Result<()> {
        if self.mode == SessionMode::SessionReadOnly {
            bail!(@InvalidArgument "performing mutation in read-only session");
        }
        Ok(())
    }

    /// Puts self into the session_store, allowing the future to re-enter this
    /// session.
    pub(crate) async fn yield_self<'a, R: Send + 'static>(
        &'a mut self,
        fut: impl Future<Output = Result<R>> + Send + 'a,
    ) -> Result<R> {
        let novi = self.novi.clone();
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SessionCommand>(1);
        let (result_tx, result_rx) = oneshot::channel();
        let old = novi.session_store.insert(self.token.clone(), cmd_tx);
        let fut = {
            let novi = novi.clone();
            let token = self.token.clone();
            async move {
                let result = fut.await;
                let Some(sender) = novi.session_store.get(&token) else {
                    warn!("session closed unexpectedly");
                    return;
                };
                let Ok(_) = sender
                    .send(SessionCommand::End {
                        commit: false,
                        notify: None,
                    })
                    .await
                else {
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
                        SessionCommand::Action(action) => action(self).await,
                        SessionCommand::End { .. } => break,
                    }
                },
            }
        }

        if let Some(old) = old {
            novi.session_store.insert(self.token.clone(), old);
        } else {
            novi.session_store.remove(&self.token);
        }
        match result_rx.await {
            Ok(result) => result,
            Err(_) => bail!(@IOError "session closed"),
        }
    }

    async fn run_hook_inner(
        &mut self,
        point: HookPoint,
        freeze: bool,
        object: &mut Object,
        old_object: Option<&Object>,
        deleted_tags: &BTreeSet<String>,
    ) -> Result<()> {
        // On editing this, also edit subscribe::dispatch_worker
        let shared = self.novi.clone();
        let hooks = shared.core_hooks.read().await;
        for (filter, f) in hooks[point as usize].iter().rev() {
            if !filter.matches(object, deleted_tags) {
                continue;
            }
            let edits = f(CoreHookArgs {
                object,
                old_object,
                session: Ok(self),
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
        point: HookPoint,
        freeze: bool,
        object: &mut Object,
        old_object: Option<&Object>,
        deleted_tags: &BTreeSet<String>,
    ) -> Result<()> {
        // Check reentrancy
        if self.running_hooks[point as usize] {
            return Ok(());
        }
        self.running_hooks[point as usize] = true;

        let result = self
            .run_hook_inner(point, freeze, object, old_object, deleted_tags)
            .await;

        self.running_hooks[point as usize] = false;
        result
    }

    pub(crate) async fn run_before_view(&mut self, object: &mut Object) -> Result<()> {
        self.run_hook(
            HookPoint::BeforeView,
            false,
            object,
            None,
            &Default::default(),
        )
        .await
    }

    async fn return_object(&mut self, mut object: Object) -> Result<Object> {
        self.run_before_view(&mut object).await?;
        Ok(object)
    }

    async fn emit_event(
        &mut self,
        kind: EventKind,
        object: Object,
        deleted_tags: BTreeSet<String>,
    ) {
        let event = Event {
            kind,
            object,
            deleted_tags,
        };
        if self.txn.is_some() {
            self.pending_events.push(event);
        } else {
            self.novi.dispatch_event(event).await;
        }
    }

    pub(crate) fn replace_identity(&mut self, identity: Arc<Identity>) -> Arc<Identity> {
        std::mem::replace(&mut self.identity, identity)
    }

    pub(crate) fn replace_internal(&mut self) -> Arc<Identity> {
        self.replace_identity(self.novi.internal_identity.clone())
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
        object: &mut Object,
        old_object: Object,
        deleted_tags: BTreeSet<String>,
    ) -> Result<()> {
        self.run_hook(
            HookPoint::BeforeUpdate,
            false,
            object,
            Some(&old_object),
            &deleted_tags,
        )
        .await?;

        self.save_object(object).await?;

        self.run_hook(
            HookPoint::AfterUpdate,
            true,
            object,
            Some(&old_object),
            &deleted_tags,
        )
        .await?;
        self.emit_event(EventKind::EventUpdate, object.clone(), deleted_tags)
            .await;

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
    pub async fn create_object(&mut self, tags: Tags) -> Result<Object> {
        self.require_mutable()?;
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
            HookPoint::BeforeCreate,
            false,
            &mut object,
            None,
            &Default::default(),
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
            HookPoint::AfterCreate,
            true,
            &mut object,
            None,
            &Default::default(),
        )
        .await?;
        self.emit_event(EventKind::EventCreate, object.clone(), Default::default())
            .await;

        self.return_object(object).await
    }

    pub async fn get_object_inner(&mut self, id: Uuid, lock: ObjectLock) -> Result<Object> {
        let sql = match lock {
            ObjectLock::LockNone => "select * from object where id = $1",
            ObjectLock::LockShare => "select * from object where id = $1 for share",
            ObjectLock::LockExclusive => "select * from object where id = $1 for update",
        };
        let sql = self.prepare_stmt(sql, &[Type::UUID]).await?;
        let row = self.connection.query_opt(&sql, &[&id]).await?;
        let row = match row {
            Some(row) => row,
            None => bail!(@ObjectNotFound ("id" => id.to_string())),
        };

        let object = Object::from_row(row)?;
        self.identity.check_access(&object, AccessKind::View)?;
        Ok(object)
    }

    pub async fn get_object(&mut self, id: Uuid, lock: ObjectLock) -> Result<Object> {
        self.identity.check_perm("object.get")?;
        let object = self.get_object_inner(id, lock).await?;
        self.return_object(object).await
    }

    pub async fn update_object(&mut self, id: Uuid, tags: Tags, force: bool) -> Result<Object> {
        self.require_mutable()?;
        self.identity.check_perm("object.edit")?;
        self.validate_tags(&tags)?;

        debug!(%id, "update object");

        let mut object = self.get_object_inner(id, ObjectLock::LockExclusive).await?;
        self.identity.check_access(&object, AccessKind::Edit)?;
        let old_object = object.clone();

        if !object.update(tags, force) {
            return self.return_object(object).await;
        }

        self.submit_change(&mut object, old_object, Default::default())
            .await?;
        self.return_object(object).await
    }

    pub async fn replace_object(
        &mut self,
        id: Uuid,
        tags: Tags,
        scopes: Option<HashSet<String>>,
        force: bool,
    ) -> Result<Object> {
        self.require_mutable()?;
        self.identity.check_perm("object.edit")?;
        self.validate_tags(&tags)?;

        debug!(%id, "replace object");

        let mut object = self.get_object_inner(id, ObjectLock::LockExclusive).await?;
        self.identity.check_access(&object, AccessKind::Edit)?;
        let old_object = object.clone();

        let Some(deleted_tags) = object.replace(tags, scopes, force) else {
            return self.return_object(object).await;
        };

        self.submit_change(&mut object, old_object, deleted_tags)
            .await?;
        self.return_object(object).await
    }

    pub async fn delete_object_tags(&mut self, id: Uuid, tags: Vec<String>) -> Result<Object> {
        self.require_mutable()?;
        self.identity.check_perm("object.edit")?;
        for tag in &tags {
            validate_tag_name(tag)?;
        }
        self.check_property_perm(tags.iter().map(|it| it.as_str()))?;

        debug!(%id, "delete object tags");

        let mut object = self.get_object_inner(id, ObjectLock::LockExclusive).await?;
        self.identity.check_access(&object, AccessKind::Edit)?;
        let old_object = object.clone();

        let deleted_tags = object.delete_tags(tags);
        if deleted_tags.is_empty() {
            return self.return_object(object).await;
        }

        self.submit_change(&mut object, old_object, deleted_tags)
            .await?;
        self.return_object(object).await
    }

    pub async fn delete_object(&mut self, id: Uuid) -> Result<()> {
        self.require_mutable()?;
        self.identity.check_perm("object.delete")?;

        debug!(%id, "delete object");

        let mut object = self.get_object_inner(id, ObjectLock::LockExclusive).await?;
        self.identity.check_access(&object, AccessKind::Delete)?;
        self.run_hook(
            HookPoint::BeforeDelete,
            true,
            &mut object,
            None,
            &Default::default(),
        )
        .await?;

        let sql = "delete from object where id = $1";
        let sql = self.prepare_stmt(sql, &[Type::UUID]).await?;
        self.connection.execute(&sql, &[&id]).await?;

        self.run_hook(
            HookPoint::AfterDelete,
            true,
            &mut object,
            None,
            &Default::default(),
        )
        .await?;
        self.emit_event(EventKind::EventDelete, object, Default::default())
            .await;

        Ok(())
    }

    pub async fn query(&mut self, filter: Filter, options: QueryOptions) -> Result<Vec<Object>> {
        self.identity.check_perm("object.query")?;

        let (sql, args, types) = filter.query(&self.identity, options);

        let stmt = self.prepare_stmt(&sql, &types).await?;
        let rows = self.connection.query(&stmt, &args_to_ref(&args)).await?;
        let mut objects = Vec::with_capacity(rows.len());

        for row in rows {
            // TODO: parallel?
            objects.push(self.return_object(Object::from_row(row)?).await?);
        }

        Ok(objects)
    }

    pub async fn subscribe(
        &mut self,
        filter: Filter,
        mut options: SubscribeOptions,
        alive: Arc<AtomicBool>,
        callback: SubscribeCallback,
    ) -> Result<()> {
        self.identity.check_perm("subscribe")?;

        info!(%filter, "new subscriber");
        let mut accept_kinds = 0u8;
        for kind in std::mem::take(&mut options.accept_kinds) {
            accept_kinds |= 1 << kind as u8;
        }
        let (objects, ckpt) = if let Some(ckpt) = options.checkpoint {
            (
                self.query(
                    filter.clone(),
                    QueryOptions {
                        checkpoint: Some(ckpt),
                        order: Order::CreatedAsc,
                        ..Default::default()
                    },
                )
                .await?,
                ckpt,
            )
        } else {
            (Vec::new(), Utc::now())
        };
        tokio::spawn(subscriber_task(
            self.novi.clone(),
            Subscriber {
                alive,
                filter,
                identity: self.identity.clone(),
                accept_kinds,
                callback,
            },
            objects,
            ckpt,
            self.novi.dispatch_tx.subscribe(),
        ));
        Ok(())
    }

    pub async fn call_function(&mut self, name: &str, arguments: &JsonMap) -> Result<JsonMap> {
        let novi = self.novi.clone();
        let Some(function) = novi.functions.get(name).map(|it| it.function.clone()) else {
            bail!(@FunctionNotFound "function not found")
        };

        function(self, arguments).await
    }
}
