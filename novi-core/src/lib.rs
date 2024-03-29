mod client;
mod config;
mod dispatch;
mod error;
mod filter;
pub mod hook;
mod ipc;
pub mod log;
mod misc;
mod model;
mod object;
mod plugin;
mod py;
mod query;
mod rule;
mod session;
mod tag;
pub mod user;

pub use client::{EventKind, RpcProvider, Subscriber};
pub use config::NoviConfig;
pub use error::{Error, ErrorKind, Result};
pub use filter::{Filter, FilterKind, TimeRange};
pub use ipc::sub_main;
pub use model::Model;
pub use object::{Object, ObjectMeta};
pub use session::Session;
pub use tag::TagValue;
pub use user::{AccessKind, User};

pub(crate) use error::{anyhow, bail};

use aes_gcm::Aes256Gcm;
use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use dispatch::Dispatcher;
use error::ResultExt;
use interprocess::local_socket::tokio as tokio_ipc;
use key_mutex::tokio::{KeyMutex, OwnedMutexGuard};
use moka::future::Cache;
use once_cell::sync::Lazy;
use plugin::{PluginInfo, PluginState};
use rule::{parse_rules, query_unsatisfied, Rule, RuleSet};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
    prelude::FromRow,
    ConnectOptions, Pool, Postgres,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    iter,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tag::{scope_of, valid_tag_char, Tag};
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task_local,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{error::Context, session::internal_scope};

pub static ROOT_PATH: Lazy<PathBuf> = Lazy::new(|| {
    (if std::env::args().nth(1).as_deref() == Some("--plugin-host") {
        "../../"
    } else {
        "."
    })
    .into()
});

task_local! {
    pub static IF_UNMODIFIED_SINCE: DateTime<Utc>;
}

fn check_action_precondition(object: &Object) -> Result<()> {
    if IF_UNMODIFIED_SINCE.try_with(|it| *it < object.meta().updated) == Ok(true) {
        bail!(@PreconditionFailed);
    }

    Ok(())
}

#[derive(FromRow)]
pub struct SimilarObject {
    pub object: Object,
    pub similarity: f64,
}

const MIGRATOR: Migrator = sqlx::migrate!();

pub trait JoinToString {
    fn join(self, sep: &str) -> String;
}

impl<V: AsRef<str>, T: Iterator<Item = V>> JoinToString for T {
    fn join(mut self, sep: &str) -> String {
        let mut result = String::new();
        if let Some(first) = self.next() {
            result += first.as_ref();
            for element in self {
                result += sep;
                result += element.as_ref();
            }
        }
        result
    }
}

struct SubscriberState {
    subscriber: Subscriber,
    exclude_unrelated: bool,
}

enum WorkerMessage {
    Event {
        kind: EventKind,
        object: Arc<Object>,
        deleted_tags: BTreeSet<String>,
    },
    NewSub {
        id: Uuid,
        filter: Filter,
        state: SubscriberState,
    },
    RemoveSub {
        id: Uuid,
    },
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Order {
    pub created: bool,
    pub asc: bool,
}
impl FromStr for Order {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (asc, field) = if let Some(body) = s.strip_prefix('-') {
            (false, body)
        } else if let Some(body) = s.strip_prefix('+') {
            (true, body)
        } else {
            (false, s)
        };
        let created = match field {
            "created" => true,
            "updated" => false,
            _ => bail!(@InvalidOrder),
        };

        Ok(Self { created, asc })
    }
}

pub type RpcArgs = BTreeMap<String, serde_json::Value>;
pub type Tags = BTreeMap<String, Option<String>>;

pub struct Novi {
    db: Pool<Postgres>,

    rule_set: RwLock<RuleSet>,

    worker_tx: mpsc::UnboundedSender<WorkerMessage>,
    object_lock: KeyMutex<Uuid, ()>,

    user_cache: Cache<Uuid, Arc<User>>,
    user_lock: RwLock<()>,

    tags: RwLock<HashMap<String, Arc<Tag>>>,

    rpc_providers: DashMap<String, (Option<Uuid>, RpcProvider)>,

    plugins: DashMap<String, PluginState>,

    pub(crate) session_key: aes_gcm::Key<Aes256Gcm>,
}

impl Novi {
    pub async fn new(database_url: &str, config: NoviConfig) -> Result<Arc<Self>> {
        let opt: PgConnectOptions = database_url.parse()?;
        let db = PgPoolOptions::new()
            .connect_with(opt.disable_statement_logging())
            .await?;
        MIGRATOR.run(&db).await.wrap()?;

        let (novi_tx, novi_rx) = oneshot::channel::<Arc<Novi>>();

        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let novi = novi_rx.await.unwrap();
            let mut subscribers = Dispatcher::new();
            while let Some(obj) = rx.recv().await {
                match obj {
                    WorkerMessage::NewSub { id, filter, state } => {
                        subscribers.insert(id, filter, state);
                    }
                    WorkerMessage::RemoveSub { id } => {
                        subscribers.remove(&id);
                    }
                    WorkerMessage::Event {
                        kind,
                        object,
                        deleted_tags,
                    } => {
                        debug!(object = %object.id, ?kind, "dispatch event");
                        for (.., sub) in subscribers.dispatch_mut(
                            &object,
                            |it| it.exclude_unrelated,
                            &deleted_tags,
                        ) {
                            (sub.subscriber)(&object, kind);
                        }

                        if kind == EventKind::Deleted {
                            let _ = std::fs::remove_file(format!("../storage/{}", object.id));
                            let _ =
                                std::fs::remove_file(format!("../storage/{}.thumb.jpg", object.id));
                            let _ = std::fs::remove_file(format!("../storage/{}.opt", object.id));
                        }
                        if object.tags.contains_key("@event") {
                            if let Err(err) = internal_scope(novi.delete_object(object.id)).await {
                                warn!(id = %object.id, ?err, "failed to delete event object");
                            }
                        }
                        if object.tags.contains_key("@group") && kind == EventKind::Deleted {
                            if let Err(err) = internal_scope::<Result<()>>(async {
                                let children = novi
                                    .query(
                                        Filter::Ands(vec![
                                            Filter::Atom(
                                                "@parent".to_owned(),
                                                FilterKind::Equals(object.id.to_string(), true),
                                            ),
                                            Filter::Neg(Box::new(Filter::Atom(
                                                "@hidden".to_owned(),
                                                FilterKind::Has,
                                            ))),
                                        ]),
                                        None,
                                        TimeRange::default(),
                                        TimeRange::default(),
                                        Order::default(),
                                        None,
                                    )
                                    .await?;
                                for child in children {
                                    novi.delete_object(child.id).await?;
                                }
                                Ok(())
                            })
                            .await
                            {
                                warn!(id = %object.id, ?err, "failed to delete group children");
                            }
                        }
                    }
                }
            }
        });

        let res = Arc::new(Self {
            db,

            rule_set: RwLock::new(RuleSet::default()),

            worker_tx: tx,
            object_lock: KeyMutex::new(),

            user_cache: Cache::new(128),
            user_lock: RwLock::default(),

            tags: RwLock::default(),

            rpc_providers: DashMap::new(),

            plugins: DashMap::new(),

            session_key: Sha256::digest(config.session_key.as_bytes()),
        });

        internal_scope::<Result<()>>(async {
            let rules = res
                .get_rules()
                .await?
                .into_iter()
                .map(|(id, value)| (id, parse_rules(&value).unwrap()));
            let tag_rules = res
                .get_tags()
                .await?
                .into_iter()
                .filter(|it| !it.implies.is_empty())
                .map(|it| {
                    (
                        it.id,
                        vec![Rule::new(
                            it.name.parse().unwrap(),
                            it.implies.parse().unwrap(),
                        )],
                    )
                });
            *res.rule_set.write().await = RuleSet::new(rules.chain(tag_rules).collect());
            Ok(())
        })
        .await?;
        novi_tx.send(Arc::clone(&res)).ok().unwrap();

        let novi = Arc::clone(&res);
        tokio::spawn(async move {
            let listener = tokio_ipc::LocalSocketListener::bind("@novi").unwrap();
            loop {
                let stream = listener.accept().await.unwrap();
                // TODO optimize: only one task on server side
                let _ = ipc::server::new_socket(Arc::clone(&novi), stream);
            }
        });

        let dir = Path::new("plugins");
        for file in dir.read_dir()? {
            let file = file?;
            let name = file.file_name();
            let state = match Self::load_plugin(&name) {
                Ok(Some(it)) => it,
                Ok(None) => continue,
                Err(err) => {
                    warn!(
                        ?err,
                        name = name.to_string_lossy().into_owned(),
                        "failed to load plugin"
                    );
                    continue;
                }
            };
            res.plugins.insert(name.to_str().unwrap().to_owned(), state);
        }

        Ok(res)
    }

    fn object_not_found(id: Uuid) -> Error {
        anyhow!(@ObjectNotFound "object {id} not found")
    }

    fn load_plugin(name: impl AsRef<Path>) -> Result<Option<PluginState>> {
        let name = name.as_ref();
        let path = Path::new("plugins").join(name);
        if !path.exists() {
            return Ok(None);
        }
        let info_file = path.join("plugin.yml");
        if !info_file.exists() {
            return Ok(None);
        }
        let info: PluginInfo = serde_yaml::from_reader(std::fs::File::open(info_file)?).wrap()?;
        if info.disabled {
            return Ok(None);
        }

        let secret_key = Uuid::new_v4();
        let process = std::process::Command::new(std::env::current_exe()?)
            .arg("--plugin-host")
            .arg(name)
            .arg(secret_key.to_string())
            .env("WORKER_THREADS", "3")
            .current_dir(&path)
            .spawn()
            .with_context(|| format!("failed to load plugin {:?}", info.name))?;
        info!(name = info.name, "plugin loaded");

        Ok(Some(PluginState {
            secret_key,
            info,
            process,
        }))
    }

    async fn fetch_object(&self, id: Uuid) -> Result<Object> {
        let row = Object::query_id(id)
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| Self::object_not_found(id));
        row.and_then(Object::from_row)
    }

    async fn fetch_and_lock(&self, id: Uuid) -> Result<(Object, OwnedMutexGuard<Uuid, ()>)> {
        let lock = self.object_lock.lock(id).await;
        let object = self.fetch_object(id).await?;
        check_action_precondition(&object)?;
        Ok((object, lock))
    }

    pub async fn get_object(&self, id: Uuid) -> Result<Object> {
        let object = self.fetch_object(id).await?;
        self.check_object(&object, AccessKind::View).await?;
        Ok(object)
    }

    pub async fn reload_plugin(&self, name: &str) {
        if let Some((_, mut state)) = self.plugins.remove(name) {
            let pid = state.process.id();
            state
                .process
                .kill()
                .and_then(|_| state.process.wait())
                .unwrap();
            ipc::server::wait_terminate(pid).await;
        }
        let state = match Self::load_plugin(name) {
            Ok(Some(it)) => it,
            Ok(None) => return,
            Err(err) => {
                warn!(?err, name, "failed to load plugin");
                return;
            }
        };
        self.plugins.insert(name.to_owned(), state);
    }

    pub async fn get_object_if_modified(
        &self,
        id: Uuid,
        since: DateTime<Utc>,
    ) -> Result<Option<Object>> {
        let mut tr = self.db.begin().await?;
        let updated = sqlx::query_scalar!("select updated from object where id = $1", id)
            .fetch_optional(tr.as_mut())
            .await?;
        let Some(updated) = updated else {
            return Err(Self::object_not_found(id));
        };
        if updated == since {
            return Ok(None);
        }

        let row = Object::query_id(id)
            .fetch_optional(tr.as_mut())
            .await?
            .ok_or_else(|| Self::object_not_found(id));
        row.and_then(Object::from_row).map(Some)
    }

    #[inline]
    pub async fn add_model<M: Model>(&self, model: &M) -> Result<Arc<Object>> {
        self.add_object(model.to_tags()).await
    }

    pub async fn get_model<M: Model>(&self, id: Uuid) -> Result<M> {
        self.get_object(id).await?.try_into()
    }

    #[inline]
    pub async fn save_model<M: Model>(&self, model: &M) -> Result<Arc<Object>> {
        self.update_object(model.id(), model.to_tags(), None, false)
            .await
    }
}

fn validate_tag(tag: &str) -> Result<()> {
    if tag.is_empty() {
        bail!(@InvalidTag "empty tag");
    }
    if tag.len() > 100 {
        bail!(@InvalidTag "tag too long");
    }

    let body = match tag.chars().next() {
        Some('#') => {
            if tag.len() == 1 {
                bail!(@InvalidTag "empty tag");
            }
            &tag[1..]
        }
        Some('@') => &tag[1..],
        _ => tag,
    };
    if !body.chars().all(valid_tag_char) {
        bail!(@InvalidTag "tag contains invalid characters");
    }

    Ok(())
}

impl Novi {
    async fn validate_tags<'a>(
        &self,
        tags: impl Iterator<Item = &'a str>,
        edit: bool,
    ) -> Result<()> {
        let can_modify_internal = session::has_perm("itag");

        for tag in tags {
            validate_tag(tag)?;

            if edit {
                if let Some(name) = tag.strip_prefix('@') {
                    if !can_modify_internal && !session::has_perm(&format!("itag:{name}")) {
                        bail!(@PermissionDenied "can't modify tag {tag:?}");
                    }
                }
            }
        }
        Ok(())
    }

    async fn check_object(&self, object: &Object, kind: AccessKind) -> Result<()> {
        if session::user_id().map_or(false, |it| Some(it) == object.meta.creator) {
            return Ok(());
        }

        fn check(object: &Object, kind: AccessKind) -> Result<()> {
            let s = format!("@access.{kind}:");
            for (tag, _) in object.tags.range(s.clone()..=(s.clone() + "\u{ff}")) {
                let perm = tag.strip_prefix(&s).unwrap();
                if !session::has_perm(perm) {
                    bail!(@AccessDenied "access {kind:?} to object {}", object.id);
                }
            }

            Ok(())
        }
        check(object, AccessKind::View)?;
        if !matches!(kind, AccessKind::View) {
            check(object, kind)?;
        }

        Ok(())
    }

    async fn submit_change(
        &self,
        old: &Object,
        mut object: Object,
        deleted_tags: BTreeSet<String>,
    ) -> Result<Arc<Object>> {
        self.user_cache.remove(&object.id).await;

        let time = object.meta.updated;

        self.rule_set.read().await.closure(&mut object, time);
        hook::run(
            hook::BeforeUpdateObject {
                old,
                object: &mut object,
            },
            |it| &it.object,
            &deleted_tags,
        )?;
        // TODO optimize
        self.rule_set.read().await.closure(&mut object, time);
        object.save().execute(&self.db).await?;
        hook::run(
            hook::AfterUpdateObject {
                old,
                object: &object,
            },
            |it| &it.object,
            &deleted_tags,
        )?;

        self.object_event(EventKind::Updated, object, deleted_tags)
            .await
    }

    async fn object_event(
        &self,
        kind: EventKind,
        object: Object,
        deleted_tags: BTreeSet<String>,
    ) -> Result<Arc<Object>> {
        let object = Arc::new(object);

        self.worker_tx
            .send(WorkerMessage::Event {
                kind,
                object: Arc::clone(&object),
                deleted_tags,
            })
            .wrap()?;

        Ok(object)
    }

    fn to_tag_values(&self, tags: Tags) -> Result<(DateTime<Utc>, BTreeMap<String, TagValue>)> {
        let time = Utc::now();
        let tags = tags
            .into_iter()
            .map(|(tag, value)| {
                (
                    tag,
                    TagValue {
                        value: value.clone(),
                        updated: time,
                    },
                )
            })
            .collect();

        Ok((time, tags))
    }

    pub async fn add_object(&self, tags: Tags) -> Result<Arc<Object>> {
        self.add_object_inner(tags, None).await
    }

    async fn add_object_inner(
        &self,
        tags: Tags,
        rule_set: Option<&RuleSet>,
    ) -> Result<Arc<Object>> {
        session::check_perm("object.create")?;

        self.validate_tags(tags.keys().map(|it| it.as_str()), true)
            .await?;

        let time = Utc::now();
        let tags = tags
            .into_iter()
            .map(|(tag, value)| (tag, TagValue::create(value, time)))
            .collect();

        let mut object = Object {
            id: Uuid::default(),
            tags,
            meta: ObjectMeta {
                creator: session::user_id(),
                updated: time,
                created: time,
            },
        };
        match rule_set {
            Some(rule_set) => rule_set.closure(&mut object, time),
            None => self.rule_set.read().await.closure(&mut object, time),
        };
        hook::run(
            hook::BeforeAddObject {
                object: &mut object,
            },
            |it| &it.object,
            &BTreeSet::new(),
        )?;
        // TODO optimize
        match rule_set {
            Some(rule_set) => rule_set.closure(&mut object, time),
            None => self.rule_set.read().await.closure(&mut object, time),
        };

        object.id = sqlx::query_scalar!(
            "insert into object(tags, created, updated) values($1, $2, $2) returning id",
            serde_json::to_value(&object.tags).unwrap(),
            time
        )
        .fetch_one(&self.db)
        .await?;

        let _guard = self.object_lock.lock(object.id).await;

        hook::run(
            hook::AfterAddObject { object: &object },
            |it| &it.object,
            &BTreeSet::new(),
        )?;

        let object = self
            .object_event(EventKind::Created, object, BTreeSet::new())
            .await?;
        if !object.tags.contains_key("@event") {
            info!(id = %object.id(), "create object");
        }

        Ok(object)
    }

    #[inline]
    pub async fn delete_object(&self, id: Uuid) -> Result<()> {
        let (mut obj, _guard) = self.fetch_and_lock(id).await?;
        if !obj.tags.contains_key("@event") {
            info!(id = %id, "delete object");
        }
        self.check_object(&obj, AccessKind::Delete).await?;

        hook::run(
            hook::BeforeDeleteObject { object: &mut obj },
            |it| &it.object,
            &BTreeSet::new(),
        )?;
        sqlx::query!("delete from object where id = $1", id)
            .execute(&self.db)
            .await?;
        hook::run(
            hook::AfterDeleteObject { object: &obj },
            |it| &it.object,
            &BTreeSet::new(),
        )?;

        if !obj.tags().contains_key("@event") {
            self.object_event(EventKind::Deleted, obj, BTreeSet::new())
                .await?;
        }

        Ok(())
    }

    async fn set_object_tags_force(&self, mut obj: Object, tags: Tags) -> Result<Arc<Object>> {
        let (time, tags) = self.to_tag_values(tags)?;
        let old = obj.clone();
        obj.tags.extend(tags.into_iter());
        obj.meta.updated = time;
        self.submit_change(&old, obj, BTreeSet::new()).await
    }

    pub async fn update_object(
        &self,
        id: Uuid,
        mut tags: Tags,
        scopes: Option<BTreeSet<String>>,
        force_update: bool,
    ) -> Result<Arc<Object>> {
        debug!(%id, ?tags, ?scopes, "put object");
        self.validate_tags(tags.keys().map(|it| it.as_str()), true)
            .await?;

        let (mut obj, _guard) = self.fetch_and_lock(id).await?;
        self.check_object(&obj, AccessKind::Edit).await?;

        if let Some(scopes) = &scopes {
            tags.retain(|tag, _| scopes.contains(scope_of(tag)));
        }
        let (time, mut tags) = self.to_tag_values(tags)?;
        if !force_update {
            for (tag, value) in &mut tags {
                if let Some(old_val) = obj.tags.get(tag) {
                    if old_val.value == value.value {
                        value.updated = old_val.updated;
                    }
                }
            }
        }

        // TODO optimize
        let old = obj.clone();

        let mut deleted_tags = BTreeSet::new();
        if let Some(scopes) = &scopes {
            obj.tags.retain(|tag, _| {
                if scopes.contains(scope_of(tag)) {
                    deleted_tags.insert(tag.to_owned());
                    false
                } else {
                    true
                }
            });
            obj.tags.extend(tags.into_iter());
        } else {
            deleted_tags = std::mem::replace(&mut obj.tags, tags).into_keys().collect();
        }
        if !force_update && old.tags == obj.tags {
            return Ok(Arc::new(obj));
        }
        obj.meta.updated = time;
        self.submit_change(&old, obj, deleted_tags).await
    }

    pub async fn set_object_tag(
        &self,
        id: Uuid,
        tag: &str,
        value: Option<String>,
        force_update: bool,
    ) -> Result<Arc<Object>> {
        self.set_object_tags(
            id,
            iter::once((tag.to_owned(), value)).collect(),
            force_update,
        )
        .await
    }

    pub async fn set_object_tags(
        &self,
        id: Uuid,
        tags: Tags,
        force_update: bool,
    ) -> Result<Arc<Object>> {
        debug!(%id, ?tags, force_update, "insert object tags");
        self.validate_tags(tags.keys().map(|it| it.as_str()), true)
            .await?;

        let (mut obj, _guard) = self.fetch_and_lock(id).await?;
        self.check_object(&obj, AccessKind::Edit).await?;

        if force_update {
            return self.set_object_tags_force(obj, tags).await;
        }

        let old = obj.clone();
        let time = Utc::now();
        let mut updated = false;
        for (tag, value) in tags {
            let entry = obj.tags.entry(tag).or_insert_with(|| {
                updated = true;
                TagValue::create(None, time)
            });
            if entry.value != value {
                entry.value = value;
                entry.updated = time;
                updated = true;
            }
        }
        if !updated {
            debug!("not updated");
            return Ok(Arc::new(obj));
        }
        obj.meta.updated = time;
        self.submit_change(&old, obj, BTreeSet::new()).await
    }

    pub async fn delete_object_tag(&self, id: Uuid, tag: &str) -> Result<Arc<Object>> {
        debug!(%id, tag, "delete object tag");
        self.validate_tags(iter::once(tag), true).await?;

        let (mut obj, _guard) = self.fetch_and_lock(id).await?;
        self.check_object(&obj, AccessKind::Edit).await?;

        let old = obj.clone();
        let time = Utc::now();
        if obj.tags.remove(tag).is_none() {
            return Ok(Arc::new(obj));
        }
        obj.meta.updated = time;
        self.submit_change(&old, obj, iter::once(tag.to_owned()).collect())
            .await
    }

    pub async fn get_object_tag(&self, id: Uuid, tag: &str) -> Result<Option<String>> {
        let mut obj = self.fetch_object(id).await?;
        self.check_object(&obj, AccessKind::View).await?;

        Ok(obj
            .tags
            .remove(tag)
            .ok_or_else(|| anyhow!(@TagNotFound "tag {tag:?} not found in object {id}"))?
            .value)
    }

    pub async fn query(
        &self,
        mut filter: Filter,
        checkpoint: Option<DateTime<Utc>>,
        updated_range: TimeRange,
        created_range: TimeRange,
        order: Order,
        limit: Option<u32>,
    ) -> Result<Vec<Object>> {
        filter += Filter::Neg(Box::new(Filter::Atom("@event".to_owned(), FilterKind::Has)));
        let (sql, args) = filter.query(checkpoint, updated_range, created_range, order, limit);

        sqlx::query_with(&sql, args)
            .fetch_all(&self.db)
            .await?
            .into_iter()
            .map(Object::from_row)
            .collect()
    }
}

impl Novi {
    pub async fn subscribe(
        &self,
        filter: Filter,
        checkpoint: Option<DateTime<Utc>>,
        with_history: bool,
        exclude_unrelated: bool,
        mut subscriber: Subscriber,
    ) -> Result<Uuid> {
        let id = Uuid::new_v4();
        info!(%id, "new subscriber");
        if let Some(ckpt) =
            Some(checkpoint).or_else(|| if with_history { Some(None) } else { None })
        {
            let mut objects = self
                .query(
                    filter.clone(),
                    ckpt,
                    TimeRange::default(),
                    TimeRange::default(),
                    Order::default(),
                    None,
                )
                .await?;
            objects.reverse();
            for object in objects {
                let object = Arc::new(object);
                subscriber(&object, EventKind::Created);
            }
        }
        self.worker_tx
            .send(WorkerMessage::NewSub {
                id,
                filter,
                state: SubscriberState {
                    subscriber,
                    exclude_unrelated,
                },
            })
            .wrap()?;
        Ok(id)
    }

    pub fn unsubscribe(&self, id: Uuid) -> Result<()> {
        info!(%id, "unsubscribe");
        self.worker_tx
            .send(WorkerMessage::RemoveSub { id })
            .wrap()?;
        Ok(())
    }
}

impl Novi {
    async fn apply_rules_inner(&self, rule_set: &RuleSet, rules: &[Rule]) -> Result<()> {
        session::check_perm("rule.modify")?;

        let mut q = query_unsatisfied(&rules);
        q.add_select("*");

        let mut tr = self.db.begin().await?;
        let (sql, args) = q.build();
        let mut objects = sqlx::query_with(&sql, args)
            .fetch_all(&self.db)
            .await?
            .into_iter()
            .map(Object::from_row)
            .collect::<Result<Vec<_>>>()?;
        for object in &mut objects {
            if rule_set.closure_inner(object, Utc::now(), rules.iter()) {
                object.save().execute(tr.as_mut()).await?;
            }
        }
        tr.commit().await?;

        Ok(())
    }

    pub async fn apply_rules(&self, s: &str) -> Result<()> {
        let rules = parse_rules(s)?;
        self.apply_rules_inner(self.rule_set.read().await.deref(), &rules)
            .await
    }

    pub async fn add_rules(&self, s: &str, apply_old: bool) -> Result<Arc<Object>> {
        session::check_perm("rule.modify")?;

        let rules = parse_rules(s)?;
        info!(?rules, "add rules");

        let mut guard = self.rule_set.write().await;
        if apply_old {
            self.apply_rules_inner(&guard, &rules).await?;
        }

        let object = internal_scope(
            self.add_object_inner(
                [("@rule".to_owned(), Some(s.to_owned()))]
                    .into_iter()
                    .collect(),
                Some(&guard),
            ),
        )
        .await?;

        guard.insert_rule(object.id, rules);

        Ok(object)
    }

    pub async fn delete_rule(&self, id: Uuid) -> Result<()> {
        session::check_perm("rule.modify")?;

        let mut rule_set = self.rule_set.write().await;

        if !rule_set.delete_rule(id) {
            return Err(Self::object_not_found(id));
        }
        internal_scope(self.delete_object(id)).await?;

        Ok(())
    }

    pub async fn get_rules(&self) -> Result<BTreeMap<Uuid, String>> {
        session::check_perm("rule.list")?;

        Ok(self
            .query(
                Filter::Atom("@rule".to_owned(), FilterKind::Has),
                None,
                TimeRange::default(),
                TimeRange::default(),
                Order::default(),
                None,
            )
            .await?
            .into_iter()
            .map(|mut it| {
                (
                    it.id,
                    it.tags.remove("@rule").and_then(|it| it.value).unwrap(),
                )
            })
            .collect())
    }
}

impl Novi {
    pub async fn get_user(&self, id: Uuid) -> Arc<User> {
        self.user_cache
            .get_with(id, async move {
                let _guard = self.user_lock.read().await;
                Arc::new(
                    internal_scope(self.get_object(id))
                        .await
                        .unwrap()
                        .try_into()
                        .unwrap(),
                )
            })
            .await
    }

    pub async fn login(&self, name: &str, password: &str) -> Result<Arc<User>> {
        let id = sqlx::query_scalar!(
            "select id from object where tags->'@user.name'->>'v' = $1",
            name
        )
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| anyhow!(@InvalidCredentials))?;
        let user = self.get_user(id).await;
        user.verify(password)?;
        Ok(user)
    }

    pub async fn with_user<R>(&self, id: Uuid, f: impl Future<Output = R>) -> R {
        let user = self.get_user(id).await;
        session::enter(user, f).await
    }

    pub async fn register(&self, name: &str, password: &str) -> Result<Uuid> {
        if !(4..=20).contains(&name.chars().count()) {
            bail!(@InvalidInput "username length must be between 4 and 20");
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            bail!(
                @InvalidInput
                "username must only contain alphanumeric characters, '-' and '_"
            );
        }
        if !(6..=30).contains(&password.len()) {
            bail!(
                @InvalidInput
                "password length must be between 6 and 30"
            );
        }

        let salt = SaltString::generate(&mut OsRng);
        let password = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string();

        let _guard = self.user_lock.write().await;
        if sqlx::query_scalar!(
            "select 0 from object where tags->'@user.name'->>'v' = $1",
            name
        )
        .fetch_optional(&self.db)
        .await?
        .is_some()
        {
            bail!(@UsernameOccupied);
        }

        let object = internal_scope(self.add_model(&User::new(name.to_owned(), password))).await?;

        Ok(object.id)
    }
}

impl Novi {
    async fn get_tags(&self) -> Result<Vec<Tag>> {
        self.query(
            Filter::Atom("@tag".to_owned(), FilterKind::Has),
            None,
            TimeRange::default(),
            TimeRange::default(),
            Order::default(),
            None,
        )
        .await?
        .into_iter()
        .map(|it| it.try_into())
        .collect()
    }

    pub async fn get_tag(&self, tag: &str) -> Option<Arc<Tag>> {
        self.tags.read().await.get(tag).map(Arc::clone)
    }

    // Requires write guard
    async fn get_tag_or_insert_inner(&self, tag: &str, fail_if_exists: bool) -> Result<Arc<Tag>> {
        if let Some(tag) = self.get_tag(tag).await {
            if fail_if_exists {
                bail!(@TagExists);
            }
            return Ok(tag);
        }

        let mut tags = self.tags.write().await;
        if let Some(tag) = tags.get(tag) {
            if fail_if_exists {
                bail!(@TagExists);
            }
            return Ok(Arc::clone(tag));
        }

        session::check_perm("object.create")?;

        let id = internal_scope(self.add_model(&Tag::new(tag.to_owned())))
            .await?
            .id;

        let tag = Arc::new(Tag {
            id,
            name: tag.to_owned(),

            implies: String::new(),

            tags: BTreeMap::new(),
        });
        tags.insert(tag.name.clone(), Arc::clone(&tag));

        Ok(tag)
    }

    #[inline]
    pub async fn get_tag_or_insert(&self, tag: &str) -> Result<Arc<Tag>> {
        self.get_tag_or_insert_inner(tag, false).await
    }

    #[inline]
    pub async fn add_tag(&self, tag: &str) -> Result<Arc<Tag>> {
        self.get_tag_or_insert_inner(tag, true).await
    }
}

impl Novi {
    pub async fn call(
        &self,
        name: &str,
        args: RpcArgs,
        timeout: Option<Duration>,
    ) -> Result<serde_json::Value> {
        debug!(name, ?args, ?timeout, "rpc");
        let Some(provider) = self.rpc_providers.get(name) else {
            bail!(@RpcNotFound "rpc {name:?} not found");
        };
        let future = (provider.1)(name, args);

        let res = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, future)
                .await
                .ok()
                .ok_or_else(|| anyhow!(@RpcTimeout))??
        } else {
            future.await?
        };

        Ok(res)
    }

    pub fn register_rpc(&self, name: &str, provider: RpcProvider) -> Result<()> {
        session::check_perm("rpc.register")?;

        info!(name, "register rpc");
        if let dashmap::mapref::entry::Entry::Vacant(vacant) =
            self.rpc_providers.entry(name.to_owned())
        {
            vacant.insert((session::user_id(), provider));
            Ok(())
        } else {
            bail!(@RpcConflict "rpc {name:?} already registered");
        }
    }

    pub fn unregister_rpc(&self, name: &str) -> Result<()> {
        session::check_perm("rpc.register")?;

        if self
            .rpc_providers
            .remove_if(name, |_, rpc| rpc.0 == session::user_id())
            .is_none()
        {
            bail!(@PermissionDenied "only registrant can unregister rpc");
        }

        Ok(())
    }
}

#[derive(Serialize)]
pub struct MarkdownElementMeta {
    #[serde(rename = "@")]
    ty: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "@res")]
    res: Option<String>,
}
#[derive(Serialize)]
pub struct RenderedMarkdown {
    content: String,
    elements: BTreeMap<Uuid, MarkdownElementMeta>,
}

impl Novi {
    pub async fn render_markdown(&self, id: Uuid) -> Result<RenderedMarkdown> {
        let obj = self.get_object(id).await?;
        if obj.get("@") != Some("text") {
            bail!(@InvalidObject "object {id} is not a text object");
        }

        let content = std::fs::read_to_string(format!("storage/{id}"))?;
        let element_ids = content
            .lines()
            .filter_map::<Uuid, _>(|it| it.strip_prefix("[[")?.strip_suffix("]]")?.parse().ok())
            .collect::<Vec<_>>();

        let mut tr = self.db.begin().await?;
        let mut elements = BTreeMap::new();
        for id in element_ids {
            let resp = sqlx::query!(
                "select tags->'@'->>'v' as ty, tags->'@res'->>'v' as res from object where id = $1",
                id
            )
            .fetch_optional(tr.as_mut())
            .await?;
            if let Some(resp) = resp {
                elements.insert(
                    id,
                    MarkdownElementMeta {
                        ty: resp.ty,
                        res: resp.res,
                    },
                );
            }
        }

        Ok(RenderedMarkdown { content, elements })
    }
}
