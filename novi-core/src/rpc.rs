pub mod client;
pub mod server;

use crate::{user::internal_scope, ErrorKind, Result, TagValue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use interprocess::local_socket::tokio as ipc;
use pyo3::{types::IntoPyDict, PyResult, Python};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{mpsc, Mutex, Semaphore},
    task_local,
};
use tracing::{error, warn};
use uuid::Uuid;

pub mod compact_date_time {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(val: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let tuple = (val.timestamp(), val.timestamp_subsec_nanos());
        tuple.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (secs, nsecs) = <(i64, u32)>::deserialize(deserializer)?;
        Ok(Utc.timestamp_opt(secs, nsecs).unwrap())
    }
}
pub mod compact_tags {
    use crate::TagValue;
    use chrono::{DateTime, Utc};
    use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    #[derive(Serialize, Deserialize)]
    struct TagValueCompact<'a> {
        value: Option<&'a str>,
        #[serde(with = "super::compact_date_time")]
        updated: DateTime<Utc>,
    }

    pub fn serialize<S>(val: &BTreeMap<String, TagValue>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(val.len()))?;
        for (k, v) in val {
            map.serialize_entry(
                k,
                &TagValueCompact {
                    value: v.value.as_deref(),
                    updated: v.updated,
                },
            )?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, TagValue>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = BTreeMap<String, TagValue>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut result = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    let TagValueCompact { value, updated } = value;
                    result.insert(
                        key,
                        TagValue {
                            value: value.map(|it| it.to_string()),
                            updated,
                        },
                    );
                }
                Ok(result)
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

// we need to flatten the object to be able to serialize it
#[derive(Serialize, Deserialize, Debug)]
pub struct FlattenedObject {
    pub id: Uuid,
    #[serde(with = "compact_tags")]
    pub tags: BTreeMap<String, TagValue>,

    pub creator: Option<Uuid>,
    #[serde(with = "compact_date_time")]
    pub created: DateTime<Utc>,
    #[serde(with = "compact_date_time")]
    pub updated: DateTime<Utc>,
}
impl From<crate::Object> for FlattenedObject {
    fn from(obj: crate::Object) -> Self {
        Self {
            id: obj.id,
            tags: obj.tags,

            creator: obj.meta.creator,
            created: obj.meta.created,
            updated: obj.meta.updated,
        }
    }
}
impl From<Arc<crate::Object>> for FlattenedObject {
    fn from(obj: Arc<crate::Object>) -> Self {
        obj.as_ref().clone().into()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PlainError {
    kind: ErrorKind,
    message: String,
}
impl Display for PlainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error {:?}: {}", self.kind, self.message)
    }
}
impl From<&crate::Error> for PlainError {
    fn from(err: &crate::Error) -> Self {
        Self {
            kind: err.kind(),
            message: err.to_string(),
        }
    }
}
impl From<PlainError> for crate::Error {
    fn from(err: PlainError) -> Self {
        crate::Error::msg(err.message).with_kind(err.kind)
    }
}

#[async_trait]
pub trait Execute: Sized {
    type Context;
    type Error;

    fn format_error(err: &Self::Error) -> String;
    async fn execute(self, socket: &Arc<IpcSocket<Self>>) -> Result<Vec<u8>, Self::Error>;
}

#[derive(Serialize, Deserialize, Debug)]
enum ExecuteOrResp<E> {
    Execute(E),
    Resp(Vec<u8>),
    Error(PlainError),
}
#[derive(Serialize, Deserialize, Debug)]
struct Message<E> {
    issue: bool,
    id: u32,
    data: ExecuteOrResp<E>,
}

struct Arena<T> {
    slots: Vec<Option<T>>,
    free: Vec<usize>,
    count: u32,
}
impl<T> Arena<T> {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            count: 0,
        }
    }

    fn insert(&mut self, value: T) -> usize {
        self.count += 1;
        if let Some(i) = self.free.pop() {
            self.slots[i] = Some(value);
            i
        } else {
            self.slots.push(Some(value));
            self.slots.len() - 1
        }
    }

    fn remove(&mut self, i: usize) {
        self.count -= 1;
        self.slots[i] = None;
        self.free.push(i);
    }
}

struct CallbackState<E> {
    sender: Option<IpcSender<E>>,
    other_id: Option<u32>,
    panicked: bool,
}

fn load_id(id: &AtomicU32) -> Option<u32> {
    let id = id.load(Ordering::SeqCst);
    if id == u32::MAX {
        None
    } else {
        Some(id)
    }
}

#[async_trait]
pub trait Close {
    async fn close(&self);
}

task_local! {
    static CURRENT_ID: AtomicU32;
    static OTHER_ID: u32;
}
type IpcSender<E> = mpsc::Sender<Message<E>>;
pub struct IpcSocket<E: Execute> {
    tx: Mutex<ipc::OwnedWriteHalf>,
    callbacks: Mutex<Arena<CallbackState<E>>>,
    pub context: E::Context,
    semaphore: Semaphore,
    name: String,
}
impl<E> IpcSocket<E>
where
    E: Execute + DeserializeOwned + Send + 'static,
    E::Context: Send + Sync + Close + 'static,
    E::Error: Send + Sync + From<PlainError> + 'static,
    for<'a> &'a E::Error: Into<PlainError>,
{
    pub fn new(
        stream: ipc::LocalSocketStream,
        context: E::Context,
        max_concurrent: usize,
        name: String,
    ) -> Arc<Self> {
        let (mut rx, tx) = stream.into_split();
        let this = Arc::new(Self {
            tx: Mutex::new(tx),
            callbacks: Mutex::new(Arena::new()),
            context,
            semaphore: Semaphore::new(max_concurrent),
            name,
        });
        tokio::spawn({
            let this = Arc::clone(&this);
            async move {
                // TODO optimize protocol
                let mut length = [0; 4];
                let mut buf = Vec::new();
                loop {
                    if rx.read_exact(&mut length).await.is_err() {
                        this.context.close().await;
                        break;
                    }
                    let length = u32::from_le_bytes(length) as usize;

                    buf.resize(length, 0);
                    rx.read_exact(&mut buf).await.expect("failed to read");
                    let msg: Message<E> = postcard::from_bytes(&buf).unwrap();

                    // println!("{} recv {msg:?}", this.name);
                    // if let ExecuteOrResp::Resp(res) = &msg.data {
                    // println!("length {}", res.len());
                    // }

                    if msg.issue {
                        match msg.data {
                            ExecuteOrResp::Execute(e) => {
                                // TODO error handling
                                let this = Arc::clone(&this);
                                tokio::spawn(internal_scope(async move {
                                    let _permit = this.semaphore.acquire().await.unwrap();
                                    if let Err(err) = CURRENT_ID
                                        .scope(
                                            AtomicU32::new(u32::MAX),
                                            OTHER_ID.scope(msg.id, this.dispatch(msg.id as _, e)),
                                        )
                                        .await
                                    {
                                        warn!(
                                            name = this.name,
                                            err = E::format_error(&err),
                                            "failed to execute"
                                        );
                                    }
                                }));
                            }
                            _ => {
                                panic!("unexpected response");
                            }
                        }
                    } else {
                        let mut cb = this.callbacks.lock().await;
                        let state = &mut cb.slots[msg.id as usize].as_mut().unwrap();
                        if state.other_id.is_none() && matches!(msg.data, ExecuteOrResp::Execute(_))
                        {
                            let mut bytes = [0; 4];
                            rx.read_exact(&mut bytes).await.expect("failed to read");
                            state.other_id = Some(u32::from_le_bytes(bytes));
                            // println!(
                            // "{} knows the other is {}",
                            // this.name,
                            // u32::from_le_bytes(bytes)
                            // );
                        }
                        if state.sender.as_ref().unwrap().send(msg).await.is_err() {
                            warn!(name = this.name, "channel closed");
                        }
                    }
                }
            }
        });

        this
    }

    async fn dispatch(self: &Arc<Self>, id: u32, e: E) -> Result<(), E::Error> {
        let res = e.execute(self).await;
        match CURRENT_ID.with(load_id) {
            Some(id) => {
                let mut cb = self.callbacks.lock().await;
                let state = cb.slots[id as usize].as_mut().unwrap();
                if state.sender.is_none() {
                    cb.remove(id as usize);
                }
            }
            None => {}
        }
        let (data, res) = match res {
            Ok(data) => (ExecuteOrResp::Resp(data), Ok(())),
            Err(err) => {
                let need_to_send = match CURRENT_ID.with(load_id) {
                    Some(id) => {
                        let mut cb = self.callbacks.lock().await;
                        let state = cb.slots[id as usize].as_mut().unwrap();
                        if !state.panicked {
                            state.panicked = true;
                            true
                        } else {
                            false
                        }
                    }
                    None => true,
                };

                if need_to_send {
                    (ExecuteOrResp::Error((&err).into()), Err(err))
                } else {
                    return Err(err);
                }
            }
        };
        let msg = Message::<()> {
            issue: false,
            id,
            data,
        };
        let buf = postcard::to_allocvec(&msg).unwrap();
        async {
            let mut tx = self.tx.lock().await;
            tx.write_all(&(buf.len() as u32).to_le_bytes()).await?;
            tx.write_all(&buf).await
        }
        .await
        .expect("failed to write");

        res
    }

    async fn invoke_inner<R: DeserializeOwned, Req: Serialize>(
        self: &Arc<Self>,
        req: Req,
    ) -> Result<R, E::Error> {
        let (tx, mut rx) = mpsc::channel(16);
        // issue: old_sender & other_id is None
        // send + inform: old_sender is None while other_id is Some
        // send: old_sender is Some and other_id is Some
        let (id, old_sender, mut other_id) = match CURRENT_ID.with(load_id) {
            Some(id) => {
                let mut cb = self.callbacks.lock().await;
                let state = &mut cb.slots[id as usize].as_mut().unwrap();
                let old_sender = std::mem::replace(&mut state.sender, Some(tx));
                (id, old_sender, state.other_id)
            }
            None => {
                let other_id = OTHER_ID.try_with(|it| *it).ok();
                let id = self.callbacks.lock().await.insert(CallbackState {
                    sender: Some(tx),
                    other_id,
                    panicked: false,
                }) as u32;
                CURRENT_ID.with(|it| it.store(id, Ordering::SeqCst));
                (id, None, other_id)
            }
        };
        let is_issue = old_sender.is_none() && other_id.is_none();

        let buf = postcard::to_allocvec(&Message {
            issue: other_id.is_none(),
            id: other_id.unwrap_or(id),
            data: ExecuteOrResp::Execute(req),
        })
        .unwrap();

        async {
            let mut tx = self.tx.lock().await;
            tx.write_all(&(buf.len() as u32).to_le_bytes()).await?;
            tx.write_all(&buf).await?;
            if old_sender.is_none() && other_id.is_some() {
                tx.write_all(&id.to_le_bytes()).await?;
            }
            std::io::Result::Ok(())
        }
        .await
        .expect("failed to write");

        loop {
            let msg = rx.recv().await.unwrap();
            match msg.data {
                ExecuteOrResp::Resp(res) => {
                    if is_issue {
                        let mut cb = self.callbacks.lock().await;
                        if let Some(old_sender) = old_sender {
                            cb.slots[id as usize].as_mut().unwrap().sender = Some(old_sender);
                        } else {
                            cb.remove(id as usize);
                        }
                    }
                    break Ok(postcard::from_bytes(&res).unwrap());
                }
                ExecuteOrResp::Execute(e) => {
                    if other_id.is_none() {
                        other_id = self.callbacks.lock().await.slots[id as usize]
                            .as_ref()
                            .unwrap()
                            .other_id;
                    }
                    self.dispatch(other_id.unwrap(), e).await?;
                }
                ExecuteOrResp::Error(plain) => {
                    self.callbacks.lock().await.slots[id as usize]
                        .as_mut()
                        .unwrap()
                        .panicked = true;
                    return Err(plain.into());
                }
            }
        }
    }

    pub async fn invoke<R: DeserializeOwned, Req: Serialize>(
        self: &Arc<Self>,
        req: Req,
    ) -> Result<R, E::Error> {
        if CURRENT_ID.try_with(|_| ()).is_err() {
            CURRENT_ID
                .scope(AtomicU32::new(u32::MAX), self.invoke_inner(req))
                .await
        } else {
            self.invoke_inner(req).await
        }
    }
}

pub async fn sub_main() {
    let plugin_name = std::env::args().nth(2).unwrap();

    let stream = ipc::LocalSocketStream::connect("@novi").await.unwrap();
    let socket = IpcSocket::<client::Command>::new(
        stream,
        client::ChildContext,
        32,
        format!("plugin-{plugin_name}"),
    );

    let result = Python::with_gil(|py| {
        crate::py::init(py, &plugin_name, socket)?;

        let runpy = py.import("runpy")?;
        let run_path = runpy.getattr("run_path")?;
        run_path.call((".",), Some([("run_name", &plugin_name)].into_py_dict(py)))?;

        PyResult::Ok(())
    });

    if let Err(err) = result {
        error!(?err, plugin = plugin_name, "failed to run python");
        Python::with_gil(|py| err.print_and_set_sys_last_vars(py));
    }

    std::future::pending::<()>().await;
}
