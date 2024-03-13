pub mod client;
pub mod server;

use crate::{session::internal_scope, ErrorKind, Result, TagValue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use interprocess::local_socket::tokio as ipc;
use pyo3::{types::IntoPyDict, PyResult, Python};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    sync::Arc,
};
use tokio::sync::{oneshot, Mutex, Semaphore};
use tracing::error;
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

type RawResp = Result<Vec<u8>, PlainError>;

#[derive(Serialize, Deserialize, Debug)]
enum Message<E> {
    Execute(u32, E),
    Resp(u32, RawResp),
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

    fn remove(&mut self, i: usize) -> Option<T> {
        self.count -= 1;
        let res = self.slots[i].take();
        self.free.push(i);
        res
    }
}

#[async_trait]
pub trait Close {
    async fn close(&self);
}

pub struct IpcSocket<E: Execute> {
    tx: Mutex<ipc::OwnedWriteHalf>,
    callbacks: Mutex<Arena<oneshot::Sender<RawResp>>>,
    pub context: E::Context,
    semaphore: Semaphore,
    _name: String,
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
            _name: name,
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

                    match msg {
                        Message::Execute(id, e) => {
                            let this = Arc::clone(&this);
                            tokio::spawn(internal_scope(async move {
                                let _permit = this.semaphore.acquire().await.unwrap();
                                this.dispatch(id, e).await;
                            }));
                        }
                        Message::Resp(id, res) => {
                            let tx = this.callbacks.lock().await.remove(id as usize).unwrap();
                            tx.send(res).unwrap();
                        }
                    }
                }
            }
        });

        this
    }

    async fn dispatch(self: &Arc<Self>, id: u32, e: E) {
        // TODO print error
        let res = e.execute(self).await;
        let msg = Message::<()>::Resp(id, res.map_err(|err| (&err).into()));
        let buf = postcard::to_allocvec(&msg).unwrap();
        async {
            let mut tx = self.tx.lock().await;
            tx.write_all(&(buf.len() as u32).to_le_bytes()).await?;
            tx.write_all(&buf).await
        }
        .await
        .expect("failed to write");
    }

    pub async fn invoke<R: DeserializeOwned, Req: Serialize>(
        self: &Arc<Self>,
        req: Req,
    ) -> Result<R, E::Error> {
        let (tx, rx) = oneshot::channel();
        let id = self.callbacks.lock().await.insert(tx) as u32;
        let buf = postcard::to_allocvec(&Message::Execute(id, req)).unwrap();

        async {
            let mut tx = self.tx.lock().await;
            tx.write_all(&(buf.len() as u32).to_le_bytes()).await?;
            tx.write_all(&buf).await
        }
        .await
        .expect("failed to write");

        let resp = rx.await.expect("failed to receive");
        match resp {
            Ok(data) => Ok(postcard::from_bytes(&data).unwrap()),
            Err(err) => Err(err.into()),
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
