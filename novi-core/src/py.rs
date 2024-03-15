use crate::{
    ipc::{
        client,
        server::{Command, RawCommand},
        FlattenedObject, IpcSocket,
    },
    log::LOGGER,
    Object, ObjectMeta, TagValue, Tags, ROOT_PATH,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{IntoPyDict, PyDict},
};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use tokio::runtime::Handle;
use tracing::Level;
use uuid::Uuid;

fn block_on<F: Future>(fut: F) -> F::Output {
    tokio::task::block_in_place(|| Handle::current().block_on(fut))
}

#[pyclass(module = "novi")]
struct BoxedFuture(Option<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send>>>);
impl BoxedFuture {
    fn take(&mut self) -> Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send>> {
        self.0.take().expect("future already polled")
    }
}
#[pymethods]
impl BoxedFuture {
    fn block(&mut self, py: Python) -> PyResult<PyObject> {
        let fut = self.take();
        py.allow_threads(move || block_on(fut))
    }

    fn coroutine<'py>(&mut self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let fut = self.take();
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }
}

#[pyclass(module = "novi")]
pub struct ObjectImpl(Object);
impl ObjectImpl {
    pub fn from_flattened(object: FlattenedObject) -> Self {
        Self(Object {
            id: object.id,
            tags: object.tags,
            meta: ObjectMeta {
                creator: object.creator,
                created: object.created,
                updated: object.updated,
            },
        })
    }
}
#[pymethods]
impl ObjectImpl {
    fn id(&self) -> String {
        self.0.id.to_string()
    }

    fn creator(&self) -> Option<String> {
        self.0.meta.creator.map(|it| it.to_string())
    }

    fn created(&self) -> DateTime<Utc> {
        self.0.meta.created
    }

    fn updated(&self) -> DateTime<Utc> {
        self.0.meta.updated
    }

    fn tags(&self) -> BTreeMap<String, TagValue> {
        self.0.tags.clone()
    }

    fn get(&self, key: &str) -> PyResult<TagValue> {
        self.0
            .tags()
            .get(key)
            .cloned()
            .ok_or_else(|| PyKeyError::new_err("tag not found"))
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> &'py PyDict {
        [
            ("tags", self.0.tags.clone().into_py(py)),
            ("id", self.0.id.to_string().into_py(py)),
            (
                "creator",
                self.0.meta.creator.map(|it| it.to_string()).into_py(py),
            ),
            ("created", self.0.meta.created.into_py(py)),
            ("updated", self.0.meta.updated.into_py(py)),
        ]
        .into_py_dict(py)
    }

    fn to_simple_json(&self) -> String {
        #[derive(Serialize)]
        struct SimpleObject<'a> {
            id: Uuid,
            tags: BTreeMap<&'a str, Option<&'a str>>,
            creator: Option<Uuid>,
            created: DateTime<Utc>,
            updated: DateTime<Utc>,
        }
        serde_json::to_string(&SimpleObject {
            id: self.0.id,
            tags: self
                .0
                .tags
                .iter()
                .map(|it| (it.0.as_str(), it.1.value.as_deref()))
                .collect(),
            creator: self.0.meta.creator,
            created: self.0.meta.created,
            updated: self.0.meta.updated,
        })
        .unwrap()
    }
}

struct PyUuid(Uuid);
impl FromPyObject<'_> for PyUuid {
    fn extract(obj: &PyAny) -> PyResult<Self> {
        let s: String = obj.extract()?;
        Uuid::parse_str(&s)
            .map(PyUuid)
            .map_err(|_| PyValueError::new_err("invalid UUID"))
    }
}

struct State {
    socket: Arc<IpcSocket<client::Command>>,
    subscribe_callbacks: DashMap<Uuid, PyObject>,
    rpc_callbacks: DashMap<String, PyObject>,
}
impl State {
    fn new(socket: Arc<IpcSocket<client::Command>>) -> Self {
        Self {
            socket,
            subscribe_callbacks: DashMap::new(),
            rpc_callbacks: DashMap::new(),
        }
    }

    async fn invoke_raw<T: Send + DeserializeOwned>(
        &self,
        session: Option<Uuid>,
        command: RawCommand,
    ) -> PyResult<T> {
        let socket = self.socket.clone();
        socket.invoke(Command { session, command }).await
    }

    fn invoke<T: Send + DeserializeOwned + 'static>(
        &self,
        session: Option<Uuid>,
        command: RawCommand,
        mapper: impl FnOnce(Python, T) -> PyResult<PyObject> + Send + 'static,
    ) -> BoxedFuture {
        let socket = self.socket.clone();
        BoxedFuture(Some(Box::pin(async move {
            socket
                .invoke(Command { session, command })
                .await
                .and_then(|it| Python::with_gil(|py| mapper(py, it)))
        })))
    }
}

#[pyclass(module = "novi")]
struct Core(Arc<State>);
#[pymethods]
impl Core {
    fn login(&self, name: String, password: String) -> BoxedFuture {
        let state = self.0.clone();
        self.0.invoke(
            None,
            RawCommand::Login { name, password },
            move |py, id: Uuid| {
                Ok(ClientImpl {
                    state,
                    session: Some(id),
                }
                .into_py(py))
            },
        )
    }

    fn login_by_token(&self, token: String) -> BoxedFuture {
        let state = self.0.clone();
        self.0.invoke(
            None,
            RawCommand::LoginByToken(token),
            move |py, id: Uuid| {
                Ok(ClientImpl {
                    state,
                    session: Some(id),
                }
                .into_py(py))
            },
        )
    }
}

#[pyclass(module = "novi")]
struct ClientImpl {
    state: Arc<State>,
    session: Option<Uuid>,
}
impl ClientImpl {
    #[inline]
    fn invoke<T: Send + DeserializeOwned + 'static>(
        &self,
        command: RawCommand,
        mapper: impl FnOnce(Python, T) -> PyResult<PyObject> + Send + 'static,
    ) -> BoxedFuture {
        self.state.invoke(self.session, command, mapper)
    }

    fn invoke_return_object(&self, command: RawCommand) -> BoxedFuture {
        self.invoke(command, |py, obj: FlattenedObject| {
            Ok(ObjectImpl::from_flattened(obj).into_py(py))
        })
    }
}
#[pymethods]
impl ClientImpl {
    fn add_object(&self, tags: Tags) -> BoxedFuture {
        self.invoke_return_object(RawCommand::AddObject(tags))
    }

    fn get_object(&self, id: &str) -> PyResult<BoxedFuture> {
        Ok(self.invoke_return_object(RawCommand::GetObject(
            Uuid::parse_str(id).map_err(|_| PyValueError::new_err("invalid UUID"))?,
        )))
    }

    fn set_object_tags(&self, id: PyUuid, tags: Tags, force_update: bool) -> BoxedFuture {
        self.invoke_return_object(RawCommand::SetObjectTags {
            id: id.0,
            tags,
            force_update,
        })
    }

    fn delete_object_tag(&self, id: PyUuid, tag: String) -> BoxedFuture {
        self.invoke_return_object(RawCommand::DeleteObjectTag(id.0, tag))
    }

    fn delete_object(&self, id: PyUuid) -> BoxedFuture {
        self.invoke(RawCommand::DeleteObject(id.0), |py, _: ()| Ok(py.None()))
    }

    #[pyo3(signature = (filter, checkpoint, updated_after, updated_before, created_after, created_before, order, limit))]
    fn query(
        &self,
        filter: &str,
        checkpoint: Option<DateTime<Utc>>,
        updated_after: Option<DateTime<Utc>>,
        updated_before: Option<DateTime<Utc>>,
        created_after: Option<DateTime<Utc>>,
        created_before: Option<DateTime<Utc>>,
        order: &str,
        limit: Option<u32>,
    ) -> PyResult<BoxedFuture> {
        Ok(self.invoke(
            RawCommand::Query {
                filter: filter.to_owned(),
                checkpoint,
                updated_range: (updated_after, updated_before),
                created_range: (created_after, created_before),
                order: order.parse()?,
                limit,
            },
            |py, objs: Vec<FlattenedObject>| {
                Ok(objs
                    .into_iter()
                    .map(ObjectImpl::from_flattened)
                    .collect::<Vec<_>>()
                    .into_py(py))
            },
        ))
    }

    #[pyo3(signature = (filter, callback, checkpoint, with_history, exclude_unrelated))]
    fn subscribe(
        &self,
        filter: &str,
        callback: PyObject,
        checkpoint: Option<DateTime<Utc>>,
        with_history: bool,
        exclude_unrelated: bool,
    ) -> BoxedFuture {
        let state = self.state.clone();
        self.invoke(
            RawCommand::Subscribe {
                filter: filter.to_owned(),
                callback: callback.as_ptr() as _,
                checkpoint,
                with_history,
                exclude_unrelated,
            },
            move |py, id: Uuid| {
                state.subscribe_callbacks.insert(id, callback);
                Ok(id.to_string().into_py(py))
            },
        )
    }

    fn unsubscribe(&self, id: PyUuid) -> BoxedFuture {
        let state = self.state.clone();
        self.invoke(RawCommand::Unsubscribe(id.0), move |py, _: ()| {
            state.subscribe_callbacks.remove(&id.0);
            Ok(py.None())
        })
    }

    fn call<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        args: &PyDict,
        timeout: Option<f64>,
    ) -> PyResult<BoxedFuture> {
        // TODO optimize
        let args: String = py
            .import("json")?
            .getattr("dumps")?
            .call1((args,))?
            .extract()?;
        Ok(self.invoke(
            RawCommand::Call {
                name: name.to_owned(),
                args,
                timeout,
            },
            |py, resp: String| {
                py.import("json")?
                    .getattr("loads")?
                    .call1((resp,))
                    .map(|obj| obj.into_py(py))
            },
        ))
    }

    fn register_rpc(&self, name: String, callback: PyObject) -> BoxedFuture {
        let state = self.state.clone();
        self.invoke(
            RawCommand::RegisterRpc {
                name: name.clone(),
                callback: callback.as_ptr() as _,
            },
            move |py, _: ()| {
                state.rpc_callbacks.insert(name, callback);
                Ok(py.None())
            },
        )
    }

    fn unregister_rpc(&self, name: String) -> BoxedFuture {
        let state = self.state.clone();
        self.invoke(RawCommand::UnregisterRpc(name.clone()), move |py, _: ()| {
            state.rpc_callbacks.remove(&name);
            Ok(py.None())
        })
    }

    fn root_path(&self) -> PyResult<&str> {
        Ok(ROOT_PATH.to_str().unwrap())
    }

    fn user_id(&self) -> PyResult<Option<String>> {
        block_on(self.state.invoke_raw(self.session, RawCommand::GetUserId))
            .map(|it: Option<Uuid>| it.map(|it| it.to_string()))
    }

    fn gen_token(&self) -> PyResult<String> {
        block_on(self.state.invoke_raw(self.session, RawCommand::GenToken))
    }
}

#[pyclass(module = "novi")]
struct LogHandler(String);
#[pymethods]
impl LogHandler {
    fn emit(&self, record: &PyAny) -> PyResult<()> {
        let level = record.getattr("levelno")?;
        let level = if level.ge(40u8)? {
            Level::ERROR
        } else if level.ge(30u8)? {
            Level::WARN
        } else if level.ge(20u8)? {
            Level::INFO
        } else if level.ge(10u8)? {
            Level::DEBUG
        } else {
            Level::TRACE
        };

        let name: String = record.getattr("name")?.extract()?;
        let mut target = format!("plugin::{}", self.0);
        if name != "root" {
            target.push_str("::");
            target.push_str(&name);
        }

        let message: String = record.getattr("getMessage")?.call0()?.extract()?;

        let file: String = record.getattr("filename")?.extract()?;
        let lineno = record.getattr("lineno")?;

        LOGGER.log(
            level,
            target,
            Some(message),
            vec![("file", file), ("line", lineno.to_string())],
        );

        Ok(())
    }
}

pub fn init(
    py: Python<'_>,
    plugin_name: &str,
    secret_key: Uuid,
    socket: Arc<IpcSocket<client::Command>>,
) -> PyResult<()> {
    let core = Core(Arc::new(State::new(socket)));
    block_on(core.0.invoke_raw::<()>(
        None,
        RawCommand::Init {
            plugin_name: plugin_name.to_owned(),
            secret_key,
        },
    ))?;

    let builtins = py.import("builtins")?;
    builtins.setattr(
        "_client",
        ClientImpl {
            state: core.0.clone(),
            session: Some(Uuid::nil()),
        }
        .into_py(py),
    )?;
    builtins.setattr(
        "_guest_client",
        ClientImpl {
            state: core.0.clone(),
            session: None,
        }
        .into_py(py),
    )?;
    builtins.setattr("_core", core.into_py(py))?;

    let path = ROOT_PATH.join("novi.py");
    let m = PyModule::from_code(
        py,
        &std::fs::read_to_string(&path).expect("failed to load novi.py"),
        &path.display().to_string(),
        "novi",
    )?;
    builtins.delattr("_client")?;
    builtins.delattr("_guest_client")?;
    builtins.delattr("_core")?;

    py.run(
        r#"
import logging
class NoviLogHandler(logging.StreamHandler):
    def __init__(self, impl):
        super().__init__()
        self.impl = impl

    def emit(self, record):
        self.impl.emit(record)

logging.basicConfig(handlers=[NoviLogHandler(impl)])
"#,
        Some([("impl", LogHandler(plugin_name.to_owned()).into_py(py))].into_py_dict(py)),
        None,
    )?;

    let sys = py.import("sys")?;
    let py_modules: &PyDict = sys.getattr("modules")?.downcast()?;
    py_modules.set_item("novi", m)?;

    Ok(())
}
