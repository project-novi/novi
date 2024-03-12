// TODO async version

use crate::{
    log::LOGGER,
    rpc::{client, server::Command, FlattenedObject, IpcSocket},
    Object, ObjectMeta, TagValue, Tags, ROOT_PATH,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{IntoPyDict, PyDict},
};
use serde::de::DeserializeOwned;
use std::{collections::BTreeMap, sync::Arc};
use tokio::runtime::Handle;
use tracing::Level;
use uuid::Uuid;

#[pyclass]
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

    fn to_json(&self) -> String {
        serde_json::to_string(&self.0).unwrap()
    }
}

struct PyUuid(Uuid);
impl FromPyObject<'_> for PyUuid {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Uuid::parse_str(&s)
            .map(PyUuid)
            .map_err(|_| PyValueError::new_err("invalid UUID"))
    }
}

#[pyclass]
struct ClientImpl {
    socket: Arc<IpcSocket<client::Command>>,
    subscribe_callbacks: DashMap<Uuid, PyObject>,
    rpc_callbacks: DashMap<String, PyObject>,
}
impl ClientImpl {
    fn new(socket: Arc<IpcSocket<client::Command>>) -> Self {
        Self {
            socket,
            subscribe_callbacks: DashMap::new(),
            rpc_callbacks: DashMap::new(),
        }
    }

    fn invoke<T: Send + DeserializeOwned>(&self, cmd: Command) -> PyResult<T> {
        let py = unsafe { Python::assume_gil_acquired() };
        let socket = self.socket.clone();
        py.allow_threads(move || {
            tokio::task::block_in_place(|| {
                Handle::current().block_on(async { socket.invoke(cmd).await })
            })
        })
    }
}
#[pymethods]
impl ClientImpl {
    fn add_object(&self, tags: Tags) -> PyResult<ObjectImpl> {
        self.invoke(Command::AddObject(tags))
            .map(ObjectImpl::from_flattened)
    }

    fn get_object(&self, id: &str) -> PyResult<ObjectImpl> {
        self.invoke(Command::GetObject(
            Uuid::parse_str(id).map_err(|_| PyValueError::new_err("invalid UUID"))?,
        ))
        .map(ObjectImpl::from_flattened)
    }

    fn set_object_tags(&self, id: PyUuid, tags: Tags, force_update: bool) -> PyResult<ObjectImpl> {
        self.invoke(Command::SetObjectTags {
            id: id.0,
            tags,
            force_update,
        })
        .map(ObjectImpl::from_flattened)
    }

    fn delete_object(&self, id: PyUuid) -> PyResult<()> {
        self.invoke(Command::DeleteObject(id.0))
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
    ) -> PyResult<Vec<ObjectImpl>> {
        self.invoke(Command::Query {
            filter: filter.to_owned(),
            checkpoint,
            updated_range: (updated_after, updated_before),
            created_range: (created_after, created_before),
            order: order.parse()?,
            limit,
        })
        .map(|objs: Vec<FlattenedObject>| {
            objs.into_iter().map(ObjectImpl::from_flattened).collect()
        })
    }

    #[pyo3(signature = (filter, callback, checkpoint, with_history, exclude_unrelated))]
    fn subscribe(
        &self,
        filter: &str,
        callback: PyObject,
        checkpoint: Option<DateTime<Utc>>,
        with_history: bool,
        exclude_unrelated: bool,
    ) -> PyResult<String> {
        self.invoke::<Uuid>(Command::Subscribe {
            filter: filter.to_owned(),
            callback: callback.as_ptr() as _,
            checkpoint,
            with_history,
            exclude_unrelated,
        })
        .map(|it| {
            self.subscribe_callbacks.insert(it, callback);
            it.to_string()
        })
    }

    fn unsubscribe(&self, id: PyUuid) -> PyResult<()> {
        self.invoke::<()>(Command::Unsubscribe(id.0)).map(|_| {
            self.subscribe_callbacks.remove(&id.0);
        })
    }

    fn call<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        args: &PyDict,
        timeout: Option<f64>,
    ) -> PyResult<&'py PyAny> {
        // TODO optimize
        let json = py.import("json")?;
        let args: String = json.getattr("dumps")?.call1((args,))?.extract()?;
        let resp: String = self.invoke(Command::Call {
            name: name.to_owned(),
            args,
            timeout,
        })?;
        json.getattr("loads")?.call1((resp,))
    }

    fn register_rpc(&self, name: String, callback: PyObject) -> PyResult<()> {
        self.invoke::<()>(Command::RegisterRpc {
            name: name.clone(),
            callback: callback.as_ptr() as _,
        })
        .map(|_| {
            self.rpc_callbacks.insert(name, callback);
        })
    }

    fn unregister_rpc(&self, name: String) -> PyResult<()> {
        self.invoke::<()>(Command::UnregisterRpc(name.clone()))
            .map(|_| {
                self.rpc_callbacks.remove(&name);
            })
    }

    fn root_path(&self) -> PyResult<&str> {
        Ok(ROOT_PATH.to_str().unwrap())
    }
}

#[pyclass]
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
    socket: Arc<IpcSocket<client::Command>>,
) -> PyResult<()> {
    let builtins = py.import("builtins")?;
    builtins.setattr("_impl", ClientImpl::new(socket).into_py(py))?;

    let m = PyModule::from_code(
        py,
        &std::fs::read_to_string(ROOT_PATH.join("novi.py")).expect("failed to load novi.py"),
        "novi.py",
        "novi",
    )?;
    builtins.delattr("_impl")?;

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
