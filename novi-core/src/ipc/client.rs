use super::{Close, Execute, FlattenedObject, IpcSocket, PlainError};
use crate::{py::ObjectImpl, ErrorKind};
use async_trait::async_trait;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

impl From<&PyErr> for PlainError {
    fn from(e: &PyErr) -> Self {
        let message = Command::format_error(e);
        PlainError {
            kind: ErrorKind::PyError,
            message,
        }
    }
}
impl From<PlainError> for PyErr {
    fn from(e: PlainError) -> Self {
        Python::with_gil(move |py| {
            let error = py.import("novi")?.getattr("NoviError")?;
            error
                .call1((e.kind.error_code(), format!("{:?}", e.kind), e.message))
                .map(PyErr::from_value)
        })
        .unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    CallSubscribe {
        callback: u64,
        object: FlattenedObject,
        kind: String,
    },
    CallRpc {
        callback: u64,
        name: String,
        args: String, // json string
        session: u32,
    },
}

pub struct ChildContext;
#[async_trait]
impl Close for ChildContext {
    async fn close(&self) {
        std::process::exit(0);
    }
}
#[async_trait]
impl Execute for Command {
    type Context = ChildContext;
    type Error = PyErr;

    fn format_error(err: &Self::Error) -> String {
        Python::with_gil(|py| {
            PyResult::Ok(format!("{}{err}", err.traceback(py).unwrap().format()?))
        })
        .unwrap()
    }

    async fn execute(self, _socket: &Arc<IpcSocket<Self>>) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::CallSubscribe {
                callback,
                object,
                kind,
            } => tokio::task::spawn_blocking(move || {
                Python::with_gil(|py| {
                    let callback = unsafe { PyObject::from_borrowed_ptr(py, callback as _) };
                    callback.call1(py, (ObjectImpl::from_flattened(object), kind.to_string()))?;
                    Ok(Vec::new())
                })
            })
            .await
            .unwrap(),
            Self::CallRpc {
                callback,
                name,
                args,
                session,
            } => tokio::task::spawn_blocking(move || {
                Python::with_gil(|py| {
                    let callback = unsafe { PyObject::from_borrowed_ptr(py, callback as _) };
                    let json = py.import("json")?;
                    // TODO optimize
                    let args = json.getattr("loads")?.call1((args,))?;
                    let res: String = callback
                        .call1(py, (name, session, args))?
                        .extract(py)?;
                    Ok(postcard::to_allocvec(&res).unwrap())
                })
            })
            .await
            .unwrap(),
        }
    }
}
