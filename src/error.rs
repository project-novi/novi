use std::{fmt, io, str::FromStr};
use strum::{AsRefStr, EnumString};
use tonic::{metadata::KeyAndValueRef, Code, Status};
use tracing::error;

const METADATA_KEYS: &[&str] = &["argument", "permission", "id", "tag", "type"];
fn metadata_key_to_static(key: &str) -> Option<&'static str> {
    METADATA_KEYS.iter().find(|it| **it == key).copied()
}

macro_rules! anyhow {
    ($fmt:literal $($args:tt)*) => {
        $crate::anyhow!(@Unspecified $fmt $($args)*)
    };
    (@$kind:ident $(($name:literal => $val:expr))* $fmt:literal $($args:tt)*) => {
        $crate::Error::new($crate::ErrorKind::$kind, Some(anyhow::anyhow!($fmt $($args)*)))
            .with_metadata(vec![$(($name, $val.to_string())),*])
    };
    (@$kind:ident $(($name:literal => $val:expr))*) => {
        $crate::Error::new($crate::ErrorKind::$kind, Some(anyhow::anyhow!("")))
            .with_metadata(vec![$(($name, $val.to_string())),*])
    };
    (@$kind:ident) => {
        $crate::Error::new($kind, None)
    };
}
pub(crate) use anyhow;

macro_rules! bail {
    ($($t:tt)*) => {
        return Err($crate::anyhow!($($t)*).into())
    };
}
pub(crate) use bail;

use crate::proto;

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub source: Option<anyhow::Error>,
    pub metadata: Vec<(&'static str, String)>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.kind)?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

impl Error {
    pub fn new(kind: ErrorKind, source: Option<anyhow::Error>) -> Self {
        Self {
            kind,
            source,
            metadata: Vec::new(),
        }
    }

    pub fn anyhow(source: anyhow::Error) -> Self {
        Self::new(ErrorKind::Unspecified, Some(source))
    }

    pub fn with_metadata(mut self, metadata: Vec<(&'static str, String)>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn msg<C>(msg: C) -> Self
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self::anyhow(anyhow::Error::msg(msg))
    }

    pub fn context<C>(mut self, context: C) -> Self
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        self.source = Some(match self.source {
            Some(err) => err.context(context),
            None => anyhow::Error::msg(context),
        });
        self
    }

    pub fn with_kind(mut self, kind: ErrorKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub fn from_pb(pb: proto::Error) -> Self {
        Self {
            kind: ErrorKind::from_str(pb.kind.as_str()).unwrap_or(ErrorKind::Unspecified),
            source: Some(anyhow::Error::msg(pb.message)),
            metadata: pb
                .metadata
                .into_iter()
                .filter_map(|(k, v)| metadata_key_to_static(&k).map(|k| (k, v)))
                .collect(),
        }
    }

    pub fn from_status(mut pb: Status) -> Self {
        let kind = pb
            .metadata_mut()
            .remove("kind")
            .and_then(|it| it.to_str().ok().and_then(|it| ErrorKind::from_str(it).ok()))
            .unwrap_or(ErrorKind::Unspecified);
        Self {
            kind,
            source: Some(anyhow::Error::msg(pb.message().to_owned())),
            metadata: pb
                .metadata()
                .iter()
                .filter_map(|kv| match kv {
                    KeyAndValueRef::Ascii(k, v) => metadata_key_to_static(k.as_ref())
                        .and_then(|k| v.to_str().ok().map(|v| (k, v.to_owned()))),
                    _ => None,
                })
                .collect(),
        }
    }
}

pub trait ErrorExt {
    fn wrap(self) -> Error;
}
impl<E> ErrorExt for E
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn wrap(self) -> Error {
        Error::anyhow(self.into())
    }
}
pub trait ResultExt<T> {
    fn wrap(self) -> Result<T>;
}
impl<T, E> ResultExt<T> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn wrap(self) -> Result<T> {
        self.map_err(|err| err.wrap())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Context<T> {
    fn kind(self, kind: ErrorKind) -> Result<T>;

    fn context<C>(self, context: C) -> Result<T>
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static;

    fn with_context<C, F>(self, f: F) -> Result<T>
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
        F: FnOnce() -> C;
}

impl<T, E> Context<T> for Result<T, E>
where
    E: ErrorExt,
{
    fn kind(self, kind: ErrorKind) -> Result<T> {
        match self {
            Ok(ok) => Ok(ok),
            Err(error) => Err(error.wrap().with_kind(kind)),
        }
    }

    fn context<C>(self, context: C) -> Result<T>
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        match self {
            Ok(ok) => Ok(ok),
            Err(error) => Err(error.wrap().context(context)),
        }
    }

    fn with_context<C, F>(self, context: F) -> Result<T, Error>
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        match self {
            Ok(ok) => Ok(ok),
            Err(error) => Err(error.wrap().context(context())),
        }
    }
}

impl<T> Context<T> for Option<T> {
    fn kind(self, kind: ErrorKind) -> Result<T> {
        self.ok_or_else(|| Error::new(kind, None))
    }

    fn context<C>(self, context: C) -> Result<T>
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        self.ok_or_else(|| Error::msg(context))
    }

    fn with_context<C, F>(self, context: F) -> Result<T, Error>
    where
        C: fmt::Display + fmt::Debug + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.ok_or_else(|| Error::msg(context()))
    }
}

#[derive(EnumString, AsRefStr, Debug, Default, PartialEq, Eq, Clone)]
pub enum ErrorKind {
    #[default]
    Unspecified,

    Unsupported,

    DBError,
    IOError,

    PermissionDenied,
    IdentityExpired,

    FileNotFound,
    FunctionNotFound,
    ObjectNotFound,

    InvalidArgument,
    InvalidCredentials,
    InvalidTag,
    InvalidObject,
    InvalidState,
}
impl ErrorKind {
    pub fn to_grpc_code(&self) -> Code {
        match self {
            ErrorKind::Unspecified | ErrorKind::DBError | ErrorKind::IOError => Code::Internal,

            ErrorKind::Unsupported => Code::Unimplemented,

            ErrorKind::PermissionDenied => Code::PermissionDenied,
            ErrorKind::IdentityExpired => Code::Unauthenticated,

            ErrorKind::FileNotFound | ErrorKind::FunctionNotFound | ErrorKind::ObjectNotFound => {
                Code::NotFound
            }

            ErrorKind::InvalidArgument | ErrorKind::InvalidTag | ErrorKind::InvalidObject => {
                Code::InvalidArgument
            }
            ErrorKind::InvalidCredentials => Code::Unauthenticated,
            ErrorKind::InvalidState => Code::FailedPrecondition,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::new(ErrorKind::IOError, Some(err.into()))
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}
impl From<redis::RedisError> for Error {
    fn from(err: redis::RedisError) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}
impl From<deadpool::managed::PoolError<tokio_postgres::Error>> for Error {
    fn from(err: deadpool::managed::PoolError<tokio_postgres::Error>) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}
impl From<deadpool::managed::PoolError<redis::RedisError>> for Error {
    fn from(err: deadpool::managed::PoolError<redis::RedisError>) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}
impl From<deadpool_postgres::CreatePoolError> for Error {
    fn from(err: deadpool_postgres::CreatePoolError) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}
impl From<deadpool_redis::CreatePoolError> for Error {
    fn from(err: deadpool_redis::CreatePoolError) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        if err.kind.to_grpc_code() == Code::Internal {
            error!(?err);
        }

        let mut status = Status::new(
            err.kind.to_grpc_code(),
            err.source.map_or_else(String::new, |it| it.to_string()),
        );
        let metadata = status.metadata_mut();
        for (key, value) in err.metadata {
            metadata.insert(key, value.parse().unwrap());
        }
        metadata.insert("kind", err.kind.as_ref().parse().unwrap());
        status
    }
}
impl From<Error> for proto::Error {
    fn from(err: Error) -> Self {
        Self {
            kind: err.kind.as_ref().to_owned(),
            message: err.to_string(),
            metadata: err
                .metadata
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect(),
        }
    }
}
