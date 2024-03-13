use serde::{Deserialize, Serialize};
use std::{fmt, io};

macro_rules! anyhow {
    ($fmt:literal $($args:tt)*) => {
        $crate::anyhow!(@Unspecified $fmt $($args)*)
    };
    (@$kind:ident $fmt:literal $($args:tt)*) => {
        $crate::Error::new($crate::ErrorKind::$kind, Some(anyhow::anyhow!($fmt $($args)*)))
    };
    (@$kind:ident) => {
        $crate::Error::new($crate::ErrorKind::$kind, None)
    };
}
pub(crate) use anyhow;

macro_rules! bail {
    ($($t:tt)*) => {
        return Err($crate::anyhow!($($t)*))
    };
}
pub(crate) use bail;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: Option<anyhow::Error>,
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
        Self { kind, source }
    }

    pub fn anyhow(source: anyhow::Error) -> Self {
        Self::new(ErrorKind::Unspecified, Some(source))
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

    pub fn kind(&self) -> ErrorKind {
        self.kind
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

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[repr(u16)]
pub enum ErrorKind {
    #[default]
    Unspecified,

    DBError,
    IOError,
    PyError,

    ObjectNotFound,
    RpcNotFound,
    TagNotFound,

    AccessDenied,
    PermissionDenied,

    InvalidCredentials,
    InvalidFilter,
    InvalidInput,
    InvalidRule,
    InvalidObject,
    InvalidOrder,
    InvalidTag,

    UsernameOccupied,

    PreconditionFailed,
    TagExists,

    RpcConflict,
    RpcTimeout,
}
impl ErrorKind {
    pub fn error_code(self) -> u16 {
        self as u16
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::new(ErrorKind::IOError, Some(err.into()))
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::new(ErrorKind::DBError, Some(err.into()))
    }
}
