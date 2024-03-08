use crate::AccessKind;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
#[repr(u16)]
pub enum Error {
    #[error("unknown error: {0}")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),

    #[error("error#{0}: {1}")]
    Plain(u16, String),

    #[error("failed to run migrations: {0}")]
    MigrateError(#[from] sqlx::migrate::MigrateError),
    #[error("internal database error: {0}")]
    DatabaseError(Box<sqlx::Error>),
    #[error("channel error")]
    ChannelError,
    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("invalid credentials")]
    InvalidCredentials,

    #[error("invalid tag: {0:?}, cause: {1}")]
    InvalidTag(String, &'static str),
    #[error("invalid username: {0:?}, cause: {1}")]
    InvalidUsername(String, &'static str),
    #[error("invalid password: {0:?}, cause: {1}")]
    InvalidPassword(String, &'static str),

    #[error("username occupied: {0}")]
    UsernameOccupied(String),

    #[error("object not found: {0}")]
    ObjectNotFound(Uuid),
    #[error("no such tag ({1:?}) in object {0}")]
    NoSuchTag(Uuid, String),

    #[error("access denied for {1}: {0}")]
    AccessDenied(Uuid, AccessKind),
    #[error("permission denied: {0:?}")]
    PermissionDenied(Option<String>),

    #[error("invalid rule: {0:?}, cause: {1}")]
    InvalidRule(String, String),
    #[error("invalid object: {0}")]
    InvalidObject(Uuid),

    #[error("alias of {0} already exists: {1}")]
    AliasAlreadyExists(String, String),
    #[error("tag already exists: {0}")]
    TagAlreadyExists(String),

    #[error("invalid parameters")]
    InvalidParameters,

    #[error("can't edit tag: {0}")]
    CantEditTag(String),

    #[error("RPC timed out")]
    RpcTimeout,
    #[error("RPC not found: {0}")]
    RpcNotFound(String),
    #[error("RPC already registered: {0}")]
    RpcAlreadyRegistered(String),

    #[error("invalid image")]
    InvalidImage,
    #[error("file not found")]
    FileNotFound,

    #[error("invalid order: {0}")]
    InvalidOrder(String),

    #[error("candle error: {0}")]
    OrtError(#[from] ort::Error),

    #[error("precondition failed")]
    PreconditionFailed,

    #[error("python error: {0}")]
    PythonError(String),
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::DatabaseError(Box::new(e))
    }
}

impl Error {
    pub fn unknown(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Unknown(Box::new(e))
    }

    pub fn error_code(&self) -> u16 {
        100 + unsafe { *(self as *const _ as *const u16) }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
