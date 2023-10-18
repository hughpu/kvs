use failure::Fail;
use slog::KV;
use std::{io, string::FromUtf8Error, sync::PoisonError, any::type_name};
use sled;

/// Error type for kvs.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Poison error.
    #[fail(display = "{}", _0)]
    PoisonError(String),
    /// String error.
    #[fail(display = "{}", _0)]
    StringError(String),
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// sled engine error
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),
    /// ivec convert to utf8 error
    #[fail(display = "{}", _0)]
    FromUtf8Error(#[cause] FromUtf8Error),
    /// Removing non-existent key error.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::FromUtf8Error(err)
    }
}

impl<T> From<PoisonError<T>> for KvsError {
    fn from(value: PoisonError<T>) -> Self {
        KvsError::PoisonError(format!("poison error with type: {}", type_name::<T>()))
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
