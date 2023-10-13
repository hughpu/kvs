#![deny(missing_docs)]
//! A simple key/value store.

pub use error::{KvsError, Result};
pub use engines::{KvsEngine, KvStore, SledKvsEngine, Command};

mod error;
mod engines;
mod protocols;

/// client module for kvs-client binary usage
pub mod client;

/// server module kvs-server binary usage
pub mod server;

/// thread pool trait and implementations
pub mod thread_pool;
