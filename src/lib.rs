#![deny(missing_docs)]
//! A simple key/value store.

pub use error::{KvsError, Result};
pub use engines::{KvsEngine, KvStore, SledKvsEngine, Command};

mod error;
mod engines;
