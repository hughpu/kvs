mod kv;
mod sled_engine;

use crate::Result;

pub use kv::KvStore;
pub use kv::Command;
pub use sled_engine::SledKvsEngine;


/// trait for general kv store engine
pub trait KvsEngine {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()>;


    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    
    
    /// Removes a given key.
    fn remove(&mut self, key: String) -> Result<()>;
}