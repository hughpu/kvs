use crate::{KvsEngine, Result, KvsError};
use {sled, sled::Db};
use std::{path::PathBuf, sync::Arc};

/// sled implemented kv store
#[derive(Clone)]
pub struct SledKvsEngine {
    sled_db: Arc<Db>,
}

impl SledKvsEngine {
    /// open path to use as kv database
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(SledKvsEngine {
            sled_db: Arc::new(sled::open(path.into())?)
        })
    }
}


impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.sled_db.insert(key, value.as_str())?;
        self.sled_db.flush()?;
        Ok(())
    }
    
    fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.sled_db.get(key)?;
        match value {
            Some(ivec) => Ok(Some(String::from_utf8(ivec.to_vec())?)),
            None => Ok(None)
        }
    }
    
    fn remove(&self, key: String) -> Result<()> {
        self.sled_db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.sled_db.flush()?;
        Ok(())
    }
}