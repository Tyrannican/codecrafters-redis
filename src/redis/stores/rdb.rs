use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::redis::{
    protocol::RedisError,
    rdb::{empty_rdb, parse_rdb, RdbInner},
};
use anyhow::Result;
use bytes::Bytes;

#[derive(Debug)]
pub struct RdbFile {
    raw: Bytes,
    inner: RdbInner,
}

impl RdbFile {
    pub fn new() -> Self {
        Self {
            raw: Bytes::new(),
            inner: RdbInner::default(),
        }
    }

    pub fn load(&mut self, path: Option<PathBuf>) -> Result<(), RedisError> {
        match path {
            Some(path) => {
                let raw = if !path.exists() {
                    empty_rdb()?
                } else {
                    let content =
                        std::fs::read(&path).map_err(|e| RedisError::FileRead(e.to_string()))?;
                    Bytes::from_iter(content.into_iter())
                };

                self.raw = raw;
            }

            None => {
                self.raw = empty_rdb()?;
            }
        }

        (_, self.inner) =
            parse_rdb(&self.raw[..]).map_err(|e| RedisError::RdbParse(e.to_string()))?;

        eprintln!("{:?}", self.inner);

        Ok(())
    }

    pub fn raw(&self) -> Bytes {
        self.raw.clone()
    }

    pub fn list(&self) -> Vec<&Bytes> {
        self.inner
            .databases
            .iter()
            .map(|db| db.entries.keys().collect::<Vec<&Bytes>>())
            .flatten()
            .collect::<Vec<&Bytes>>()
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        for db in self.inner.databases.iter() {
            match db.entries.get(key) {
                Some(entry) => {
                    if let Some(ex) = entry.expiry {
                        let start = SystemTime::now();
                        let now = start.duration_since(UNIX_EPOCH).expect("time goes forward");
                        if now.as_millis() > ex.as_millis() {
                            return None;
                        }
                    }

                    return Some(entry.value.clone());
                }
                None => return None,
            }
        }

        None
    }
}
