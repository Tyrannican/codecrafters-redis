use std::{path::PathBuf, time::Instant};

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

    pub fn get(&self, key: &Bytes) {
        for db in self.inner.databases.iter() {
            for entry in db.entries.iter() {
                let e_key = &entry.key;
                if e_key == key {
                    todo!()
                }
            }
        }
    }
}
