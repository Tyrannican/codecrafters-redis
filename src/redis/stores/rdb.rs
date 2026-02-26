use std::path::PathBuf;

use crate::redis::{protocol::RedisError, utils::empty_rdb};
use anyhow::Result;
use bytes::Bytes;

#[derive(Debug)]
pub struct RdbFile {
    raw: Bytes,
}

impl RdbFile {
    pub fn new() -> Self {
        Self { raw: Bytes::new() }
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

        Ok(())
    }

    pub fn raw(&self) -> Bytes {
        self.raw.clone()
    }
}
