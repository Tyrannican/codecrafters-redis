#![allow(dead_code)]
use crate::redis::RedisError;
use bytes::Bytes;

mod parser;
pub use parser::parse_rdb;

use std::{collections::HashMap, time::Duration};

pub const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn empty_rdb() -> Result<Bytes, RedisError> {
    Ok(hex::decode(EMPTY_RDB)
        .map_err(|e| RedisError::HexError(e.to_string()))?
        .into())
}

#[derive(Default, Debug, Clone)]
pub struct RdbInner {
    pub version: u32,
    pub auxiliary: HashMap<Bytes, Bytes>,
    pub databases: Vec<RdbDatabase>,
}

#[derive(Debug, Clone, Default)]
pub struct RdbDatabase {
    pub id: u64,
    pub entries: HashMap<Bytes, RdbDatabaseEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct RdbDatabaseEntry {
    pub expiry: Option<Duration>,
    pub value: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct RdbKeyValue {
    pub expiry: Option<RdbExpiry>,
    pub key: Bytes,
    pub value: RdbValue,
}

#[derive(Debug, Clone)]
pub enum RdbExpiry {
    Seconds(u32),
    Milliseconds(u64),
}

#[derive(Debug, Clone)]
pub enum RdbValue {
    String(Bytes),
}

impl Default for RdbValue {
    fn default() -> Self {
        Self::String(Bytes::new())
    }
}
