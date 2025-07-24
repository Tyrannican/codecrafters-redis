use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::redis::{
    protocol::RedisError,
    utils::{bytes_to_number, bytes_to_str},
};
use bytes::Bytes;

type Stream = BTreeMap<Bytes, BTreeSet<(Bytes, Bytes)>>;

pub struct StreamStore {
    map: BTreeMap<Bytes, Stream>,
}

impl StreamStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
        }
    }

    pub fn contains(&self, key: &Bytes) -> bool {
        self.map.contains_key(key)
    }

    // TODO: Start here
    pub fn validate_entry_id(&self, entry_key: &Bytes) {
        if entry_key.len() == 1 && entry_key[0] == b'*' {
            //
        }
    }

    pub fn add_entry<'a>(
        &mut self,
        stream_key: &'a Bytes,
        entry_key: &'a Bytes,
        values: Option<&'a [(Bytes, Bytes)]>,
    ) {
        let stream = self.map.entry(stream_key.clone()).or_default();
        let entry = stream.entry(entry_key.clone()).or_default();
        if let Some(values) = values {
            for value in values {
                entry.insert(value.clone());
            }
        }
    }
}

fn autogenerate_entry_id() -> Bytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("should not be a problem")
        .as_millis();

    format!("{now}-0").into()
}
