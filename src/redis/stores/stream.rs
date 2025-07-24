use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::redis::{protocol::RedisError, utils::bytes_to_str};
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

    pub fn add_entry<'a>(
        &mut self,
        stream_key: &'a Bytes,
        entry_key: &'a Bytes,
        values: Option<&'a [(Bytes, Bytes)]>,
    ) -> Result<Bytes, RedisError> {
        let stream = self.map.entry(stream_key.clone()).or_default();
        let entry_key = validate_entry_id(entry_key, stream)?;
        let entry = stream.entry(entry_key.clone()).or_default();
        if let Some(values) = values {
            for value in values {
                entry.insert(value.clone());
            }
        }

        Ok(entry_key)
    }
}

fn autogenerate_entry_id() -> Bytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("should not be a problem")
        .as_millis();

    format!("{now}-0").into()
}

// TODO: Start here
pub fn validate_entry_id(entry_id: &Bytes, stream: &mut Stream) -> Result<Bytes, RedisError> {
    let entry_id_str = bytes_to_str(entry_id)?;
    if entry_id_str == "*" {
        return Ok(autogenerate_entry_id());
    }

    match stream.last_entry() {
        Some(last) => {
            let Some((timestamp, seq)) = entry_id_str.split_once("-") else {
                todo!("error");
            };
            let Some((l_timestamp, l_seq)) = bytes_to_str(last.key())?.split_once("-") else {
                todo!("error");
            };

            if entry_id_str <= "0-0" {
                return Err(RedisError::StreamError(
                    "ERR The ID specified in XADD must be greater than 0-0".to_string(),
                ));
            }

            let next_seq = l_seq
                .parse::<usize>()
                .map_err(|_| RedisError::NumberParse)?
                + 1;

            if timestamp > l_timestamp {
                if seq == "*" {
                    return Ok(format!("{timestamp}-{next_seq}").into());
                }

                return Ok(entry_id.clone());
            } else if timestamp == l_timestamp {
                if seq == "*" {
                    return Ok(format!("{timestamp}-{next_seq}").into());
                }
                if seq > l_seq {
                    return Ok(entry_id.clone());
                }

                return Err(RedisError::StreamError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()));
            } else {
                return Err(RedisError::StreamError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()));
            }
        }
        None => {
            if entry_id_str <= "0-0" {
                return Err(RedisError::StreamError(
                    "ERR The ID specified in XADD must be greater than 0-0".to_string(),
                ));
            }

            return Ok(entry_id.clone());
        }
    }
}
