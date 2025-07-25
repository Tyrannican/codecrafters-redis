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

    pub fn xrange<'a>(
        &'a self,
        stream_key: &Bytes,
        start_id: &Bytes,
        end_id: &Bytes,
    ) -> Result<Vec<Vec<&'a Bytes>>, RedisError> {
        let Some(stream) = self.map.get(stream_key) else {
            todo!("error");
        };

        let start_id_str = bytes_to_str(start_id)?;
        let end_id_str = bytes_to_str(end_id)?;

        let mut values = Vec::new();
        for (entry_id, entry) in stream.iter() {
            if start_id_str != "-" && entry_id < start_id {
                continue;
            }

            if end_id_str != "+" && entry_id > end_id {
                continue;
            }

            let mut entry_vec = Vec::new();
            entry_vec.push(entry_id);
            for (key, value) in entry.iter() {
                entry_vec.push(key);
                entry_vec.push(value);
            }

            values.push(entry_vec);
        }

        Ok(values)
    }
}

fn autogenerate_entry_id() -> Bytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("should not be a problem")
        .as_millis();

    format!("{now}-0").into()
}

// TODO: Refactor this mess...
pub fn validate_entry_id(entry_id: &Bytes, stream: &mut Stream) -> Result<Bytes, RedisError> {
    let entry_id_str = bytes_to_str(entry_id)?;
    if entry_id_str == "*" {
        return Ok(autogenerate_entry_id());
    }

    if entry_id_str == "0-0" {
        return Err(RedisError::StreamIdError(
            "ERR The ID specified in XADD must be greater than 0-0".to_string(),
        ));
    }

    let (timestamp, seq) = entry_id_str
        .split_once("-")
        .expect("this should be a valid id");

    match stream.last_entry() {
        Some(last) => {
            let (l_timestamp, l_seq) = bytes_to_str(last.key())?
                .split_once("-")
                .expect("this should be a valid entry id");

            if l_timestamp > timestamp {
                return Err(RedisError::StreamIdError(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                ));
            }

            if seq == "*" {
                if timestamp == l_timestamp {
                    let next_seq = l_seq
                        .parse::<usize>()
                        .map_err(|_| RedisError::NumberParse)?
                        + 1;

                    return Ok(format!("{timestamp}-{next_seq}").into());
                }

                return Ok(format!("{timestamp}-0").into());
            } else {
                if timestamp == l_timestamp {
                    if seq <= l_seq {
                        return Err(RedisError::StreamIdError(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                        ));
                    }

                    return Ok(format!("{timestamp}-{seq}").into());
                }

                return Ok(format!("{timestamp}-{seq}").into());
            }
        }
        None => {
            if timestamp == "0" {
                return Ok("0-1".into());
            } else if seq == "*" {
                return Ok(format!("{timestamp}-0").into());
            } else {
                return Ok(format!("{timestamp}-{seq}").into());
            }
        }
    }
}
