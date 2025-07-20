use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::redis::protocol::RedisError;

struct MapStoreValue {
    value: Bytes,
    created: Instant,
    ttl: Option<Duration>,
}

impl MapStoreValue {
    pub fn new(value: Bytes, ttl: Option<Duration>) -> Self {
        Self {
            value,
            created: Instant::now(),
            ttl,
        }
    }

    pub fn expired(&self) -> bool {
        match self.ttl {
            None => false,
            Some(ex) => self.created.elapsed().as_millis() > ex.as_millis(),
        }
    }
}

pub struct MapStore {
    map: BTreeMap<Bytes, MapStoreValue>,
}

impl MapStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<&Bytes> {
        let Some(value) = self.map.get(key) else {
            return None;
        };

        // TODO: Cleanup of expired values
        if value.expired() {
            return None;
        }

        Some(&value.value)
    }

    pub fn set(
        &mut self,
        key: &Bytes,
        value: &Bytes,
        ttl: Option<Bytes>,
    ) -> Result<(), RedisError> {
        let ttl = if let Some(ex) = ttl {
            let ms_str = str::from_utf8(&ex).map_err(|_| RedisError::StringConversion)?;
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| RedisError::IntegerParse)?;

            Some(Duration::from_millis(ms))
        } else {
            None
        };

        let store_value = MapStoreValue::new(value.clone(), ttl);

        self.map.insert(key.clone(), store_value);
        Ok(())
    }
}

#[cfg(test)]
mod map_store_tests {
    use super::*;

    #[test]
    fn expired_values() {
        let v1 = MapStoreValue::new(Bytes::new(), None);
        assert!(!v1.expired());

        let v2 = MapStoreValue::new(Bytes::new(), Some(Duration::from_millis(200)));
        assert!(!v2.expired());
        std::thread::sleep(Duration::from_millis(300));
        assert!(v2.expired());
    }

    #[test]
    fn store_test() -> Result<(), RedisError> {
        let mut ms = MapStore::new();
        ms.set(&"hello".into(), &"there".into(), None)?;
        assert!(ms.get(&"hello".into()).is_some());
        assert!(ms.get(&"not present".into()).is_none());

        Ok(())
    }

    #[test]
    fn store_expired_values() -> Result<(), RedisError> {
        let mut ms = MapStore::new();
        ms.set(&"hello".into(), &"there".into(), Some("200".into()))?;
        assert!(ms.get(&"hello".into()).is_some());
        std::thread::sleep(Duration::from_millis(300));
        assert!(ms.get(&"hello".into()).is_none());

        Ok(())
    }
}
