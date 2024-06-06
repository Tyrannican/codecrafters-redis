use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RedisStoreEntry {
    value: String,
    expires: Option<u128>,
}

impl RedisStoreEntry {
    pub fn new(value: String) -> Self {
        Self {
            value,
            expires: None,
        }
    }

    pub fn expires(mut self, millis: u128) -> Self {
        self.expires = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + millis,
        );

        self
    }

    pub fn is_expired(&self) -> bool {
        if self.expires.is_none() {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let expires_at = self.expires.unwrap();
        if now > expires_at {
            return true;
        }

        return false;
    }
}

#[derive(Debug, Clone)]
pub struct RedisStore {
    inner: HashMap<String, RedisStoreEntry>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: impl AsRef<str>) -> Option<RedisStoreEntry> {
        match self.inner.get(key.as_ref()) {
            Some(entry) => {
                if entry.is_expired() {
                    self.inner.remove(key.as_ref());
                    return None;
                }

                Some(entry.clone())
            }
            None => None,
        }
    }

    pub fn set(&mut self, key: String, value: RedisStoreEntry) {
        self.inner.insert(key, value);
    }
}
