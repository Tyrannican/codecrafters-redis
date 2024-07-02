use evmap::{ReadHandle, WriteHandle};
use evmap_derive::ShallowCopy;

pub type StoreReader = ReadHandle<String, RedisStoreEntry>;
pub type StoreWriter = WriteHandle<String, RedisStoreEntry>;

#[derive(ShallowCopy, Debug, Clone, Hash, Eq, PartialEq)]
pub struct RedisStoreEntry {
    pub value: String,
    expires: Option<u128>,
}

impl RedisStoreEntry {
    pub fn new(value: String) -> Self {
        Self {
            value,
            expires: None,
        }
    }

    pub fn value(&self) -> Option<&str> {
        if self.is_expired() {
            return None;
        }

        Some(&self.value)
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
