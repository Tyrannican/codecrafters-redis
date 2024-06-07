use crate::redis::store::{RedisStore, RedisStoreEntry};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ServerRole {
    Master,
    Replica,
}

impl ServerRole {
    pub fn as_string(self) -> String {
        match self {
            Self::Master => "master".to_string(),
            Self::Replica => "slave".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerContext {
    pub role: ServerRole,
    store: RedisStore,
}

impl ServerContext {
    pub fn new(role: ServerRole) -> Self {
        Self {
            role,
            store: RedisStore::new(),
        }
    }

    pub fn update_store(&mut self, key: String, value: RedisStoreEntry) {
        self.store.set(key, value);
    }

    pub fn retrieve_from_store(&mut self, key: impl AsRef<str>) -> Option<RedisStoreEntry> {
        self.store.get(key.as_ref())
    }
}
