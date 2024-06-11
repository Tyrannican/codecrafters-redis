use crate::redis::store::{RedisStore, RedisStoreEntry};

// NOTE: Format is (master ip, master port, replica port)
pub type ReplicaMaster = (String, u16, u16);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRole {
    Master,
    Replica(ReplicaMaster),
}

impl ServerRole {
    pub fn as_string(&self) -> String {
        match *self {
            Self::Master => "master".to_string(),
            Self::Replica(_) => "slave".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerInformation {
    master_replid: String,
    master_repl_offset: usize,
}

impl ServerInformation {
    pub fn new() -> Self {
        Self {
            // TODO: Pass this in if required
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerContext {
    pub role: ServerRole,
    store: RedisStore,
    server_information: ServerInformation,
}

impl ServerContext {
    pub fn new(role: ServerRole) -> Self {
        Self {
            role,
            store: RedisStore::new(),
            server_information: ServerInformation::new(),
        }
    }

    pub fn server_information(&self) -> String {
        format!(
            "role:{}master_replid:{}master_repl_offset:{}",
            self.role.as_string(),
            self.server_information.master_replid,
            self.server_information.master_repl_offset
        )
    }

    pub fn server_replid(&self) -> String {
        self.server_information.master_replid.clone()
    }

    pub fn update_store(&mut self, key: String, value: RedisStoreEntry) {
        self.store.set(key, value);
    }

    pub fn retrieve_from_store(&mut self, key: impl AsRef<str>) -> Option<RedisStoreEntry> {
        self.store.get(key.as_ref())
    }
}
