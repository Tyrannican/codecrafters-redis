use bytes::Bytes;
use std::collections::BTreeMap;

use crate::redis::protocol::RedisCommand;

pub struct QueueStore {
    map: BTreeMap<Bytes, Vec<RedisCommand>>,
}

impl QueueStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
        }
    }

    pub fn create_queue(&mut self, client_id: &Bytes) {
        self.map.insert(client_id.clone(), Vec::default());
    }

    pub fn has_queue(&self, client_id: &Bytes) -> bool {
        self.map.get(client_id).is_some()
    }

    pub fn remove_queue(&mut self, client_id: &Bytes) {
        self.map.remove(client_id);
    }

    pub fn enqueue(&mut self, client_id: &Bytes, cmd: RedisCommand) {
        self.map
            .entry(client_id.clone())
            .and_modify(|q| q.push(cmd));
    }
}
