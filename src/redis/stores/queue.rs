use bytes::Bytes;
use std::collections::BTreeMap;

use crate::redis::protocol::{RedisCommand, Transaction};

pub struct TransactionStore {
    map: BTreeMap<Bytes, Transaction>,
}

impl TransactionStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
        }
    }

    pub fn create_transaction(&mut self, client_id: &Bytes) {
        self.map.insert(client_id.clone(), Transaction::default());
    }

    pub fn has_transaction(&self, client_id: &Bytes) -> bool {
        self.map.get(client_id).is_some()
    }

    pub fn remove_transaction(&mut self, client_id: &Bytes) -> Option<Transaction> {
        self.map.remove(client_id)
    }

    pub fn add_to_transaction(&mut self, client_id: &Bytes, cmd: RedisCommand) {
        self.map.entry(client_id.clone()).and_modify(|q| q.add(cmd));
    }
}
