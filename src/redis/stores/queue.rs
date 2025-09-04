use bytes::Bytes;
use std::collections::BTreeMap;

use crate::redis::protocol::{RedisCommand, Transaction};

pub struct TransactionStore {
    map: BTreeMap<Bytes, Vec<Transaction>>,
}

impl TransactionStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
        }
    }

    pub fn create_queue(&mut self, client_id: &Bytes) {
        self.map
            .insert(client_id.clone(), vec![Transaction::default()]);
    }

    pub fn has_queue(&self, client_id: &Bytes) -> bool {
        self.map.get(client_id).is_some()
    }

    pub fn remove_queue(&mut self, client_id: &Bytes) {
        self.map.remove(client_id);
    }

    pub fn enqueue(&mut self, client_id: &Bytes, cmd: RedisCommand) {
        self.map.entry(client_id.clone()).and_modify(|q| {
            if let Some(last) = q.last_mut() {
                last.add(cmd);
            }
        });
    }

    pub fn pop(&mut self, client_id: &Bytes) -> Option<Transaction> {
        match self.map.get_mut(client_id) {
            Some(txns) => txns.pop(),
            None => None,
        }
    }
}
