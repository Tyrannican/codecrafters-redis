use std::collections::BTreeMap;

use bytes::Bytes;

pub struct ListStore {
    map: BTreeMap<Bytes, Vec<Bytes>>,
}

impl ListStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn len(&self, key: &Bytes) -> usize {
        match self.map.get(key) {
            Some(entry) => entry.len(),
            None => 0,
        }
    }

    pub fn append(&mut self, key: Bytes, element: Bytes) -> usize {
        let entry = self.map.entry(key).or_insert(Vec::new());
        entry.push(element);
        entry.len()
    }
}
