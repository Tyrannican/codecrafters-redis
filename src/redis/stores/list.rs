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

    pub fn append(&mut self, key: &Bytes, element: &Bytes) -> usize {
        let entry = self.map.entry(key.clone()).or_insert(Vec::new());
        entry.push(element.clone());
        entry.len()
    }

    pub fn slice(&self, key: &Bytes, start: i64, end: i64) -> Option<&[Bytes]> {
        if let Some(list) = self.map.get(key) {
            let start = if start < 0 {
                if i64::abs(start) > list.len() as i64 {
                    0
                } else {
                    (list.len() as i64 + start) as usize
                }
            } else {
                start as usize
            };

            let end = if end < 0 {
                if i64::abs(end) > list.len() as i64 {
                    0
                } else {
                    (list.len() as i64 + end) as usize
                }
            } else if end >= list.len() as i64 {
                list.len() - 1
            } else {
                end as usize
            };

            dbg!(start, end);
            if start > end || start >= list.len() {
                return None;
            }

            return Some(&list[start..=end]);
        }

        None
    }
}
