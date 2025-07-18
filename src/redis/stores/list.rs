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

    pub fn prepend(&mut self, key: &Bytes, element: &Bytes) -> usize {
        let entry = self.map.entry(key.clone()).or_insert(Vec::new());
        entry.insert(0, element.clone());
        entry.len()
    }

    pub fn slice(&self, key: &Bytes, start: i64, end: i64) -> Option<&[Bytes]> {
        if let Some(list) = self.map.get(key) {
            let list_size = list.len();
            let start = idx_calc(start, list_size);
            let mut end = idx_calc(end, list_size);

            if end >= list_size {
                end = list_size - 1
            }

            if start > end || start >= list.len() {
                return None;
            }

            return Some(&list[start..=end]);
        }

        None
    }

    pub fn remove(&mut self, key: &Bytes, to_remove: usize) -> Option<Vec<Bytes>> {
        if let Some(list) = self.map.get_mut(key) {
            let sub_list: Vec<Bytes> = list.drain(..to_remove).collect();
            return Some(sub_list);
        }

        None
    }
}

#[inline]
fn idx_calc(int: i64, list_size: usize) -> usize {
    if int < 0 {
        if i64::abs(int) > list_size as i64 {
            0
        } else {
            (list_size as i64 + int) as usize
        }
    } else {
        int as usize
    }
}
