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

    pub fn contains(&self, key: &Bytes) -> bool {
        self.map.contains_key(key)
    }

    pub fn append(&mut self, key: &Bytes, element: &Bytes) -> usize {
        let entry = self.map.entry(key.clone()).or_default();
        entry.push(element.clone());

        entry.len()
    }

    pub fn prepend(&mut self, key: &Bytes, element: &Bytes) -> usize {
        let entry = self.map.entry(key.clone()).or_default();
        entry.insert(0, element.clone());
        entry.len()
    }

    pub fn slice(&self, key: &Bytes, start: i64, end: i64) -> Option<&[Bytes]> {
        let list = self.map.get(key)?;

        let list_size = list.len();
        let start = idx_calc(start, list_size);
        let mut end = idx_calc(end, list_size);

        if end >= list_size {
            end = list_size - 1
        }

        if start > end || start >= list.len() {
            return None;
        }

        Some(&list[start..=end])
    }

    pub fn remove(&mut self, key: &Bytes, to_remove: usize) -> Option<Vec<Bytes>> {
        let list = self.map.get_mut(key)?;

        if list.is_empty() {
            return Some(Vec::new());
        }

        let sub_list: Vec<Bytes> = list.drain(..to_remove).collect();

        Some(sub_list)
    }

    pub fn remove_single(&mut self, key: &Bytes) -> Option<Bytes> {
        let list = self.map.get_mut(key)?;
        Some(list.remove(0))
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
