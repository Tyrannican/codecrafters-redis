use bytes::Bytes;
use crossbeam_skiplist::SkipSet;
use ordered_float::OrderedFloat;

use std::collections::HashMap;

type SetEntry = (OrderedFloat<f64>, Bytes);

#[derive(Debug)]
pub struct SortedSetStore {
    sets: HashMap<Bytes, SortedSet>,
}

impl SortedSetStore {
    pub fn new() -> Self {
        Self {
            sets: HashMap::new(),
        }
    }

    pub fn zadd(&mut self, set: &Bytes, name: &Bytes, score: f64) -> usize {
        let entry = self.sets.entry(set.clone()).or_insert(SortedSet::new());
        entry.add(name, score)
    }

    pub fn zrank(&self, set: &Bytes, name: &Bytes) -> Option<usize> {
        match self.sets.get(set) {
            Some(set) => set.rank(name),
            None => None,
        }
    }

    pub fn zrange(&self, set: &Bytes, start: i32, end: i32) -> Vec<Bytes> {
        match self.sets.get(set) {
            Some(set) => set.range(start, end),
            None => Vec::new(),
        }
    }

    pub fn zcard(&self, set: &Bytes) -> usize {
        match self.sets.get(set) {
            Some(set) => set.len(),
            None => 0,
        }
    }

    pub fn zscore(&self, set: &Bytes, name: &Bytes) -> Option<f64> {
        match self.sets.get(set) {
            Some(set) => set.get(name),
            None => None,
        }
    }

    pub fn zrem(&mut self, set: &Bytes, name: &Bytes) -> usize {
        match self.sets.get_mut(set) {
            Some(set) => set.remove(name),
            None => 0,
        }
    }
}

#[derive(Debug)]
struct SortedSet {
    map: HashMap<Bytes, f64>,
    set: SkipSet<SetEntry>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            set: SkipSet::new(),
        }
    }

    pub fn add(&mut self, name: &Bytes, score: f64) -> usize {
        if self.map.contains_key(name) {
            0
        } else {
            self.map.insert(name.clone(), score);
            self.set.insert((OrderedFloat(score), name.clone()));
            1
        }
    }

    pub fn get(&self, name: &Bytes) -> Option<f64> {
        match self.map.get(name) {
            Some(score) => {
                let target = (OrderedFloat(*score), name.clone());
                let Some(entry) = self.set.get(&target) else {
                    panic!("expected entry in set - got None");
                };

                Some(entry.0.into_inner())
            }
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn rank(&self, name: &Bytes) -> Option<usize> {
        match self.map.get(name) {
            Some(score) => {
                let target = (OrderedFloat(*score), name.clone());
                if self.set.get(&target).is_none() {
                    return None;
                }

                let count = self.set.range(..=target).count();
                Some(count)
            }
            None => None,
        }
    }

    pub fn range(&self, start: i32, end: i32) -> Vec<Bytes> {
        let start = self.idx_converter(start);
        let mut end = self.idx_converter(end);

        if start >= self.set.len() || start > end {
            return Vec::new();
        }

        if end >= self.set.len() {
            end = self.set.len() - 1;
        }

        let members: Vec<Bytes> = self.set.iter().map(|entry| entry.1.clone()).collect();

        members[start..=end].to_vec()
    }

    pub fn remove(&mut self, name: &Bytes) -> usize {
        if self.map.contains_key(name) {
            let score = self
                .map
                .remove(name)
                .expect("exists because of check above");

            let target = (OrderedFloat(score), name.clone());
            self.set.remove(&target);
            1
        } else {
            0
        }
    }

    fn idx_converter(&self, idx: i32) -> usize {
        if idx < 0 {
            if idx.abs() >= self.set.len() as i32 {
                return 0;
            }

            (self.set.len() as i32 + idx) as usize
        } else {
            idx as usize
        }
    }
}
