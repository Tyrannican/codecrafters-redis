pub mod list;
pub mod map;

use bytes::Bytes;
use list::ListStore;
use map::MapStore;

use kanal::AsyncSender;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Instant,
};

use super::protocol::RedisError;

struct Interest {
    interest: BTreeSet<Bytes>,
    timestamp: Instant,
    sender: AsyncSender<Bytes>,
}

pub struct Notifier {
    clients: BTreeMap<Bytes, Interest>,
}

impl Notifier {
    pub fn new() -> Self {
        Self {
            clients: BTreeMap::default(),
        }
    }

    pub fn register_client(&mut self, id: Bytes, interest: &[Bytes], sender: AsyncSender<Bytes>) {
        self.clients.insert(
            id,
            Interest {
                interest: BTreeSet::from_iter(interest.iter().cloned()),
                timestamp: Instant::now(),
                sender,
            },
        );
    }

    pub fn unregister_client(&mut self, id: &Bytes) {
        self.clients.remove(id);
    }

    pub fn client_sender(&self, msg: &Bytes) -> Option<AsyncSender<Bytes>> {
        match self.longest_waiting_client() {
            Some(client) => {
                if client.interest.contains(msg) {
                    return Some(client.sender.clone());
                }

                None
            }
            None => None,
        }
    }

    fn longest_waiting_client(&self) -> Option<&Interest> {
        match self.clients.iter().max_by(|a, b| {
            a.1.timestamp
                .elapsed()
                .as_millis()
                .cmp(&b.1.timestamp.elapsed().as_millis())
        }) {
            Some(client) => Some(client.1),
            None => None,
        }
    }
}

pub struct GlobalStore {
    notifier: RwLock<Notifier>,
    maps: RwLock<MapStore>,
    lists: RwLock<ListStore>,
}

impl GlobalStore {
    pub fn new() -> Self {
        Self {
            notifier: RwLock::new(Notifier::new()),
            maps: RwLock::new(MapStore::new()),
            lists: RwLock::new(ListStore::new()),
        }
    }

    pub fn register_interest(
        &self,
        id: Bytes,
        interest: &[Bytes],
        sender: AsyncSender<Bytes>,
    ) -> Result<(), RedisError> {
        let mut notifier = self.notifier.write().map_err(|_| RedisError::WriteLock)?;
        notifier.register_client(id, interest, sender);

        Ok(())
    }

    pub fn unregister_interest(&self, id: &Bytes) -> Result<(), RedisError> {
        let mut notifier = self.notifier.write().map_err(|_| RedisError::WriteLock)?;
        notifier.unregister_client(id);
        Ok(())
    }

    pub fn client_sender(&self, msg: &Bytes) -> Result<Option<AsyncSender<Bytes>>, RedisError> {
        let notifier = self.notifier.read().map_err(|_| RedisError::ReadLock)?;
        Ok(notifier.client_sender(msg))
    }

    pub fn map_reader(&self) -> Result<RwLockReadGuard<'_, MapStore>, RedisError> {
        self.maps.read().map_err(|_| RedisError::ReadLock)
    }

    pub fn map_writer(&self) -> Result<RwLockWriteGuard<'_, MapStore>, RedisError> {
        self.maps.write().map_err(|_| RedisError::WriteLock)
    }

    pub fn list_reader(&self) -> Result<RwLockReadGuard<'_, ListStore>, RedisError> {
        self.lists.read().map_err(|_| RedisError::ReadLock)
    }

    pub fn list_writer(&self) -> Result<RwLockWriteGuard<'_, ListStore>, RedisError> {
        self.lists.write().map_err(|_| RedisError::WriteLock)
    }
}
