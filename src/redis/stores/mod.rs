pub mod list;
pub mod map;
pub mod stream;

use bytes::Bytes;
use list::ListStore;
use map::MapStore;
use stream::StreamStore;

use kanal::{AsyncReceiver, AsyncSender};

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

    pub fn register_client(&mut self, id: Bytes, interest: &[Bytes]) -> AsyncReceiver<Bytes> {
        let (sender, receiver) = kanal::unbounded_async();
        self.clients.insert(
            id,
            Interest {
                interest: BTreeSet::from_iter(interest.iter().cloned()),
                timestamp: Instant::now(),
                sender,
            },
        );

        receiver
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
    streams: RwLock<StreamStore>,
}

impl GlobalStore {
    pub fn new() -> Self {
        Self {
            notifier: RwLock::new(Notifier::new()),
            maps: RwLock::new(MapStore::new()),
            lists: RwLock::new(ListStore::new()),
            streams: RwLock::new(StreamStore::new()),
        }
    }

    pub fn register_interest(
        &self,
        id: Bytes,
        interest: &[Bytes],
    ) -> Result<AsyncReceiver<Bytes>, RedisError> {
        let mut notifier = self.notifier.write().map_err(|_| RedisError::WriteLock)?;
        let receiver = notifier.register_client(id, interest);

        Ok(receiver)
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

    pub fn stream_reader(&self) -> Result<RwLockReadGuard<'_, StreamStore>, RedisError> {
        self.streams.read().map_err(|_| RedisError::ReadLock)
    }

    pub fn stream_writer(&self) -> Result<RwLockWriteGuard<'_, StreamStore>, RedisError> {
        self.streams.write().map_err(|_| RedisError::WriteLock)
    }

    pub fn key_type<'a>(&self, key: &Bytes) -> Result<&'a str, RedisError> {
        let map = self.map_reader()?;
        if map.contains(key) {
            return Ok("string");
        }

        let list = self.list_reader()?;
        if list.contains(key) {
            return Ok("list");
        }

        let stream = self.stream_reader()?;
        if stream.contains(key) {
            return Ok("stream");
        }

        Ok("none")
    }
}
