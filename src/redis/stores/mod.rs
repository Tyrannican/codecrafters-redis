mod list;
mod map;
mod notifier;
mod queue;
mod stream;

use bytes::Bytes;
use list::ListStore;
use map::MapStore;
use notifier::Notifier;
use queue::TransactionStore;
use stream::StreamStore;

use kanal::{AsyncReceiver, AsyncSender};

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use super::protocol::RedisError;

pub struct GlobalStore {
    replicas: AtomicUsize,
    notifier: RwLock<Notifier>,
    maps: RwLock<MapStore>,
    lists: RwLock<ListStore>,
    streams: RwLock<StreamStore>,
    txns: RwLock<TransactionStore>,
}

impl GlobalStore {
    pub fn new() -> Self {
        Self {
            replicas: AtomicUsize::new(0),
            notifier: RwLock::new(Notifier::new()),
            maps: RwLock::new(MapStore::new()),
            lists: RwLock::new(ListStore::new()),
            streams: RwLock::new(StreamStore::new()),
            txns: RwLock::new(TransactionStore::new()),
        }
    }

    pub async fn register_interest(
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

    pub fn add_replica(&self) {
        self.replicas.fetch_add(1, Ordering::SeqCst);
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn notifier_reader(&self) -> Result<RwLockReadGuard<'_, Notifier>, RedisError> {
        self.notifier.read().map_err(|_| RedisError::ReadLock)
    }

    pub fn notifier_writer(&self) -> Result<RwLockWriteGuard<'_, Notifier>, RedisError> {
        self.notifier.write().map_err(|_| RedisError::WriteLock)
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

    pub fn _transaction_reader(&self) -> Result<RwLockReadGuard<'_, TransactionStore>, RedisError> {
        self.txns.read().map_err(|_| RedisError::ReadLock)
    }

    pub fn transaction_writer(&self) -> Result<RwLockWriteGuard<'_, TransactionStore>, RedisError> {
        self.txns.write().map_err(|_| RedisError::WriteLock)
    }

    pub fn key_type(&self, key: &Bytes) -> Result<Bytes, RedisError> {
        let map = self.map_reader()?;
        if map.contains(key) {
            return Ok("string".into());
        }

        let list = self.list_reader()?;
        if list.contains(key) {
            return Ok("list".into());
        }

        let stream = self.stream_reader()?;

        if stream.contains(key) {
            return Ok("stream".into());
        }

        Ok("none".into())
    }
}
