use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use bytes::Bytes;
use kanal::{AsyncReceiver, AsyncSender};

struct Interest {
    interest: BTreeSet<Bytes>,
    timestamp: Instant,
    sender: AsyncSender<Bytes>,
}

pub struct Notifier {
    clients: BTreeMap<Bytes, Interest>,
    pub backlog: Vec<Bytes>,
}

impl Notifier {
    pub fn new() -> Self {
        Self {
            clients: BTreeMap::default(),
            backlog: Vec::new(),
        }
    }

    pub fn add_to_backlog(&mut self, item: Bytes) {
        self.backlog.push(item);
    }

    pub fn register_client(&mut self, id: Bytes, interest: &[Bytes]) -> AsyncReceiver<Bytes> {
        let (sender, receiver) = kanal::unbounded_async();

        self.clients.insert(
            id,
            Interest {
                interest: BTreeSet::from_iter(interest.iter().cloned()),
                timestamp: Instant::now(),
                sender: sender.clone(),
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
