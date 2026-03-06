use std::collections::HashMap;

use bytes::Bytes;
use kanal::AsyncSender;

use crate::redis::protocol::{RedisError, Value};

type SubscriberChannel = AsyncSender<Vec<Value>>;

#[derive(Debug, Clone)]
pub struct Topic {
    name: Bytes,
    subscribers: HashMap<Bytes, SubscriberChannel>,
}

impl Topic {
    pub async fn publish_message(&self, msg: Bytes) -> Result<usize, RedisError> {
        for sub in self.subscribers.values() {
            let msg = Value::Array(vec![
                Value::String("message".into()),
                Value::String(self.name.clone()),
                Value::String(msg.clone()),
            ]);

            sub.send(vec![msg])
                .await
                .map_err(|_| RedisError::ChannelSendError)?
        }

        Ok(self.subscribers.len())
    }
}

#[derive(Debug, Clone)]
pub struct PubSubStore {
    subscribers: HashMap<Bytes, usize>,
    topics: HashMap<Bytes, Topic>,
}

impl PubSubStore {
    pub fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            topics: HashMap::new(),
        }
    }

    pub fn is_subscribed(&self, client_id: &Bytes) -> bool {
        self.subscribers.contains_key(client_id)
    }

    pub fn subscribe(
        &mut self,
        channel_name: Bytes,
        client_id: &Bytes,
        responder: SubscriberChannel,
    ) -> usize {
        self.topics
            .entry(channel_name.clone())
            .and_modify(|ch| {
                ch.subscribers.insert(client_id.clone(), responder.clone());
            })
            .or_insert_with(|| {
                let mut subs = HashMap::new();
                subs.insert(client_id.clone(), responder);

                Topic {
                    name: channel_name,
                    subscribers: subs,
                }
            });

        let channel_count = self
            .subscribers
            .entry(client_id.clone())
            .and_modify(|e| *e += 1)
            .or_insert(1);

        *channel_count
    }

    pub fn unsubscribe(&mut self, channel_name: &Bytes, client_id: &Bytes) -> usize {
        if let Some(topic) = self.topics.get_mut(channel_name) {
            if topic.subscribers.contains_key(client_id) {
                topic.subscribers.remove(client_id);
                self.subscribers
                    .entry(client_id.clone())
                    .and_modify(|c| *c -= 1);
            }
        }

        match self.subscribers.get(client_id) {
            Some(count) => {
                if *count == 0 {
                    self.subscribers.remove(client_id);
                    return 0;
                }

                *count
            }
            None => 0,
        }
    }

    pub fn get_topic(&self, channel_name: &Bytes) -> Option<&Topic> {
        self.topics.get(channel_name)
    }
}
