use kanal::{AsyncReceiver, AsyncSender};

pub mod protocol;
mod store;
use protocol::{RedisError, Value};
use store::MapStore;

pub type Request = (Value, AsyncSender<Vec<Value>>);

pub struct Node {
    request_channel: AsyncReceiver<Request>,
    map_store: MapStore,
}

impl Node {
    pub fn new(rx: AsyncReceiver<Request>) -> Self {
        Self {
            request_channel: rx,
            map_store: MapStore::new(),
        }
    }

    pub async fn process(&mut self) -> Result<(), RedisError> {
        while let Ok((req, responder)) = self.request_channel.recv().await {
            let Value::Array(args) = req else {
                return Err(RedisError::UnexpectedValue);
            };

            let Value::String(cmd_bytes) = &args[0] else {
                return Err(RedisError::UnexpectedValue);
            };

            match str::from_utf8(&cmd_bytes[..])
                .map_err(|_| RedisError::StringConversion)?
                .to_lowercase()
                .as_str()
            {
                "ping" => responder
                    .send(vec![Value::SimpleString("PONG".into())])
                    .await
                    .map_err(|_| RedisError::ChannelSendError)?,

                "echo" => {
                    let Some(Value::String(arg)) = args.get(1) else {
                        return Err(RedisError::UnexpectedValue);
                    };

                    responder
                        .send(vec![Value::String(arg.clone())])
                        .await
                        .map_err(|_| RedisError::ChannelSendError)?;
                }
                "get" => {
                    let Some(Value::String(key)) = args.get(1) else {
                        return Err(RedisError::UnexpectedValue);
                    };

                    match self.map_store.get(key) {
                        Some(value) => responder
                            .send(vec![Value::String(value.clone())])
                            .await
                            .map_err(|_| RedisError::ChannelSendError)?,
                        None => responder
                            .send(vec![Value::NullString])
                            .await
                            .map_err(|_| RedisError::ChannelSendError)?,
                    }
                }
                "set" => {
                    let has_expiry = args[1..].len() == 4;
                    let Some(Value::String(key)) = args.get(1) else {
                        return Err(RedisError::UnexpectedValue);
                    };

                    let Some(Value::String(value)) = args.get(2) else {
                        return Err(RedisError::UnexpectedValue);
                    };

                    let ttl = if has_expiry {
                        let Some(Value::String(ex)) = args.get(4) else {
                            return Err(RedisError::UnexpectedValue);
                        };

                        Some(ex.clone())
                    } else {
                        None
                    };

                    self.map_store.set(key.clone(), value.clone(), ttl)?;
                    responder
                        .send(vec![Value::ok()])
                        .await
                        .map_err(|_| RedisError::ChannelSendError)?
                }
                _ => {}
            }
        }

        Ok(())
    }
}
