use kanal::{AsyncReceiver, AsyncSender};

pub mod protocol;
mod stores;
mod utils;
use protocol::{CommandType, RedisCommand, RedisError, Value};
use stores::{ListStore, MapStore};
use utils::bytes_to_integer;

pub type Request = (Value, AsyncSender<Vec<Value>>);

pub struct Node {
    request_channel: AsyncReceiver<Request>,
    map_store: MapStore,
    list_store: ListStore,
}

impl Node {
    pub fn new(rx: AsyncReceiver<Request>) -> Self {
        Self {
            request_channel: rx,
            map_store: MapStore::new(),
            list_store: ListStore::new(),
        }
    }

    pub async fn process(&mut self) -> Result<(), RedisError> {
        while let Ok((req, responder)) = self.request_channel.recv().await {
            let req = RedisCommand::new(req)?;
            let mut resp = Vec::new();

            match req.cmd {
                CommandType::Ping => resp.push(Value::SimpleString("PONG".into())),
                CommandType::Echo => {
                    if req.args.is_empty() {
                        resp.push(Value::error("insuffient arguments for echo"));
                    } else {
                        let msg = &req.args[0];
                        resp.push(Value::String(msg.clone()));
                    }
                }
                CommandType::Get => {
                    if req.args.is_empty() {
                        resp.push(Value::error("insuffient arguments for get"));
                    } else {
                        let key = &req.args[0];
                        match self.map_store.get(key) {
                            Some(value) => resp.push(Value::String(value.clone())),
                            None => resp.push(Value::NullString),
                        }
                    }
                }
                CommandType::Set => {
                    if req.args.len() < 2 {
                        resp.push(Value::error("insufficient arguments for set"));
                    } else {
                        let key = &req.args[0];
                        let value = &req.args[1];
                        let ttl = if req.args.len() == 4 {
                            Some(req.args[3].clone())
                        } else {
                            None
                        };

                        match self.map_store.set(key, value, ttl) {
                            Ok(_) => resp.push(Value::ok()),
                            Err(e) => resp.push(Value::Error(e.to_string().into())),
                        }
                    }
                }
                CommandType::RPush => {
                    if req.args.len() < 2 {
                        resp.push(Value::error("insufficient arguments for rpush"));
                    } else {
                        let mut size = 0;
                        let key = &req.args[0];
                        for value in req.args[1..].iter() {
                            size = self.list_store.append(key, value);
                        }

                        resp.push(Value::Integer(size as i64));
                    }
                }
                CommandType::LPush => {
                    if req.args.len() < 2 {
                        resp.push(Value::error("insufficient arguments for rpush"));
                    } else {
                        let mut size = 0;
                        let key = &req.args[0];
                        for value in req.args[1..].iter() {
                            size = self.list_store.prepend(key, value);
                        }

                        resp.push(Value::Integer(size as i64));
                    }
                }
                CommandType::LRange | CommandType::RRange => {
                    if req.args.len() != 3 {
                        resp.push(Value::error("insufficient arguments for rpush"));
                    } else {
                        let key = &req.args[0];
                        let start = bytes_to_integer(&req.args[1])?;
                        let end = bytes_to_integer(&req.args[2])?;
                        match self.list_store.slice(key, start, end) {
                            Some(slice) => {
                                let values = slice
                                    .into_iter()
                                    .map(|v| Value::String(v.clone()))
                                    .collect::<Vec<Value>>();

                                resp.push(Value::Array(values));
                            }
                            None => resp.push(Value::EmptyArray),
                        }
                    }
                }
                CommandType::LLen => {
                    if req.args.is_empty() {
                        resp.push(Value::error("insufficient arguments for llen"));
                    } else {
                        let key = &req.args[0];
                        let size = self.list_store.len(key);
                        resp.push(Value::Integer(size as i64));
                    }
                }
                CommandType::LPop => {
                    if req.args.is_empty() {
                        resp.push(Value::error("insufficient arguments for lpop"));
                    } else {
                        let key = &req.args[0];
                        let to_remove = match req.args.get(1) {
                            Some(total) => bytes_to_integer(total)? as usize,
                            None => 1,
                        };

                        match self.list_store.remove(key, to_remove) {
                            Some(elements) => {
                                if elements.len() == 1 {
                                    resp.push(Value::String(elements[0].clone()))
                                } else {
                                    let values = elements
                                        .into_iter()
                                        .map(|v| Value::String(v))
                                        .collect::<Vec<Value>>();
                                    resp.push(Value::Array(values));
                                }
                            }
                            None => resp.push(Value::NullString),
                        }
                    }
                }
                _ => todo!(),
            }

            responder
                .send(resp)
                .await
                .map_err(|_| RedisError::ChannelSendError)?;
        }

        Ok(())
    }
}
