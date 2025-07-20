use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;

pub mod protocol;
mod stores;
mod utils;

use protocol::{CommandType, RedisCommand, RedisError, Value};
use stores::{ListStore, MapStore};
use utils::{bytes_to_integer, validate_args_len};

pub type Request = (Value, Bytes, AsyncSender<Vec<Value>>);

pub struct Node {
    worker_count: usize,
    pool: BTreeMap<usize, JoinHandle<Result<(), RedisError>>>,
    map_store: Arc<RwLock<MapStore>>,
    list_store: Arc<RwLock<ListStore>>,
}

impl Node {
    pub fn new(worker_count: usize) -> Self {
        Self {
            worker_count,
            pool: BTreeMap::new(),
            map_store: Arc::new(RwLock::new(MapStore::new())),
            list_store: Arc::new(RwLock::new(ListStore::new())),
        }
    }

    pub fn start(&mut self, receiver: AsyncReceiver<Request>) {
        for i in 0..self.worker_count {
            let rx = receiver.clone();
            let map_store = Arc::clone(&self.map_store);
            let list_store = Arc::clone(&self.list_store);

            let handle =
                tokio::task::spawn(async move { worker_fn(rx, map_store, list_store).await });

            self.pool.insert(i, handle);
        }
    }
}

async fn worker_fn(
    rx: AsyncReceiver<Request>,
    map_store: Arc<RwLock<MapStore>>,
    list_store: Arc<RwLock<ListStore>>,
) -> Result<(), RedisError> {
    while let Ok((req, client_id, responder)) = rx.recv().await {
        let mut task = WorkerTask {
            request: req,
            client_id,
            map_store: Arc::clone(&map_store),
            list_store: Arc::clone(&list_store),
        };

        match task.process_request().await {
            Ok(response) => {
                responder
                    .send(response)
                    .await
                    .map_err(|_| RedisError::ChannelSendError)?;
            }

            Err(e) => match e {
                RedisError::InsufficientArugments(cmd) => {
                    responder
                        .send(vec![Value::Error(
                            format!("insufficient arugments for command '{cmd}'").into(),
                        )])
                        .await
                        .map_err(|_| RedisError::ChannelSendError)?;
                }
                _ => return Err(e),
            },
        }
    }

    Ok(())
}

struct WorkerTask {
    request: Value,
    client_id: Bytes,
    map_store: Arc<RwLock<MapStore>>,
    list_store: Arc<RwLock<ListStore>>,
}

impl WorkerTask {
    pub async fn process_request(&mut self) -> Result<Vec<Value>, RedisError> {
        let mut response = Vec::new();
        let request = RedisCommand::new(&self.request)?;

        match request.cmd {
            CommandType::Ping => response.push(Value::SimpleString("PONG".into())),
            CommandType::Echo => {
                validate_args_len(&request, 1)?;

                let msg = &request.args[0];
                response.push(Value::String(msg.clone()));
            }
            CommandType::Get => {
                validate_args_len(&request, 1)?;

                let key = &request.args[0];
                let store = self.map_store.read().map_err(|_| RedisError::ReadLock)?;

                match store.get(key) {
                    Some(value) => {
                        response.push(Value::String(value.clone()));
                    }
                    None => response.push(Value::NullString),
                }
            }
            CommandType::Set => {
                validate_args_len(&request, 2)?;

                let key = &request.args[0];
                let value = &request.args[1];
                let ttl = if request.args.len() == 4 {
                    Some(request.args[3].clone())
                } else {
                    None
                };

                let mut store = self.map_store.write().map_err(|_| RedisError::WriteLock)?;

                match store.set(key, value, ttl) {
                    Ok(_) => response.push(Value::ok()),
                    Err(e) => response.push(Value::Error(e.to_string().into())),
                }
            }
            CommandType::RPush => {
                validate_args_len(&request, 2)?;

                let mut size = 0;
                let key = &request.args[0];
                let mut store = self.list_store.write().map_err(|_| RedisError::WriteLock)?;

                for value in request.args[1..].iter() {
                    size = store.append(key, value);
                }

                response.push(Value::Integer(size as i64));
            }
            CommandType::LPush => {
                validate_args_len(&request, 2)?;

                let mut size = 0;
                let key = &request.args[0];
                let mut store = self.list_store.write().map_err(|_| RedisError::WriteLock)?;

                for value in request.args[1..].iter() {
                    size = store.prepend(key, value);
                }

                response.push(Value::Integer(size as i64));
            }
            CommandType::LRange | CommandType::RRange => {
                validate_args_len(&request, 3)?;

                let key = &request.args[0];
                let start = bytes_to_integer(&request.args[1])?;
                let end = bytes_to_integer(&request.args[2])?;
                let store = self.list_store.read().map_err(|_| RedisError::ReadLock)?;

                match store.slice(key, start, end) {
                    Some(slice) => {
                        let values = slice
                            .into_iter()
                            .map(|v| Value::String(v.clone()))
                            .collect::<Vec<Value>>();

                        response.push(Value::Array(values));
                    }
                    None => response.push(Value::EmptyArray),
                }
            }
            CommandType::LLen => {
                validate_args_len(&request, 1)?;

                let key = &request.args[0];
                let store = self.list_store.read().map_err(|_| RedisError::ReadLock)?;

                let size = store.len(key);

                response.push(Value::Integer(size as i64));
            }
            CommandType::LPop => {
                validate_args_len(&request, 1)?;

                let key = &request.args[0];
                let to_remove = match request.args.get(1) {
                    None => 1,
                    Some(total) => bytes_to_integer(total)? as usize,
                };

                let mut store = self.list_store.write().map_err(|_| RedisError::WriteLock)?;

                match store.remove(key, to_remove) {
                    Some(elements) => match elements.len() {
                        0 => response.push(Value::NullString),
                        1 => response.push(Value::String(elements[0].clone())),
                        _ => {
                            let values = elements
                                .into_iter()
                                .map(|v| Value::String(v))
                                .collect::<Vec<Value>>();

                            response.push(Value::Array(values));
                        }
                    },
                    None => response.push(Value::NullString),
                }
            }
            CommandType::BLPop => {}
        }

        Ok(response)
    }
}
