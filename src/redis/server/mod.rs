use std::sync::Arc;
use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;

use super::protocol::{CommandType, RedisCommand, RedisError, Value};
use super::stores::GlobalStore;
use super::utils::{bytes_to_number, empty_rdb, validate_args_len};

mod replica;
use replica::ReplicaMasterConnection;

const WORKER_COUNT: usize = 10;

pub type Request = (Value, Bytes, AsyncSender<Vec<Value>>);

pub enum ServerRole {
    Master,
    Replica((String, u16)),
}

impl ServerRole {
    pub fn replica_address(&self) -> Option<(String, u16)> {
        match self {
            Self::Master => None,
            Self::Replica((addr, port)) => Some((addr.to_owned(), *port)),
        }
    }
}

impl std::fmt::Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master => write!(f, "master"),
            Self::Replica(_) => write!(f, "slave"),
        }
    }
}

pub struct RedisServer {
    role: Arc<ServerRole>,
    port: u16,
    worker_count: usize,
    pool: BTreeMap<usize, JoinHandle<Result<(), RedisError>>>,
    store: Arc<GlobalStore>,
}

impl RedisServer {
    pub fn new(role: ServerRole, port: u16) -> Self {
        Self {
            port,
            role: Arc::new(role),
            worker_count: WORKER_COUNT,
            pool: BTreeMap::new(),
            store: Arc::new(GlobalStore::new()),
        }
    }

    pub fn start(&mut self, receiver: AsyncReceiver<Request>) {
        if let Some((master_addr, master_port)) = self.role.replica_address() {
            let port = self.port;
            let store = Arc::clone(&self.store);
            tokio::task::spawn(async move {
                let mut master_connection =
                    ReplicaMasterConnection::new(master_addr, master_port, port, store).await?;

                master_connection.replicate().await
            });
        }

        for i in 0..self.worker_count {
            let rx = receiver.clone();
            let store = Arc::clone(&self.store);
            let role = Arc::clone(&self.role);
            let mut worker = Worker::new(store, role, rx);
            let handle = tokio::task::spawn(async move { worker.start().await });

            self.pool.insert(i, handle);
        }

        // TODO: Some kind of recovery mechanism
    }
}

pub struct Worker {
    store: Arc<GlobalStore>,
    role: Arc<ServerRole>,
    receiver: AsyncReceiver<Request>,
}

impl Worker {
    pub fn new(
        store: Arc<GlobalStore>,
        role: Arc<ServerRole>,
        receiver: AsyncReceiver<Request>,
    ) -> Self {
        Self {
            store,
            role,
            receiver,
        }
    }

    pub async fn start(&mut self) -> Result<(), RedisError> {
        while let Ok((req, client_id, responder)) = self.receiver.recv().await {
            match self.process_request(req, client_id).await {
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

    async fn process_request(
        &mut self,
        request: Value,
        client_id: Bytes,
    ) -> Result<Vec<Value>, RedisError> {
        let mut response = Vec::new();
        let request = RedisCommand::new(&request)?;

        {
            let mut txn_writer = self.store.transaction_writer()?;
            if txn_writer.has_transaction(&client_id) {
                match request.cmd {
                    CommandType::Exec | CommandType::Discard => {}
                    _ => {
                        txn_writer.add_to_transaction(&client_id, request);
                        response.push(Value::SimpleString("QUEUED".into()));
                        return Ok(response);
                    }
                }
            }
        }

        let response = self.execute_command(request, client_id).await?;

        Ok(response)
    }

    async fn execute_command(
        &mut self,
        request: RedisCommand,
        client_id: Bytes,
    ) -> Result<Vec<Value>, RedisError> {
        let mut response = Vec::new();
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
                let store = self.store.map_reader()?;

                match store.get(key) {
                    Some(value) => response.push(Value::String(value.clone())),
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

                let mut store = self.store.map_writer()?;
                match store.set(key, value, ttl) {
                    Ok(_) => response.push(Value::ok()),
                    Err(e) => response.push(Value::Error(e.to_string().into())),
                }
            }
            CommandType::RPush => {
                validate_args_len(&request, 2)?;

                let mut size = 0;
                let key = &request.args[0];
                {
                    let mut store = self.store.list_writer()?;

                    for value in request.args[1..].iter() {
                        size = store.append(key, value);
                    }
                }

                if let Some(sender) = self.store.client_sender(key)? {
                    sender
                        .send(key.clone())
                        .await
                        .map_err(|_| RedisError::ChannelSendError)?;
                }

                response.push(Value::Integer(size as i64));
            }
            CommandType::LPush => {
                validate_args_len(&request, 2)?;

                let mut size = 0;
                let key = &request.args[0];
                let mut store = self.store.list_writer()?;

                for value in request.args[1..].iter() {
                    size = store.prepend(key, value);
                }

                response.push(Value::Integer(size as i64));
            }
            CommandType::LRange | CommandType::RRange => {
                validate_args_len(&request, 3)?;

                let key = &request.args[0];
                let start = bytes_to_number(&request.args[1])?;
                let end = bytes_to_number(&request.args[2])?;

                let store = self.store.list_reader()?;
                match store.slice(key, start, end) {
                    Some(slice) => {
                        let values = slice
                            .iter()
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
                let store = self.store.list_reader()?;

                let size = store.len(key);

                response.push(Value::Integer(size as i64));
            }
            CommandType::LPop => {
                validate_args_len(&request, 1)?;

                let key = &request.args[0];
                let to_remove = match request.args.get(1) {
                    None => 1,
                    Some(total) => bytes_to_number::<usize>(total)?,
                };

                let mut store = self.store.list_writer()?;

                match store.remove(key, to_remove) {
                    Some(elements) => match elements.len() {
                        0 => response.push(Value::NullString),
                        1 => response.push(Value::String(elements[0].clone())),
                        _ => {
                            let values = elements
                                .into_iter()
                                .map(Value::String)
                                .collect::<Vec<Value>>();

                            response.push(Value::Array(values));
                        }
                    },
                    None => response.push(Value::NullString),
                }
            }
            CommandType::BLPop => {
                validate_args_len(&request, 2)?;
                let keys = &request.args[..&request.args.len() - 1];
                let timeout = &request.args.last().unwrap();
                let rx = self.store.register_interest(client_id.clone(), keys)?;

                let timeout = bytes_to_number::<f64>(timeout)?;
                if timeout == 0.0 {
                    let key = rx.recv().await.map_err(|_| RedisError::ChannelSendError)?;
                    let mut writer = self.store.list_writer()?;
                    match writer.remove_single(&key) {
                        Some(value) => response.push(Value::Array(vec![
                            Value::String(key.clone()),
                            Value::String(value),
                        ])),
                        None => response.push(Value::NullString),
                    }
                } else {
                    match tokio::time::timeout(
                        Duration::from_millis((timeout * 1000.0) as u64),
                        rx.recv(),
                    )
                    .await
                    {
                        Ok(Ok(key)) => {
                            let mut writer = self.store.list_writer()?;

                            match writer.remove_single(&key) {
                                Some(value) => response.push(Value::Array(vec![
                                    Value::String(key.clone()),
                                    Value::String(value),
                                ])),
                                None => response.push(Value::NullArray),
                            }
                        }
                        _ => response.push(Value::NullArray),
                    }
                }

                self.store.unregister_interest(&client_id)?;
            }

            CommandType::Type => {
                validate_args_len(&request, 1)?;
                let key = &request.args[0];
                let key_type = self.store.key_type(key)?;
                response.push(Value::SimpleString(key_type));
            }
            CommandType::XAdd => {
                validate_args_len(&request, 2)?;

                let stream_key = &request.args[0];
                let entry_id = &request.args[1];

                let values = if request.args.len() > 2 {
                    if &request.args[2..].len() % 2 != 0 {
                        response.push(Value::error(
                            "need even number of keys and values for stream".into(),
                        ));

                        return Ok(response);
                    }

                    let pairs: Vec<(Bytes, Bytes)> = request.args[2..]
                        .chunks(2)
                        .map(|p| (p[0].clone(), p[1].clone()))
                        .collect();

                    Some(pairs)
                } else {
                    None
                };

                {
                    let mut store = self.store.stream_writer()?;
                    match store.add_entry(stream_key, entry_id, values.as_deref()) {
                        Ok(entry_key) => response.push(entry_key),
                        Err(e) => match e {
                            RedisError::StreamIdError(se) => response.push(Value::Error(se.into())),
                            _ => return Err(e),
                        },
                    }
                }

                if let Some(sender) = self.store.client_sender(stream_key)? {
                    sender
                        .send(stream_key.clone())
                        .await
                        .map_err(|_| RedisError::ChannelSendError)?;
                }
            }

            CommandType::XRange => {
                validate_args_len(&request, 3)?;

                let key = &request.args[0];
                let start = &request.args[1];
                let end = &request.args[2];
                let store = self.store.stream_reader()?;
                let values = store.xrange(key, start, end)?;
                response.push(values);
            }

            CommandType::XRead => {
                validate_args_len(&request, 3)?;
                let (timeout, keys) = if request.args.contains(&"block".into()) {
                    let timeout = &request.args[1];
                    let keys = &request.args[3..];
                    (Some(bytes_to_number::<usize>(timeout)?), keys)
                } else {
                    (None, &request.args[1..])
                };

                let mid = keys.len() / 2;
                let stream_keys = &keys[..mid];
                let entry_ids = &keys[mid..];

                match timeout {
                    Some(to) => {
                        let receiver = self.store.register_interest(client_id.clone(), keys)?;

                        if to == 0 {
                            if let Ok(v) = receiver.recv().await {
                                let store = self.store.stream_reader()?;
                                let result = store.xread(&[v], entry_ids);
                                response.push(result);
                            }
                        } else {
                            match tokio::time::timeout(
                                Duration::from_millis(to as u64),
                                receiver.recv(),
                            )
                            .await
                            {
                                Ok(Ok(item)) => {
                                    let store = self.store.stream_reader()?;
                                    let result = store.xread(&[item], entry_ids);
                                    response.push(result);
                                }
                                _ => response.push(Value::NullArray),
                            }
                        }

                        self.store.unregister_interest(&client_id)?;
                    }
                    None => {
                        let store = self.store.stream_reader()?;
                        let results = store.xread(stream_keys, entry_ids);
                        response.push(results);
                    }
                }
            }

            CommandType::Incr => {
                validate_args_len(&request, 1)?;

                let key = &request.args[0];
                let mut map = self.store.map_writer()?;
                match map.incr(key) {
                    Ok(value) => response.push(Value::Integer(value)),
                    Err(_) => response.push(Value::error(
                        "ERR value is not an integer or out of range".into(),
                    )),
                }
            }

            CommandType::Multi => {
                let mut writer = self.store.transaction_writer()?;
                writer.create_transaction(&client_id);
                response.push(Value::ok());
            }

            CommandType::Exec => {
                let txn = {
                    let mut writer = self.store.transaction_writer()?;
                    match writer.remove_transaction(&client_id) {
                        Some(txn) => txn,
                        None => {
                            response.push(Value::error("ERR EXEC without MULTI".into()));
                            return Ok(response);
                        }
                    }
                };

                let mut results = Vec::new();
                for cmd in txn.commands() {
                    results.push(Box::pin(self.execute_command(cmd, client_id.clone())).await?);
                }

                response = results.into_iter().flatten().collect();
                if response.is_empty() {
                    response.push(Value::Array(vec![]));
                } else {
                    response = vec![Value::Array(response)];
                }
            }

            CommandType::Discard => {
                let mut writer = self.store.transaction_writer()?;
                match writer.remove_transaction(&client_id) {
                    Some(_) => response.push(Value::ok()),
                    None => response.push(Value::error("ERR DISCARD without MULTI".into())),
                }
            }

            CommandType::Info => {
                validate_args_len(&request, 1)?;

                match str::from_utf8(&request.args[0]).map_err(|_| RedisError::StringConversion)? {
                    "replication" => {
                        let info_string = format!(
                            "role:{}\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0",
                            self.role
                        );
                        response.push(Value::String(info_string.into()));
                    }
                    _ => {}
                }
            }

            CommandType::ReplConf => {
                // TODO: Flesh out when required
                response.push(Value::ok());
            }

            CommandType::Psync => {
                // TODO: Flesh out when required
                response.push(Value::SimpleString(
                    "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".into(),
                ));

                let rdb = empty_rdb()?;
                response.push(Value::Rdb(rdb));
            }
        }

        Ok(response)
    }
}
