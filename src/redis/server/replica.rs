use std::sync::Arc;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::redis::{
    protocol::{CommandType, RedisCommand, RedisError, RespProtocol, Value},
    stores::GlobalStore,
    utils::validate_args_len,
};

pub struct ReplicaMasterConnection {
    repl_port: u16,
    stream: Framed<TcpStream, RespProtocol>,
    store: Arc<GlobalStore>,
}

impl ReplicaMasterConnection {
    pub async fn new(
        addr: String,
        port: u16,
        repl_port: u16,
        store: Arc<GlobalStore>,
    ) -> Result<Self, RedisError> {
        let stream = TcpStream::connect(format!("{addr}:{port}")).await?;

        Ok(Self {
            store,
            repl_port,
            stream: Framed::new(stream, RespProtocol),
        })
    }

    async fn handshake(&mut self) -> Result<(), RedisError> {
        self.stream
            .send(Value::Array(vec![Value::String("PING".into())]))
            .await?;

        let _ = self.stream.next().await;

        let repl_conf1 = vec![
            Value::String("REPLCONF".into()),
            Value::String("listening-port".into()),
            Value::String(format!("{}", self.repl_port).into()),
        ];
        self.stream.send(Value::Array(repl_conf1)).await?;
        let _ = self.stream.next().await;

        let repl_conf2 = vec![
            Value::String("REPLCONF".into()),
            Value::String("capa".into()),
            Value::String("psync2".into()),
        ];
        self.stream.send(Value::Array(repl_conf2)).await?;
        let _ = self.stream.next().await;

        let psync = vec![
            Value::String("PSYNC".into()),
            Value::String("?".into()),
            Value::String("-1".into()),
        ];
        self.stream.send(Value::Array(psync)).await?;
        let _ = self.stream.next().await;
        let _rdb = self.stream.next().await;

        Ok(())
    }

    pub async fn replicate(&mut self) -> Result<(), RedisError> {
        self.handshake().await?;
        let mut holder = Vec::new();

        // FIXME: The Command is broken after RDB is sent so we need to gather it up
        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(value) => {
                    eprintln!("RAW: {value:?}");
                    match value {
                        Value::String(_) => holder.push(value.clone()),
                        _ => {
                            if !holder.is_empty() {
                                let broken_cmd = RedisCommand::new(&Value::Array(holder.clone()))?;
                                holder.clear();

                                let cmd = RedisCommand::new(&value)?;
                                self.process_command(broken_cmd).await?;
                                self.process_command(cmd).await?;
                            } else {
                                let cmd = RedisCommand::new(&value)?;
                                self.process_command(cmd).await?;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("REPL PARSE ERROR {e:#?}");
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn process_command(&self, request: RedisCommand) -> Result<(), RedisError> {
        match request.cmd {
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
                store.set(key, value, ttl)?;
            }
            _ => {}
        }
        Ok(())
    }
}
