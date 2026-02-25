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
    offset: usize,
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
            offset: 0,
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

    // HAX: This is rotten but meh
    async fn catch_trailing(&mut self) -> Result<(), RedisError> {
        let mut fragments = Vec::new();
        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(value) => match &value {
                    Value::String(_) => {
                        fragments.push(value);
                        if fragments.len() < 3 {
                            continue;
                        }

                        match RedisCommand::new(&Value::Array(fragments.clone())) {
                            Ok(cmd) => {
                                self.process_command(cmd).await?;
                                break;
                            }
                            Err(_) => {}
                        }
                    }
                    _ => {
                        eprintln!("SOMETHING WRONG WITH TRAILING COMMAND");
                    }
                },
                Err(e) => {
                    eprintln!("REPL ERROR: {e:?}");
                    return Err(RedisError::UnexpectedValue);
                }
            }
        }

        Ok(())
    }

    pub async fn replicate(&mut self) -> Result<(), RedisError> {
        self.handshake().await?;
        self.catch_trailing().await?;

        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(value) => {
                    let cmd = RedisCommand::new(&value)?;
                    eprintln!("RAW: {value:?} CMD SIZE: {}", cmd.size());
                    self.process_command(cmd).await?;
                }
                Err(e) => {
                    eprintln!("REPL PARSE ERROR {e:#?}");
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn process_command(&mut self, request: RedisCommand) -> Result<(), RedisError> {
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

            CommandType::ReplConf => {
                validate_args_len(&request, 2)?;
                let arg = &request.args[0];
                let _value = &request.args[1];
                if **arg == *b"GETACK" {
                    let value = format!("{}", self.offset);
                    let response = Value::Array(vec![
                        Value::String("REPLCONF".into()),
                        Value::String("ACK".into()),
                        Value::String(value.into()),
                    ]);
                    self.stream.send(response).await?;
                }
            }
            _ => {}
        }

        self.offset += request.size();
        Ok(())
    }
}
