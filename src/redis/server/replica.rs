use std::sync::Arc;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::redis::{
    protocol::{RedisError, RespProtocol, Value},
    stores::GlobalStore,
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
            .send(Value::Array(vec![Value::SimpleString("PING".into())]))
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

        Ok(())
    }

    pub async fn replicate(&mut self) -> Result<(), RedisError> {
        self.handshake().await?;
        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(_value) => {
                    todo!("handle replication")
                }
                Err(e) => {
                    eprintln!("{e:#?}");
                    break;
                }
            }
        }

        Ok(())
    }
}
