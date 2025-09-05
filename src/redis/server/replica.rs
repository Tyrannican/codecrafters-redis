use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use kanal::AsyncReceiver;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::redis::protocol::{RespProtocol, Value};

pub struct ReplicaMasterConnection {
    port: u16,
    repl_port: u16,
    replica_in: AsyncReceiver<Vec<Value>>,
    stream: Framed<TcpStream, RespProtocol>,
}

impl ReplicaMasterConnection {
    pub async fn new(
        addr: String,
        port: u16,
        repl_port: u16,
        replica_in: AsyncReceiver<Vec<Value>>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(format!("{addr}:{port}")).await?;

        Ok(Self {
            port,
            repl_port,
            replica_in,
            stream: Framed::new(stream, RespProtocol),
        })
    }

    async fn handshake(&mut self) -> Result<()> {
        self.stream
            .send(Value::Array(vec![Value::SimpleString("PING".into())]))
            .await?;

        let _ = self.stream.next();

        let repl_conf1 = vec![
            Value::String("REPLCONF".into()),
            Value::String("listening-port".into()),
            Value::String(format!("{}", self.repl_port).into()),
        ];
        self.stream.send(Value::Array(repl_conf1)).await?;

        let repl_conf2 = vec![
            Value::String("REPLCONF".into()),
            Value::String("capa".into()),
            Value::String("psync2".into()),
        ];
        self.stream.send(Value::Array(repl_conf2)).await?;

        // let psync = vec![
        //     Value::String("PSYNC".into()),
        //     Value::String("?".into()),
        //     Value::String("-1".into()),
        // ];
        // self.stream.send(Value::Array(psync)).await?;

        Ok(())
    }

    pub async fn forward(&mut self) -> Result<()> {
        self.handshake().await?;
        loop {
            while let Ok(response) = self.replica_in.recv().await {
                for value in response {
                    self.stream.send(value).await?;
                }
            }
        }
    }
}
