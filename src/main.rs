use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod ops;
mod protocol;
mod store;

use ops::RedisCommand;
use protocol::RedisProtocol;
use store::RedisStore;

#[derive(Debug)]
pub struct RedisNode {
    listener: TcpListener,
}

impl RedisNode {
    pub async fn new(address: &str) -> Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener })
    }

    pub async fn serve(&self) -> Result<()> {
        loop {
            let client = match self.listener.accept().await {
                Ok((client, _)) => client,
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            tokio::task::spawn(async move { handler(client).await });
        }
    }
}

async fn handler(mut client: TcpStream) -> Result<()> {
    loop {
        let mut buf = vec![0; 4096];
        let n = client.read(&mut buf).await?;
        buf.truncate(n);
        let command = RedisProtocol::parse_input(&buf)?;
        let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
        match command {
            RedisCommand::Ping => {
                client
                    .write(RedisProtocol::simple_string("PONG").as_bytes())
                    .await?;
            }
            RedisCommand::Echo => {
                let response = RedisProtocol::string(&args[0]);
                client.write(response.as_bytes()).await?;
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let node = RedisNode::new("127.0.0.1:6379").await?;
    node.serve().await
}
