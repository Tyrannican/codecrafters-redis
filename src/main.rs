use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

mod redis;

use redis::ops::RedisCommand;
use redis::protocol::RedisProtocol;
use redis::store::{RedisStore, RedisStoreEntry};

#[derive(Debug)]
pub struct RedisNode {
    listener: TcpListener,
    store: Arc<Mutex<RedisStore>>,
}

impl RedisNode {
    pub async fn new(address: &str) -> Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let store = Arc::new(Mutex::new(RedisStore::new()));
        Ok(Self { listener, store })
    }

    pub async fn serve(&self) -> Result<()> {
        loop {
            let client = match self.listener.accept().await {
                Ok((client, _)) => client,
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            let store = Arc::clone(&self.store);
            tokio::task::spawn(async move { handler(client, store).await });
        }
    }
}

// TODO: extract out and cleanup
async fn handler(mut client: TcpStream, store: Arc<Mutex<RedisStore>>) -> Result<()> {
    loop {
        let mut buf = vec![0; 4096];
        let n = client.read(&mut buf).await?;
        buf.truncate(n);
        let command = RedisProtocol::parse_input(&buf)?;
        let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
        let response = command.process(args, Arc::clone(&store)).await?;
        client.write(response.as_bytes()).await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let node = RedisNode::new("127.0.0.1:6379").await?;
    node.serve().await
}
