use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

mod ops;
mod protocol;
mod store;

use ops::RedisCommand;
use protocol::RedisProtocol;
use store::{RedisStore, RedisStoreEntry};

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
        let store = Arc::new(Mutex::new(RedisStore::new()));
        loop {
            let client = match self.listener.accept().await {
                Ok((client, _)) => client,
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            let store = Arc::clone(&store);
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
            RedisCommand::Get => {
                let key = &args[0];
                let mut store = store.lock().await;
                match store.get(key) {
                    Some(entry) => {
                        client
                            .write(RedisProtocol::string(entry.value).as_bytes())
                            .await?;
                    }
                    None => {
                        client
                            .write(RedisProtocol::null_string().as_bytes())
                            .await?;
                    }
                }
            }
            RedisCommand::Set => {
                let (key, value) = (&args[0], &args[1]);
                let entry = RedisStoreEntry::new(value.to_string());
                let mut store = store.lock().await;
                store.set(key.to_string(), entry);
                client.write(RedisProtocol::ok().as_bytes()).await?;
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
