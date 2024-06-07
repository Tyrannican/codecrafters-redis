use crate::{
    connection::client::RedisClient,
    redis::{ops::RedisCommand, protocol::RedisProtocol, store::RedisStore},
};

use anyhow::Result;
use tokio::{net::TcpListener, sync::Mutex};

use std::sync::Arc;

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
                Ok((client, _)) => RedisClient::from_stream(client),
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            let store = Arc::clone(&self.store);
            tokio::task::spawn(async move { handler(client, store).await });
        }
    }
}

async fn handler(mut client: RedisClient, store: Arc<Mutex<RedisStore>>) -> Result<()> {
    loop {
        let request = client.recv().await?;
        let command = RedisProtocol::parse_input(&request)?;
        let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
        let response = command.process(args, Arc::clone(&store)).await?;
        client.send(response.as_bytes()).await?;
    }
}
