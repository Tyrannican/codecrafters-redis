use crate::{
    connection::{
        client::RedisClient,
        ctx::{ServerContext, ServerRole},
    },
    redis::{ops::RedisCommand, protocol::RedisProtocol},
};

use anyhow::Result;
use tokio::{net::TcpListener, sync::Mutex};

use std::sync::Arc;

use super::ctx::ReplicaMaster;

#[derive(Debug)]
pub struct RedisNode {
    listener: TcpListener,
    ctx: Arc<Mutex<ServerContext>>,
}

impl RedisNode {
    pub async fn new(address: &str, server_role: ServerRole) -> Result<Self> {
        let listener = TcpListener::bind(address).await?;
        match &server_role {
            ServerRole::Replica(addr) => repl_handshake(&addr).await?,
            _ => {}
        }
        let ctx = Arc::new(Mutex::new(ServerContext::new(server_role)));
        Ok(Self { listener, ctx })
    }

    pub async fn serve(&self) -> Result<()> {
        loop {
            let client = match self.listener.accept().await {
                Ok((client, _)) => RedisClient::from_stream(client),
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            let ctx = Arc::clone(&self.ctx);
            tokio::task::spawn(async move { handler(client, ctx).await });
        }
    }
}

async fn handler(mut client: RedisClient, ctx: Arc<Mutex<ServerContext>>) -> Result<()> {
    loop {
        let request = client.recv().await?;
        if request.is_empty() {
            return Ok(());
        }

        let command = RedisProtocol::parse_input(&request)?;
        let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
        let response = command.process(args, Arc::clone(&ctx)).await?;
        client.send(response.as_bytes()).await?;
    }
}

pub async fn repl_handshake(master: &ReplicaMaster) -> Result<()> {
    let (m_addr, m_port, r_port) = master;
    let mut master = RedisClient::new(format!("{m_addr}:{m_port}")).await?;
    master
        .send(RedisProtocol::array(&["PING"]).as_bytes())
        .await?;

    let _ = master.recv().await?;

    master
        .send(
            RedisProtocol::array(&["REPLCONF", "listening-port", &format!("{r_port}")]).as_bytes(),
        )
        .await?;

    let _ = master.recv().await?;

    master
        .send(RedisProtocol::array(&["REPLCONF", "capa", "psync2"]).as_bytes())
        .await?;

    let _ = master.recv().await?;

    Ok(())
}
