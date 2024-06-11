use crate::{
    connection::{
        client::RedisClient,
        ctx::{ReplicaMaster, ServerContext, ServerRole},
    },
    redis::{ops::RedisCommand, protocol::RedisProtocol},
};

use anyhow::Result;
use tokio::{net::TcpListener, sync::Mutex};

use std::{io::Write, sync::Arc};

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

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

        // Any extras to be sent
        post_processing(&mut client, Arc::clone(&ctx), command).await?;
    }
}

async fn post_processing(
    client: &mut RedisClient,
    _ctx: Arc<Mutex<ServerContext>>,
    command: RedisCommand,
) -> Result<()> {
    match command {
        // Only occurs for the master
        RedisCommand::Psync => {
            let rdb_file = hex::decode(EMPTY_RDB)?;
            let mut content = vec![];
            content.write(format!("${}\r\n", rdb_file.len()).as_bytes())?;
            content.write(&rdb_file)?;

            client.send(&content).await?;
        }
        _ => {}
    }
    Ok(())
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
        .send(RedisProtocol::array(&["REPLCONF", "capa", "eof", "capa", "psync2"]).as_bytes())
        .await?;

    let _ = master.recv().await?;

    master
        .send(RedisProtocol::array(&["PSYNC", "?", "-1"]).as_bytes())
        .await?;

    let _ = master.recv().await?;

    Ok(())
}
