use crate::{
    connection::{
        client::RedisClient,
        ctx::{ReplicaMaster, ServerContext},
    },
    redis::{
        ops::RedisCommand,
        protocol::RedisProtocol,
        store::{StoreReader, StoreWriter},
    },
};

use anyhow::Result;
use tokio::sync::Mutex;

use std::sync::Arc;

pub async fn start_replication(
    master: &ReplicaMaster,
    reader: StoreReader,
    writer: Arc<Mutex<StoreWriter>>,
    ctx: Arc<Mutex<ServerContext>>,
) -> Result<()> {
    let master = repl_handshake(master).await?;
    replication_handler(master, reader, writer, ctx).await
}

async fn repl_handshake(master: &ReplicaMaster) -> Result<RedisClient> {
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

    // Check if we get the RDB alongside this response
    let resync = master.recv().await?;
    let parts = resync
        .split(|&b| b == b'\n')
        .filter_map(|b| b.strip_suffix(b"\r"))
        .collect::<Vec<&[u8]>>();

    // Only one item means we didn't get the RDB
    if parts.len() == 1 {
        let _ = master.recv().await?;
    }

    Ok(master)
}

async fn replication_handler(
    mut master: RedisClient,
    reader: StoreReader,
    writer: Arc<Mutex<StoreWriter>>,
    ctx: Arc<Mutex<ServerContext>>,
) -> Result<()> {
    loop {
        let request = master.recv().await?;

        if request.is_empty() {
            return Ok(());
        }

        let messages = RedisProtocol::parse_input(&request)?;

        for message in messages {
            let responses = message
                .command
                .process(reader.clone(), writer.clone(), ctx.clone())
                .await?;
            if message.command == RedisCommand::ReplConf
                && message.args.contains(&"getack".to_string())
            {
                for response in responses {
                    master.send(&response).await?;
                }
            }
        }
    }
}
