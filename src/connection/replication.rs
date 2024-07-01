use crate::{
    connection::client::RedisClient,
    redis::{ops::RedisCommand, protocol::RedisProtocol},
};

use anyhow::Result;

pub async fn start_replication(master: RedisClient, replica_port: u16) -> Result<()> {
    let master = repl_handshake(master, replica_port).await?;
    replication_handler(master).await
}

async fn repl_handshake(mut master: RedisClient, replica_port: u16) -> Result<RedisClient> {
    master
        .send(RedisProtocol::array(&["PING"]).as_bytes())
        .await?;

    let _ = master.recv().await?;

    master
        .send(
            RedisProtocol::array(&["REPLCONF", "listening-port", &format!("{replica_port}")])
                .as_bytes(),
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

async fn replication_handler(mut master: RedisClient) -> Result<()> {
    loop {
        let request = master.recv().await?;

        if request.is_empty() {
            return Ok(());
        }

        let messages = RedisProtocol::parse_input(&request)?;

        for message in messages {
            // Update total processed but don't send a response
            match message.command {
                RedisCommand::Ping => {
                    master.ping();
                }
                RedisCommand::Echo => {
                    master.echo(&message.args);
                }
                RedisCommand::Get => {
                    master.get(&message.args);
                }
                RedisCommand::Set => {
                    master.set(&message.args).await?;
                }
                RedisCommand::Info => {
                    master.info(&message.args).await?;
                }
                RedisCommand::ReplConf => {
                    // Except here, we need to respond to this
                    let response = master.replconf(&message.args).await;
                    master.send(response.as_bytes()).await?;
                }
                RedisCommand::Psync => {
                    master.psync(&message.args).await;
                }
                _ => unreachable!("not possible"),
            };
        }
    }
}
