use crate::{
    connection::{
        client::RedisClient,
        ctx::{ServerContext, ServerRole},
        replication::start_replication,
    },
    redis::{
        protocol::RedisProtocol,
        store::{StoreReader, StoreWriter},
    },
};

use anyhow::Result;
use tokio::{net::TcpListener, sync::Mutex};

use std::sync::Arc;

#[derive(Debug)]
pub struct RedisNode {
    listener: TcpListener,
    store_reader: StoreReader,
    store_writer: Arc<Mutex<StoreWriter>>,
    ctx: Arc<Mutex<ServerContext>>,
}

impl RedisNode {
    pub async fn new(address: &str, server_role: ServerRole) -> Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let ctx = Arc::new(Mutex::new(ServerContext::new(server_role.clone())));
        let (store_reader, store_writer) = evmap::new();
        let store_writer = Arc::new(Mutex::new(store_writer));

        match server_role {
            ServerRole::Replica(addr) => {
                let (m_addr, m_port, r_port) = addr;
                let addr = format!("{m_addr}:{m_port}");
                let ctx = Arc::clone(&ctx);
                let reader = store_reader.clone();
                let writer = Arc::clone(&store_writer);
                let master = RedisClient::new(addr, ctx, reader, writer).await?;
                tokio::task::spawn(async move { start_replication(master, r_port).await });
            }
            _ => {}
        }

        Ok(Self {
            listener,
            store_reader,
            store_writer,
            ctx,
        })
    }

    pub async fn serve(&self) -> Result<()> {
        loop {
            let client = match self.listener.accept().await {
                Ok((client, _)) => {
                    let reader = self.store_reader.clone();
                    let writer = Arc::clone(&self.store_writer);
                    let ctx = Arc::clone(&self.ctx);
                    RedisClient::from_stream(client, ctx, reader, writer)
                }
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            tokio::task::spawn(async move { handler(client).await });
        }
    }
}

async fn handler(mut client: RedisClient) -> Result<()> {
    loop {
        let repl_channel = client.receiver();
        let request = tokio::select! {
            cmd = repl_channel.recv() => {
                let cmd = cmd?;
                client.send(cmd.as_bytes()).await?;
                continue;
            }
            req = client.recv() => { req? }
        };

        if request.is_empty() {
            return Ok(());
        }

        let messages = RedisProtocol::parse_input(&request)?;
        for message in messages {
            client.process(&message).await?;
        }

        // for command in commands {
        //     let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
        //     let responses = command.process(args, Arc::clone(&ctx)).await?;
        //     for response in responses {
        //         client.send(&response).await?;
        //     }

        //     if command == RedisCommand::Psync {
        //         let mut ctx = ctx.lock().await;
        //         ctx.add_replica(client.sender());
        //     }

        //     replicate(Arc::clone(&ctx)).await?;
        // }
    }
}
