use crate::{
    connection::{
        client::RedisClient,
        ctx::{ServerContext, ServerRole},
        replication::start_replication,
    },
    redis::{
        ops::RedisCommand,
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
                let ctx = Arc::clone(&ctx);
                let reader = store_reader.clone();
                let writer = Arc::clone(&store_writer);
                tokio::task::spawn(
                    async move { start_replication(&addr, reader, writer, ctx).await },
                );
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
                Ok((client, _)) => RedisClient::from_stream(client),
                Err(err) => anyhow::bail!("something went wrong: {err}"),
            };

            let ctx = Arc::clone(&self.ctx);
            let writer = Arc::clone(&self.store_writer);
            let reader = self.store_reader.clone();
            tokio::task::spawn(async move { handler(client, reader, writer, ctx).await });
        }
    }
}

async fn handler(
    mut client: RedisClient,
    reader: StoreReader,
    writer: Arc<Mutex<StoreWriter>>,
    ctx: Arc<Mutex<ServerContext>>,
) -> Result<()> {
    loop {
        // let repl_channel = client.receiver();
        // let request = tokio::select! {
        //     cmd = repl_channel.recv() => {
        //         let cmd = cmd?;
        //         client.send(cmd.as_bytes()).await?;
        //         continue;
        //     }
        //     req = client.recv() => { req? }
        // };
        let request = client.recv().await?;

        // if request.is_empty() {
        //     return Ok(());
        // }

        let commands = RedisProtocol::parse_input(&request)?;
        for (size, command) in commands {
            let ctx = ctx.lock().await;
            let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
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

pub async fn replicate(ctx: Arc<Mutex<ServerContext>>) -> Result<()> {
    let mut ctx = ctx.lock().await;
    let cmd_queue = ctx.command_queue().drain(..).collect::<Vec<String>>();
    for cmd in cmd_queue {
        for replica in ctx.replicas() {
            replica.send(cmd.clone()).await?;
        }
    }

    Ok(())
}
