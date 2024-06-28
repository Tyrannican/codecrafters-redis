use crate::{
    connection::{
        client::RedisClient,
        ctx::{ServerContext, ServerRole},
        replication::start_replication,
    },
    redis::{ops::RedisCommand, protocol::RedisProtocol},
};

use anyhow::Result;
use tokio::{net::TcpListener, sync::Mutex};

use std::sync::Arc;

#[derive(Debug)]
pub struct RedisNode {
    listener: TcpListener,
    ctx: Arc<Mutex<ServerContext>>,
}

impl RedisNode {
    pub async fn new(address: &str, server_role: ServerRole) -> Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let test = ServerContext::new(ServerRole::Master);
        let ctx = Arc::new(Mutex::new(ServerContext::new(server_role.clone())));
        match server_role {
            ServerRole::Replica(addr) => {
                let ctx = Arc::clone(&ctx);
                tokio::task::spawn(async move { start_replication(&addr, ctx).await });
            }
            _ => {}
        }

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

        let commands = RedisProtocol::parse_input(&request)?;

        for command in commands {
            let (command, args) = (RedisCommand::from(&command[0]), &command[1..]);
            let responses = command.process(args, Arc::clone(&ctx)).await?;
            for response in responses {
                client.send(&response).await?;
            }

            if command == RedisCommand::Psync {
                let mut ctx = ctx.lock().await;
                ctx.add_replica(client.sender());
            }

            replicate(Arc::clone(&ctx)).await?;
        }
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
