use anyhow::Result;
use clap::Parser;

mod connection;
mod redis;
use connection::{ctx::ServerRole, server::RedisNode};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let address = format!("127.0.0.1:{}", cli.port);
    let server_role = match cli.replicaof {
        Some(addr) => {
            if let Some((addr, port)) = addr.split_once(" ") {
                let addr = addr.replace("localhost", "127.0.0.1");
                ServerRole::Replica(format!("{addr}:{port}"))
            } else {
                anyhow::bail!("invalid replica address: {addr}");
            }
        }
        None => ServerRole::Master,
    };

    let node = RedisNode::new(&address, server_role).await?;
    node.serve().await
}
