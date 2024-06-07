use anyhow::Result;
use clap::Parser;

mod connection;
mod redis;
use connection::server::RedisNode;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let address = format!("127.0.0.1:{}", cli.port);
    let node = RedisNode::new(&address).await?;
    node.serve().await
}
