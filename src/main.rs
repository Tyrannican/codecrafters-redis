use anyhow::Result;

mod connection;
mod redis;

use connection::server::RedisNode;

#[tokio::main]
async fn main() -> Result<()> {
    let node = RedisNode::new("127.0.0.1:6379").await?;
    node.serve().await
}
