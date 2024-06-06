use anyhow::Result;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let _client = match listener.accept().await {
            Ok((client, _)) => client,
            Err(err) => anyhow::bail!("something went wrong: {err}"),
        };
    }
}
