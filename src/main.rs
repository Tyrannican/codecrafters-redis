use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let mut client = match listener.accept().await {
            Ok((client, _)) => client,
            Err(err) => anyhow::bail!("something went wrong: {err}"),
        };

        let mut buf = vec![0; 4096];
        let n = client.read(&mut buf).await?;
        buf.truncate(n);
        client.write(b"+PONG\r\n").await?;
    }
}
