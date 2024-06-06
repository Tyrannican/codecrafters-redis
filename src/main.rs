use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod protocol;
use protocol::{RedisProtocolParser, RedisValue};

async fn handler(mut client: TcpStream) -> Result<()> {
    loop {
        let mut buf = vec![0; 4096];
        let n = client.read(&mut buf).await?;
        buf.truncate(n);
        match RedisProtocolParser::parse_input(&buf)? {
            RedisValue::Array(command) => match command[0].to_lowercase().as_str() {
                "ping" => {
                    client
                        .write(RedisProtocolParser::simple_string("PONG").as_bytes())
                        .await?;
                }
                "echo" => {
                    let response = RedisProtocolParser::string(&command[1]);
                    client.write(response.as_bytes()).await?;
                }
                _ => {}
            },
            _ => anyhow::bail!("incoming commands should be an array"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let client = match listener.accept().await {
            Ok((client, _)) => client,
            Err(err) => anyhow::bail!("something went wrong: {err}"),
        };

        tokio::task::spawn(async move { handler(client).await });
    }
}
