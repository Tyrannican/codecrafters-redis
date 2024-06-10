use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Debug)]
pub struct RedisClient {
    stream: TcpStream,
}

impl RedisClient {
    pub fn from_stream(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn new(addr: impl AsRef<str>) -> Result<Self> {
        let stream = TcpStream::connect(addr.as_ref()).await?;
        Ok(Self { stream })
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let mut buf = vec![0; 4096];
        let n = self.stream.read(&mut buf).await?;
        buf.truncate(n);

        Ok(buf)
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write(data).await?;
        Ok(())
    }
}
