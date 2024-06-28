use anyhow::Result;
use bytes::BytesMut;
use kanal::{unbounded_async, AsyncReceiver, AsyncSender};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Debug)]
pub struct RedisClient {
    pub stream: TcpStream,
    cmd_sender: AsyncSender<String>,
    cmd_receiver: AsyncReceiver<String>,
}

impl RedisClient {
    pub fn from_stream(stream: TcpStream) -> Self {
        let (cmd_sender, cmd_receiver) = unbounded_async();
        Self {
            stream,
            cmd_sender,
            cmd_receiver,
        }
    }

    pub async fn new(addr: impl AsRef<str>) -> Result<Self> {
        let stream = TcpStream::connect(addr.as_ref()).await?;
        let (cmd_sender, cmd_receiver) = unbounded_async();
        Ok(Self {
            stream,
            cmd_sender,
            cmd_receiver,
        })
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let mut buf = BytesMut::with_capacity(4096);
        self.stream.read_buf(&mut buf).await?;

        Ok(buf.to_vec())
    }

    pub fn sender(&self) -> AsyncSender<String> {
        self.cmd_sender.clone()
    }

    pub fn receiver(&self) -> AsyncReceiver<String> {
        self.cmd_receiver.clone()
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write(data).await?;
        Ok(())
    }
}
