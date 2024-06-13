use anyhow::Result;
use kanal::{unbounded_async, AsyncReceiver, AsyncSender};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Debug)]
pub struct RedisClient {
    stream: TcpStream,
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
        let mut buf = vec![0; 4096];
        let n = self.stream.read(&mut buf).await?;
        buf.truncate(n);

        Ok(buf)
    }

    pub async fn sender(&self) -> AsyncSender<String> {
        self.cmd_sender.clone()
    }

    pub async fn recv_on_channel(&mut self) -> Result<String> {
        let cmd = self.cmd_receiver.recv().await?;
        Ok(cmd)
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write(data).await?;
        Ok(())
    }
}
