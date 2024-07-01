use anyhow::Result;
use bytes::BytesMut;
use kanal::{unbounded_async, AsyncReceiver, AsyncSender};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use std::sync::Arc;

use crate::{
    connection::ctx::ServerContext,
    redis::{
        ops::RedisCommand,
        protocol::{RedisMessage, RedisProtocol},
        rdb::empty_rdb,
        store::{RedisStoreEntry, StoreReader, StoreWriter},
    },
};

#[derive(Debug)]
pub struct RedisClient {
    pub stream: TcpStream,
    ctx: Arc<Mutex<ServerContext>>,
    store_reader: StoreReader,
    store_writer: Arc<Mutex<StoreWriter>>,
    cmd_sender: AsyncSender<String>,
    cmd_receiver: AsyncReceiver<String>,
}

impl RedisClient {
    pub fn from_stream(
        stream: TcpStream,
        ctx: Arc<Mutex<ServerContext>>,
        reader: StoreReader,
        writer: Arc<Mutex<StoreWriter>>,
    ) -> Self {
        let (cmd_sender, cmd_receiver) = unbounded_async();
        Self {
            stream,
            ctx,
            store_reader: reader,
            store_writer: writer,
            cmd_sender,
            cmd_receiver,
        }
    }

    pub async fn new(
        addr: impl AsRef<str>,
        ctx: Arc<Mutex<ServerContext>>,
        reader: StoreReader,
        writer: Arc<Mutex<StoreWriter>>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr.as_ref()).await?;
        let (cmd_sender, cmd_receiver) = unbounded_async();
        Ok(Self {
            stream,
            ctx,
            store_reader: reader,
            store_writer: writer,
            cmd_sender,
            cmd_receiver,
        })
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let mut buf = BytesMut::with_capacity(4096);
        self.stream.read_buf(&mut buf).await?;

        Ok(buf.to_vec())
    }

    pub fn receiver(&self) -> AsyncReceiver<String> {
        self.cmd_receiver.clone()
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write(data).await?;
        Ok(())
    }

    pub async fn process(&mut self, message: &RedisMessage) -> Result<()> {
        let command = &message.command;
        let args = &message.args;

        match command {
            RedisCommand::Ping => {
                let response = self.ping();
                self.send(response.as_bytes()).await?
            }
            RedisCommand::Echo => {
                let response = self.echo(&args);
                self.send(response.as_bytes()).await?
            }
            RedisCommand::Get => {
                let response = self.get(&args);
                self.send(response.as_bytes()).await?;
            }
            RedisCommand::Set => {
                let response = self.set(&args).await?;
                self.send(response.as_bytes()).await?;

                let mut command = vec![command.to_string()];
                command.extend_from_slice(&args);
                let command = RedisProtocol::array(&command);
                self.replicate(command).await?;
            }
            RedisCommand::Info => {
                let response = self.info(&args).await?;
                self.send(response.as_bytes()).await?;
            }
            RedisCommand::ReplConf => {
                let response = self.replconf(&args).await;
                self.send(response.as_bytes()).await?;
            }
            RedisCommand::Psync => {
                if args.len() < 2 {
                    anyhow::bail!("not enough arguments for psync");
                }

                let resp = self.psync(&args).await;
                self.send(resp.as_bytes()).await?;
                self.send(&empty_rdb()?).await?;
            }
            RedisCommand::Unknown(cmd) => anyhow::bail!("unknown command received: {cmd}"),
        }
        Ok(())
    }

    pub fn ping(&self) -> String {
        RedisProtocol::simple_string("PONG")
    }

    pub fn echo(&self, args: &[String]) -> String {
        RedisProtocol::string(&args[0])
    }

    pub fn get(&mut self, args: &[String]) -> String {
        let key = &args[0];
        match self.store_reader.get_one(key) {
            Some(entry) => match entry.value() {
                Some(v) => RedisProtocol::string(v),
                None => RedisProtocol::null_string(),
            },
            None => RedisProtocol::null_string(),
        }
    }

    pub async fn set(&mut self, args: &[String]) -> Result<String> {
        let (key, value) = (&args[0], &args[1]);
        let entry = if args.len() > 2 {
            let (expiry_type, expiry_len) = (&args[2], &args[3]);
            let expiry_len = match expiry_type.to_lowercase().as_str() {
                "px" => expiry_len.parse::<u128>()?,
                "ex" => expiry_len.parse::<u128>()? * 1000,
                _ => anyhow::bail!("invalid expiry type: {expiry_type}"),
            };

            RedisStoreEntry::new(value.to_string()).expires(expiry_len)
        } else {
            RedisStoreEntry::new(value.to_string())
        };

        let mut writer = self.store_writer.lock().await;
        writer.insert(key.to_string(), entry);
        writer.refresh();
        drop(writer);

        Ok(RedisProtocol::ok())
    }

    pub async fn info(&mut self, args: &[String]) -> Result<String> {
        let info_type = &args[0];
        let ctx = self.ctx.lock().await;
        let server_info = ctx.server_information();
        drop(ctx);

        match info_type.as_str() {
            "replication" => Ok(RedisProtocol::string(server_info)),
            _ => anyhow::bail!("invalid info type: {info_type}"),
        }
    }

    pub async fn replconf(&mut self, args: &[String]) -> String {
        let cmd = &args[0];
        match cmd.as_str() {
            "getack" => RedisProtocol::array(&["REPLCONF", "ACK", "0"]),
            _ => RedisProtocol::ok(),
        }
    }

    pub async fn psync(&mut self, args: &[String]) -> String {
        let (_master_repl_id, _offset) = (&args[0], &args[1]);
        let mut ctx = self.ctx.lock().await;
        ctx.add_replica(self.cmd_sender.clone());
        let replid = ctx.server_replid();
        drop(ctx);

        RedisProtocol::simple_string(format!("FULLRESYNC {} 0", replid))
    }

    async fn replicate(&mut self, cmd: String) -> Result<()> {
        let ctx = self.ctx.lock().await;
        let replicas = ctx.replicas();

        for replica in replicas {
            replica.send(cmd.clone()).await?;
        }
        drop(ctx);

        Ok(())
    }
}
