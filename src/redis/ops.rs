use crate::{
    connection::ctx::ServerContext,
    redis::{protocol::RedisProtocol, store::RedisStoreEntry},
};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisCommand {
    Ping,
    Echo,
    Get,
    Set,
    Info,
    ReplConf,
    Psync,
    Unknown(String),
}

impl From<&String> for RedisCommand {
    fn from(value: &String) -> Self {
        match value.to_lowercase().as_str() {
            "echo" => Self::Echo,
            "ping" => Self::Ping,
            "get" => Self::Get,
            "set" => Self::Set,
            "info" => Self::Info,
            "replconf" => Self::ReplConf,
            "psync" => Self::Psync,
            _ => Self::Unknown(value.to_owned()),
        }
    }
}

impl RedisCommand {
    pub async fn process(self, args: &[String], ctx: Arc<Mutex<ServerContext>>) -> Result<String> {
        match self {
            Self::Ping => Ok(RedisProtocol::simple_string("PONG")),
            Self::Echo => Ok(RedisProtocol::string(&args[0])),
            Self::Get => {
                let key = &args[0];
                let mut ctx = ctx.lock().await;
                match ctx.retrieve_from_store(key) {
                    Some(entry) => Ok(RedisProtocol::string(entry.value)),
                    None => Ok(RedisProtocol::null_string()),
                }
            }
            Self::Set => {
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

                let mut ctx = ctx.lock().await;
                ctx.update_store(key.to_string(), entry);
                Ok(RedisProtocol::ok())
            }
            Self::Info => {
                let info_type = &args[0];
                let ctx = ctx.lock().await;
                match info_type.as_str() {
                    "replication" => Ok(RedisProtocol::string(ctx.server_information())),
                    _ => anyhow::bail!("invalid info type: {info_type}"),
                }
            }
            Self::ReplConf => {
                // TODO: Implement properly
                Ok(RedisProtocol::ok())
            }
            Self::Psync => {
                if args.len() < 2 {
                    anyhow::bail!("not enough arguments for psync");
                }
                let (_master_repl_id, _offset) = (&args[0], &args[1]);
                let ctx = ctx.lock().await;
                let resp = format!("FULLRESYNC {} 0", ctx.server_replid());
                Ok(RedisProtocol::simple_string(resp))
            }
            Self::Unknown(cmd) => anyhow::bail!("unknown command received: {cmd}"),
        }
    }
}
