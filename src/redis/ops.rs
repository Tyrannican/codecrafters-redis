use crate::{
    connection::ctx::ServerContext,
    redis::{protocol::RedisProtocol, rdb::empty_rdb, store::RedisStoreEntry},
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

impl RedisCommand {
    pub fn to_string(&self) -> String {
        match *self {
            Self::Echo => "echo".to_string(),
            Self::Ping => "ping".to_string(),
            Self::Get => "get".to_string(),
            Self::Set => "set".to_string(),
            Self::Info => "info".to_string(),
            Self::ReplConf => "replconf".to_string(),
            Self::Psync => "psync".to_string(),
            _ => unimplemented!("meh"),
        }
    }
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
    pub async fn process(
        &self,
        args: &[String],
        ctx: Arc<Mutex<ServerContext>>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut responses = Vec::new();

        match self {
            Self::Ping => responses.push(RedisProtocol::simple_string("PONG").into_bytes()),
            Self::Echo => responses.push(RedisProtocol::string(&args[0]).into_bytes()),
            Self::Get => {
                let key = &args[0];
                let mut ctx = ctx.lock().await;
                match ctx.retrieve_from_store(key) {
                    Some(entry) => responses.push(RedisProtocol::string(entry.value).into_bytes()),
                    None => responses.push(RedisProtocol::null_string().into_bytes()),
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
                let mut command = vec![self.to_string()];
                command.extend_from_slice(args);
                ctx.add_command(RedisProtocol::array(&command));
                responses.push(RedisProtocol::ok().into_bytes());
            }
            Self::Info => {
                let info_type = &args[0];
                let ctx = ctx.lock().await;
                match info_type.as_str() {
                    "replication" => {
                        responses.push(RedisProtocol::string(ctx.server_information()).into_bytes())
                    }
                    _ => anyhow::bail!("invalid info type: {info_type}"),
                }
            }
            Self::ReplConf => responses.push(RedisProtocol::ok().into_bytes()),
            Self::Psync => {
                if args.len() < 2 {
                    anyhow::bail!("not enough arguments for psync");
                }
                let (_master_repl_id, _offset) = (&args[0], &args[1]);
                let ctx = ctx.lock().await;
                let resp =
                    RedisProtocol::simple_string(format!("FULLRESYNC {} 0", ctx.server_replid()));

                responses.push(resp.into_bytes());
                responses.push(empty_rdb()?);
            }
            Self::Unknown(cmd) => anyhow::bail!("unknown command received: {cmd}"),
        }

        Ok(responses)
    }
}
