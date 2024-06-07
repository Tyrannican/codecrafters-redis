use crate::redis::{
    protocol::RedisProtocol,
    store::{RedisStore, RedisStoreEntry},
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
    Unknown(String),
}

impl From<&String> for RedisCommand {
    fn from(value: &String) -> Self {
        match value.to_lowercase().as_str() {
            "echo" => Self::Echo,
            "ping" => Self::Ping,
            "get" => Self::Get,
            "set" => Self::Set,
            _ => Self::Unknown(value.to_owned()),
        }
    }
}

impl RedisCommand {
    pub async fn process(self, args: &[String], store: Arc<Mutex<RedisStore>>) -> Result<String> {
        match self {
            Self::Ping => Ok(RedisProtocol::simple_string("PONG")),
            Self::Echo => Ok(RedisProtocol::string(&args[0])),
            Self::Get => {
                let key = &args[0];
                let mut store = store.lock().await;
                match store.get(key) {
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

                let mut store = store.lock().await;
                store.set(key.to_string(), entry);
                Ok(RedisProtocol::ok())
            }
            Self::Unknown(cmd) => anyhow::bail!("unknown command received: {cmd}"),
        }
    }
}
