#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisCommand {
    Ping,
    Echo,
    Get,
    Set,
    Info,
    ReplConf,
    Psync,
    Wait,
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
            Self::Wait => "wait".to_string(),
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
            "wait" => Self::Wait,
            _ => Self::Unknown(value.to_owned()),
        }
    }
}
