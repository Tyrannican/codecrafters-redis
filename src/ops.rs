#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RedisCommand {
    Ping,
    Echo,
    Get,
    Set,
    Unknown,
}

impl From<&String> for RedisCommand {
    fn from(value: &String) -> Self {
        match value.to_lowercase().as_str() {
            "echo" => Self::Echo,
            "ping" => Self::Ping,
            "get" => Self::Get,
            "set" => Self::Set,
            _ => Self::Unknown,
        }
    }
}
