use bytes::Bytes;
use thiserror::Error;

mod codec;
use super::utils::bytes_to_str;
pub use codec::*;

#[derive(Debug, Copy, Clone)]
pub enum CommandType {
    Ping,
    Echo,
    Get,
    Set,
    RPush,
    LPush,
    LRange,
    RRange,
    LLen,
    LPop,
    BLPop,
    Type,
    XAdd,
    XRange,
    XRead,
    Incr,
}

impl CommandType {
    pub fn from_bytes(b: &Bytes) -> Result<Self, RedisError> {
        match bytes_to_str(b)?.to_lowercase().as_str() {
            "ping" => Ok(Self::Ping),
            "echo" => Ok(Self::Echo),
            "get" => Ok(Self::Get),
            "set" => Ok(Self::Set),
            "rpush" => Ok(Self::RPush),
            "lpush" => Ok(Self::LPush),
            "lrange" => Ok(Self::LRange),
            "rrange" => Ok(Self::RRange),
            "llen" => Ok(Self::LLen),
            "lpop" => Ok(Self::LPop),
            "blpop" => Ok(Self::BLPop),
            "type" => Ok(Self::Type),
            "xadd" => Ok(Self::XAdd),
            "xrange" => Ok(Self::XRange),
            "xread" => Ok(Self::XRead),
            "incr" => Ok(Self::Incr),
            cmd => Err(RedisError::UnsupportedCommand(cmd.to_string())),
        }
    }
}

impl std::fmt::Display for CommandType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ping => write!(f, "ping"),
            Self::Echo => write!(f, "echo"),
            Self::Set => write!(f, "set"),
            Self::Get => write!(f, "get"),
            Self::RPush => write!(f, "rpush"),
            Self::LPush => write!(f, "lpush"),
            Self::LRange => write!(f, "lrange"),
            Self::RRange => write!(f, "rrange"),
            Self::LLen => write!(f, "llen"),
            Self::LPop => write!(f, "lpop"),
            Self::BLPop => write!(f, "blpop"),
            Self::Type => write!(f, "type"),
            Self::XAdd => write!(f, "xadd"),
            Self::XRange => write!(f, "xrange"),
            Self::XRead => write!(f, "xread"),
            Self::Incr => write!(f, "incr"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Value {
    String(Bytes),
    SimpleString(Bytes),
    Error(Bytes),
    NullString,
    Integer(i64),
    Array(Vec<Value>),
    NullArray,
    EmptyArray,
}

impl Value {
    pub fn ok() -> Self {
        Value::SimpleString("OK".into())
    }

    pub fn error(msg: Bytes) -> Self {
        Value::Error(msg)
    }
}

#[derive(Debug)]
pub struct RedisCommand {
    pub cmd: CommandType,
    pub args: Vec<Bytes>,
}

impl RedisCommand {
    pub fn new(inc_cmd: &Value) -> Result<Self, RedisError> {
        let Value::Array(args) = inc_cmd else {
            return Err(RedisError::UnexpectedValue);
        };
        assert!(!args.is_empty());

        let Some(Value::String(cmd_bytes)) = args.first() else {
            return Err(RedisError::UnexpectedValue);
        };

        let cmd_type = CommandType::from_bytes(cmd_bytes)?;
        let args = if args.len() > 1 {
            args[1..]
                .iter()
                .map(|v| {
                    let Value::String(inner) = v else {
                        todo!();
                    };

                    inner.clone()
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(Self {
            cmd: cmd_type,
            args,
        })
    }
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("failed to parse number")]
    NumberParse,

    #[error("invalid size parameter detected {0}")]
    InvalidSize(i64),

    #[error("invalid protocol byte - '{0}'")]
    InvalidProtocolByte(char),

    #[error("io error")]
    IOError(#[from] std::io::Error),

    #[error("unexpected value")]
    UnexpectedValue,

    #[error("unable to convert from bytes to str")]
    StringConversion,

    #[error("unable to send value across channel")]
    ChannelSendError,

    #[error("unsupported command - '{0}'")]
    UnsupportedCommand(String),

    #[error("insufficient arguments for '{0}'")]
    InsufficientArugments(CommandType),

    #[error("read lock error occurred")]
    ReadLock,

    #[error("write lock error occurred")]
    WriteLock,

    #[error("stream error - '{0}'")]
    StreamIdError(String),
}
