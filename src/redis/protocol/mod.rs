use bytes::Bytes;
use thiserror::Error;

mod codec;
pub use codec::*;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo,
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
}

impl Value {
    pub fn ok() -> Self {
        Value::SimpleString("OK".into())
    }

    pub fn error(msg: &'static str) -> Self {
        Value::Error(msg.into())
    }
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("failed to parse integer")]
    IntegerParse,

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
}
