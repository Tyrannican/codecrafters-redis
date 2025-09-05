use std::str::FromStr;

use crate::redis::{RedisCommand, RedisError};
use bytes::Bytes;

pub const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn empty_rdb() -> Result<Bytes, RedisError> {
    Ok(hex::decode(EMPTY_RDB)
        .map_err(|e| RedisError::HexError(e.to_string()))?
        .into())
}

pub fn bytes_to_str(b: &Bytes) -> Result<&str, RedisError> {
    str::from_utf8(&b[..]).map_err(|_| RedisError::StringConversion)
}

pub fn bytes_to_number<T: FromStr>(b: &Bytes) -> Result<T, RedisError> {
    let str = bytes_to_str(b)?;
    str.parse::<T>().map_err(|_| RedisError::NumberParse)
}

pub fn validate_args_len(req: &RedisCommand, len: usize) -> Result<(), RedisError> {
    if req.args.len() < len {
        return Err(RedisError::InsufficientArugments(req.cmd));
    }

    Ok(())
}
