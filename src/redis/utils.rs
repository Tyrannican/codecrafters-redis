use std::str::FromStr;

use crate::redis::{RedisCommand, RedisError};
use bytes::Bytes;

pub fn bytes_to_str(b: &Bytes) -> Result<&str, RedisError> {
    str::from_utf8(&b[..]).map_err(|_| RedisError::StringConversion)
}

pub fn bytes_to_float(b: &Bytes) -> Result<f64, RedisError> {
    let str = bytes_to_str(b)?;
    str.parse::<f64>().map_err(|_| RedisError::NumberParse)
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
