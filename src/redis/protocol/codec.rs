//! Good resource: https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators

use bytes::{Bytes, BytesMut};
use std::str;
use tokio_util::codec::{Decoder, Encoder};

use super::{RedisError, Value};

type ProtocolResult = Result<Option<(usize, InterimValue)>, RedisError>;

// Start and End index of a phrase
#[derive(Debug)]
struct Phrase(usize, usize);

impl Phrase {
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

enum InterimValue {
    String(Phrase),
    NullString,
    Integer(i64),
    Array(Vec<InterimValue>),
    NullArray,
}

impl InterimValue {
    fn into_value(self, buf: &Bytes) -> Value {
        match self {
            Self::String(s) => Value::String(s.as_bytes(buf)),
            Self::Array(arr) => {
                Value::Array(arr.into_iter().map(|item| item.into_value(buf)).collect())
            }
            Self::Integer(int) => Value::Integer(int),
            Self::NullString => Value::NullString,
            Self::NullArray => Value::NullArray,
        }
    }
}

fn parse(buf: &BytesMut, mut pos: usize) -> ProtocolResult {
    if buf.is_empty() {
        return Ok(None);
    }

    loop {
        let idx = pos + 1;
        match buf[pos] {
            b'+' => return simple_string(buf, idx),
            b'$' => return bulk_string(buf, idx),
            b':' => return integer(buf, idx),
            b'*' => return array(buf, idx),
            b'\r' | b'\n' => {
                if buf[pos] == b'\r' {
                    pos += 2;
                } else {
                    pos += 1;
                }

                continue;
            }
            b => return Err(RedisError::InvalidProtocolByte(b as char)),
        }
    }
}

#[inline]
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, Phrase)> {
    if buf.len() <= pos {
        return None;
    }

    memchr::memchr(b'\r', &buf[pos..]).and_then(|end| {
        if end + 1 < buf.len() {
            // Pos + end + 2 = Skips the CRLF and points to the next section
            // Phrase is the start and end index of the current phrase in the buffer
            Some((pos + end + 2, Phrase(pos, pos + end)))
        } else {
            None
        }
    })
}

fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RedisError> {
    match word(buf, pos) {
        Some((next, phrase)) => {
            let s = str::from_utf8(phrase.as_slice(buf)).map_err(|_| RedisError::NumberParse)?;

            let integer = s.parse().map_err(|_| RedisError::NumberParse)?;

            Ok(Some((next, integer)))
        }
        None => Ok(None),
    }
}

fn simple_string(buf: &BytesMut, pos: usize) -> ProtocolResult {
    Ok(word(buf, pos).map(|(pos, phrase)| (pos, InterimValue::String(phrase))))
}

fn bulk_string(buf: &BytesMut, pos: usize) -> ProtocolResult {
    match int(buf, pos)? {
        Some((next, -1)) => Ok(Some((next, InterimValue::NullString))),
        Some((next, size)) if size >= 0 => {
            let total_size = next + size as usize;
            if buf.len() < total_size + 2 {
                return Ok(None);
            }

            let bulk_string = InterimValue::String(Phrase(next, total_size));
            Ok(Some((total_size + 2, bulk_string)))
        }
        Some((_, invalid)) => Err(RedisError::InvalidSize(invalid)),
        None => Ok(None),
    }
}

fn integer(buf: &BytesMut, pos: usize) -> ProtocolResult {
    Ok(int(buf, pos)?.map(|(pos, int)| (pos, InterimValue::Integer(int))))
}

fn array(buf: &BytesMut, pos: usize) -> ProtocolResult {
    match int(buf, pos)? {
        None => Ok(None),
        Some((next, -1)) => Ok(Some((next, InterimValue::NullArray))),
        Some((pos, total_size)) if total_size >= 0 => {
            let mut values = Vec::with_capacity(total_size as usize);
            let mut current_idx = pos;
            for _ in 0..total_size {
                match parse(buf, current_idx)? {
                    Some((new_pos, value)) => {
                        current_idx = new_pos;
                        values.push(value);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((current_idx, InterimValue::Array(values))))
        }
        Some((_, invalid)) => Err(RedisError::InvalidSize(invalid)),
    }
}

fn write_value(value: Value, dst: &mut BytesMut) {
    match value {
        Value::SimpleString(ss) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(&ss);
            dst.extend_from_slice(b"\r\n");
        }
        Value::String(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        Value::Integer(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Value::Array(arr) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(arr.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for v in arr {
                write_value(v, dst);
            }
        }
        Value::NullString => dst.extend_from_slice(b"$-1\r\n"),
        Value::NullArray => dst.extend_from_slice(b"*-1\r\n"),
        Value::EmptyArray => dst.extend_from_slice(b"*0\r\n"),
        Value::Rdb(rdb) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(rdb.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&rdb);
        }
        Value::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
    }
}

pub struct RespProtocol;

impl Decoder for RespProtocol {
    type Item = Value;
    type Error = RedisError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match parse(src, 0)? {
            Some((pos, value)) => {
                let data = src.split_to(pos);
                Ok(Some(value.into_value(&data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<Value> for RespProtocol {
    type Error = std::io::Error;

    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write_value(item, dst);
        Ok(())
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use futures_util::StreamExt;
    use tokio_util::codec::FramedRead;

    #[tokio::test]
    async fn multi_array_proto_test() {
        let input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n*1\r\n+PING\r\n";
        let mut reader = FramedRead::new(&input[..], RespProtocol);
        let frame1 = reader.next().await;
        let frame2 = reader.next().await;

        assert!(frame1.is_some());
        assert!(frame1.unwrap().is_ok());

        assert!(frame2.is_some());
        assert!(frame2.unwrap().is_ok());
    }

    #[tokio::test]
    async fn incomplete_proto_test() {
        let input = b"*2\r\n$4\r\nEC";
        let mut reader = FramedRead::new(&input[..], RespProtocol);
        let frame1 = reader.next().await;
        assert!(frame1.is_some());
        assert!(frame1.unwrap().is_err());
    }

    #[tokio::test]
    async fn empty_proto_test() {
        let input = b"";
        let mut reader = FramedRead::new(&input[..], RespProtocol);
        let frame = reader.next().await;
        assert!(frame.is_none());
    }
}
