use bytes::Bytes;
use nom::{
    bytes::complete::{tag, take},
    number::complete::{be_u32, be_u8, le_i16, le_i32, le_i8, le_u32, le_u64},
    IResult,
};

use super::{RdbDatabase, RdbDatabaseEntry, RdbExpiry, RdbInner, RdbKeyValue, RdbValue};

use std::{collections::HashMap, time::Duration};

#[derive(Debug)]
enum LengthEncoding {
    Len(u64),
    Int8,
    Int16,
    Int32,
}

fn parse_length_encoding(input: &[u8]) -> IResult<&[u8], LengthEncoding> {
    let mask = 0xCE;
    let (input, first) = be_u8(input)?;
    let encoding = (first & mask) >> 6;
    match encoding {
        0x00 => Ok((input, LengthEncoding::Len((first & 0x3F) as u64))),
        0x01 => {
            let (input, next) = be_u8(input)?;
            let size = (((first & 0x3F) as u64) << 8) | (next as u64);
            Ok((input, LengthEncoding::Len(size)))
        }
        0x02 => {
            let (input, size) = be_u32(input)?;
            Ok((input, LengthEncoding::Len(size as u64)))
        }
        0x03 => match first & 0x3F {
            0x00 => Ok((input, LengthEncoding::Int8)),
            0x01 => Ok((input, LengthEncoding::Int16)),
            0x02 => Ok((input, LengthEncoding::Int32)),
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Switch,
            ))),
        },
        _ => unreachable!(),
    }
}

fn parse_length_only(input: &[u8]) -> IResult<&[u8], u64> {
    let (input, size) = parse_length_encoding(input)?;
    match size {
        LengthEncoding::Len(s) => Ok((input, s)),
        _ => Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::MapRes,
        ))),
    }
}

fn parse_string(input: &[u8]) -> IResult<&[u8], Bytes> {
    let (input, encoding) = parse_length_encoding(input)?;

    match encoding {
        LengthEncoding::Len(size) => {
            let (input, bytes) = take(size)(input)?;
            Ok((input, Bytes::copy_from_slice(bytes)))
        }
        LengthEncoding::Int8 => {
            let (input, size) = le_i8(input)?;
            Ok((input, Bytes::from(size.to_string().into_bytes())))
        }
        LengthEncoding::Int16 => {
            let (input, size) = le_i16(input)?;
            Ok((input, Bytes::from(size.to_string().into_bytes())))
        }
        LengthEncoding::Int32 => {
            let (input, size) = le_i32(input)?;
            Ok((input, Bytes::from(size.to_string().into_bytes())))
        }
    }
}

fn parse_expiry_time(input: &[u8]) -> IResult<&[u8], Option<RdbExpiry>> {
    let (_, marker) = be_u8(input)?;

    match marker {
        0xFC => {
            let (input, _) = be_u8(input)?;
            let (input, ms) = le_u64(input)?;
            Ok((input, Some(RdbExpiry::Milliseconds(ms))))
        }
        0xFD => {
            let (input, _) = be_u8(input)?;
            let (input, secs) = le_u32(input)?;
            Ok((input, Some(RdbExpiry::Seconds(secs))))
        }
        _ => Ok((input, None)),
    }
}

fn parse_aux(input: &[u8]) -> IResult<&[u8], (Bytes, Bytes)> {
    let (input, _) = be_u8(input)?;
    let (input, key) = parse_string(input)?;
    let (input, val) = parse_string(input)?;

    Ok((input, (key, val)))
}

fn parse_db_stats(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = be_u8(input)?;
    let (input, _) = parse_length_only(input)?;
    let (input, _) = parse_length_only(input)?;

    Ok((input, ()))
}

fn parse_key_value(input: &[u8]) -> IResult<&[u8], RdbKeyValue> {
    let (input, expiry) = parse_expiry_time(input)?;
    let (input, value_type) = be_u8(input)?;
    let (input, key) = parse_string(input)?;

    let (input, value) = match value_type {
        0x00 => {
            let (input, s) = parse_string(input)?;
            Ok((input, RdbValue::String(s)))
        }
        _ => unimplemented!("other types not supported yet"),
    }?;

    Ok((input, RdbKeyValue { expiry, key, value }))
}

fn parse_database(input: &[u8]) -> IResult<&[u8], RdbDatabase> {
    let mut entries = HashMap::new();
    let (input, _) = be_u8(input)?;
    let (input, id) = parse_length_only(input)?;

    let input = if input.first() == Some(&0xFB) {
        let (input, _) = parse_db_stats(input)?;
        input
    } else {
        input
    };

    let mut input = input;

    loop {
        match input.first() {
            Some(&0xFE) | Some(&0xFF) | None => break,
            _ => {
                let (i, kv) = parse_key_value(input)?;
                let expiry = if let Some(expiry) = kv.expiry {
                    match expiry {
                        RdbExpiry::Seconds(s) => Some(Duration::from_millis((s as u64) * 1000)),
                        RdbExpiry::Milliseconds(ms) => Some(Duration::from_millis(ms)),
                    }
                } else {
                    None
                };

                let RdbValue::String(value) = kv.value;
                entries.insert(kv.key, RdbDatabaseEntry { expiry, value });
                input = i;
            }
        }
    }

    Ok((input, RdbDatabase { id, entries }))
}

fn parse_magic_header(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = tag(&b"REDIS"[..])(input)?;
    Ok((input, ()))
}

fn parse_version(input: &[u8]) -> IResult<&[u8], u32> {
    let (input, digits) = take(4usize)(input)?;
    let version = std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
        })?;

    Ok((input, version))
}

pub fn parse_rdb(input: &[u8]) -> IResult<&[u8], RdbInner> {
    let (mut input, _) = parse_magic_header(input)?;
    let (i, version) = parse_version(input)?;
    input = i;

    let mut aux = HashMap::new();
    let mut databases = Vec::new();

    loop {
        let (_, opcode) = be_u8(input)?;

        match opcode {
            0xFA => {
                let (i, pair) = parse_aux(input)?;
                aux.insert(pair.0, pair.1);
                input = i;
            }
            0xFE => {
                let (i, db) = parse_database(input)?;
                databases.push(db);
                input = i;
            }
            0xFF => {
                let (i, _) = be_u8(input)?;
                let (i, _crc) = take(8usize)(i)?;
                input = i;
                break;
            }
            _ => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Switch,
                )))
            }
        }
    }

    Ok((
        input,
        RdbInner {
            version,
            auxiliary: aux,
            databases,
        },
    ))
}
