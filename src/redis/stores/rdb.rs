use std::path::PathBuf;

use crate::redis::{protocol::RedisError, utils::empty_rdb};
use anyhow::Result;
use bytes::{Buf, Bytes};

#[derive(Clone, PartialEq, Debug)]
pub enum LengthEncoding {
    Size(usize),
    Str(Bytes),
}

#[derive(Debug)]
pub struct RdbFile {
    raw: Bytes,
}

impl RdbFile {
    pub fn new() -> Self {
        Self { raw: Bytes::new() }
    }

    pub fn load(&mut self, path: Option<PathBuf>) -> Result<(), RedisError> {
        match path {
            Some(path) => {
                let raw = if !path.exists() {
                    empty_rdb()?
                } else {
                    let content =
                        std::fs::read(&path).map_err(|e| RedisError::FileRead(e.to_string()))?;
                    Bytes::from_iter(content.into_iter())
                };

                self.raw = raw;
                self.parse()?;
            }

            None => {
                self.raw = empty_rdb()?;
            }
        }

        Ok(())
    }

    pub fn raw(&self) -> Bytes {
        self.raw.clone()
    }

    pub fn parse(&self) -> Result<(), RedisError> {
        let mut file = Bytes::from(self.raw[..].to_vec());
        // Skip the header for now
        file.advance(9);

        eprintln!("{file:?}");
        loop {
            match file.get_u8() {
                0xFA => {
                    let key = extract_value(&mut file);
                    eprintln!("KEY: {key:?}");
                    let value = extract_value(&mut file);
                    eprintln!("Value: {value:?}");
                }
                0xFE => {
                    let idx = length_encoding(&mut file);
                    assert_eq!(file.get_u8(), 0xFB);
                    let size = length_encoding(&mut file);
                    let expiry_values = length_encoding(&mut file);

                    eprintln!("DB IDX: {idx:?} Size: {size:?} Expiry: {expiry_values:?}");
                }
                0xFF => break,
                _ => continue,
            }
        }

        Ok(())
    }
}

fn length_encoding(buf: &mut Bytes) -> LengthEncoding {
    let mask = 0xC0;
    let lead = buf.get_u8();
    match lead & mask {
        0x00 => LengthEncoding::Size((lead & 0x3F) as usize),
        0x40 => {
            let size: u16 = ((lead & 0x3F) as u16) << 8 | (buf.get_u8() as u16);
            LengthEncoding::Size(size as usize)
        }
        0x80 => LengthEncoding::Size(buf.get_u32() as usize),
        0xC0 => LengthEncoding::Str(Bytes::from(vec![buf.get_u8()])),
        0xC1 => {
            let num = buf.get_u16_le();
            LengthEncoding::Str(Bytes::from(format!("{num}").into_bytes()))
        }
        0xC2 => {
            let num = buf.get_u32_le();
            LengthEncoding::Str(Bytes::from(format!("{num}").into_bytes()))
        }
        invalid_byte => panic!("invalid length encoding byte: {invalid_byte}"),
    }
}

fn extract_value(buf: &mut Bytes) -> Bytes {
    match length_encoding(buf) {
        LengthEncoding::Size(size) => {
            let data = buf.slice(..size);
            buf.advance(size);
            data
        }
        LengthEncoding::Str(value) => value,
    }
}
