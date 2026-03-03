use std::path::PathBuf;

use crate::redis::{protocol::RedisError, utils::empty_rdb};
use anyhow::Result;
use bytes::{Buf, Bytes};

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
        file.advance(9);

        eprintln!("{file:?}");
        loop {
            match file.get_u8() {
                0xFA => {
                    let key = extract_value(&mut file);
                    let value = extract_value(&mut file);
                    eprintln!("Key: {key:?} Value: {value:?}");
                }
                0xFE => {
                    let idx = extract_value(&mut file);
                    assert_eq!(file.get_u8(), 0xFB);
                    let size = extract_value(&mut file);
                    let expiry_values = extract_value(&mut file);

                    eprintln!("DB IDX: {idx:?} Size: {size:?} Expiry: {expiry_values:?}");
                }
                0xFF => break,
                _ => continue,
            }
        }

        Ok(())
    }
}

fn extract_value(buf: &mut Bytes) -> Bytes {
    let mask = 0xC0;

    let lead = buf.get_u8();
    match lead & mask {
        0x00 => {
            // TODO: Fix this - Return something proper
            let data = if lead == 0 {
                Bytes::from(vec![lead])
            } else {
                buf.slice(..lead as usize)
            };

            buf.advance(lead as usize);
            data
        }
        0x40 => {
            let size = (((lead & 0x3F) as u16) << 8) | buf.get_u8() as u16;
            let data = buf.slice(..size as usize);
            buf.advance(size as usize);
            data
        }
        0x80 => {
            let size = buf.get_u32();
            let data = buf.slice(..size as usize);
            buf.advance(size as usize);
            data
        }
        0xC0 => Bytes::from(vec![buf.get_u8()]),
        0xC1 => {
            let num = buf.get_u16_le();
            Bytes::from(format!("{num}").into_bytes())
        }
        0xC2 => {
            let num = buf.get_u32_le();
            Bytes::from(format!("{num}").into_bytes())
        }
        invalid_byte => panic!("invalid length encoding: {invalid_byte}"),
    }
}
