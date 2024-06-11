use anyhow::Result;
use std::fmt::Write;

pub struct RedisProtocol;

impl RedisProtocol {
    pub fn parse_input(input: &[u8]) -> Result<Vec<String>> {
        let parts = input
            .split(|&b| b == b'\n')
            .map(|b| b.strip_suffix(b"\r").unwrap_or(b))
            .collect::<Vec<&[u8]>>();

        let mut iter = parts.into_iter();
        let Some(lead) = iter.next() else {
            panic!("no data to read");
        };

        if lead[0] != b'*' {
            anyhow::bail!("expected an array, got {}", lead[0]);
        }

        let size = String::from_utf8(lead[1..].to_vec())?.parse::<usize>()?;
        let mut items = Vec::new();
        for _ in 0..size {
            // TODO: Nested arrays
            // Note: For arrays, there should always be two more entries
            // One for the tag and one for the item
            let _item_tag = iter.next();
            let item = iter.next().unwrap();

            items.push(String::from_utf8(item.to_vec())?.to_lowercase());
        }

        Ok(items)
    }

    pub fn _parse(input: &[u8]) -> Result<String> {
        let parts = input
            .split(|&b| b == b'\n')
            .map(|b| b.strip_suffix(b"\r").unwrap_or(b))
            .collect::<Vec<&[u8]>>();

        if parts.is_empty() {
            anyhow::bail!("no data to read");
        }

        let mut iter = parts.into_iter();
        let Some(lead) = iter.next() else {
            anyhow::bail!("no data!");
        };

        match lead[0] {
            b'+' => {
                let basic_str = String::from_utf8(lead[1..].to_vec())?;
                return Ok(RedisProtocol::simple_string(basic_str));
            }
            b'$' => {
                let bulk_str = String::from_utf8(lead[1..].to_vec())?;
                return Ok(RedisProtocol::string(bulk_str));
            }
            b'*' => unimplemented!("arrays not yet parsable!"),
            _ => anyhow::bail!("unknown tag: {}", lead[0]),
        }
    }

    pub fn array(input: &[impl AsRef<str>]) -> String {
        let mut output = format!("*{}\r\n", input.len());
        for param in input {
            let _ = write!(output, "{}", Self::string(param));
        }

        output
    }

    pub fn string(input: impl AsRef<str>) -> String {
        let str_ref = input.as_ref();
        format!("${}\r\n{}\r\n", str_ref.len(), str_ref)
    }

    pub fn simple_string(input: impl AsRef<str>) -> String {
        let s_ref = input.as_ref();
        format!("+{s_ref}\r\n")
    }

    pub fn null_string() -> String {
        String::from("$-1\r\n")
    }

    pub fn ok() -> String {
        String::from("+OK\r\n")
    }
}
