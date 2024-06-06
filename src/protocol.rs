use anyhow::Result;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RedisValue {
    Array(Vec<String>),
    SimpleString(String),
    String(String),
}

pub struct RedisProtocolParser;

impl RedisProtocolParser {
    pub fn parse_input(input: &[u8]) -> Result<RedisValue> {
        let parts = input
            .split(|&b| b == b'\n')
            .map(|b| b.strip_suffix(b"\r").unwrap_or(b))
            .collect::<Vec<&[u8]>>();

        let mut iter = parts.into_iter();
        let Some(lead) = iter.next() else {
            panic!("no data to read");
        };

        match lead[0] {
            b'*' => {
                let size = String::from_utf8(lead[1..].to_vec())?.parse::<usize>()?;
                let mut items = Vec::new();
                for _ in 0..size {
                    // TODO: Nested arrays
                    // Note: For arrays, there should always be two more entries
                    // One for the tag and one for the item
                    let _item_tag = iter.next();
                    let item = iter.next().unwrap();

                    items.push(String::from_utf8(item.to_vec())?);
                }
                return Ok(RedisValue::Array(items));
            }
            b'+' => {
                let value = String::from_utf8(lead[1..].to_vec())?;
                return Ok(RedisValue::SimpleString(value));
            }
            b'$' => {
                let Some(string) = iter.next() else {
                    anyhow::bail!("need another parameter after tag");
                };

                let string = String::from_utf8(string.to_vec())?;
                return Ok(RedisValue::String(string));
            }
            _ => {}
        }

        anyhow::bail!("redis protocol needs to match something");
    }

    pub fn string(input: impl AsRef<str>) -> String {
        let str_ref = input.as_ref();
        format!("${}\r\n{}\r\n", str_ref.len(), str_ref)
    }

    pub fn simple_string(input: impl AsRef<str>) -> String {
        let s_ref = input.as_ref();
        format!("+{s_ref}\r\n")
    }
}
