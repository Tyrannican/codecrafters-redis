use super::ops::RedisCommand;
use anyhow::Result;
use std::fmt::Write;

pub struct RedisProtocol;

#[derive(Debug, Clone)]
pub struct RedisMessage {
    pub size: usize,
    pub command: RedisCommand,
    pub args: Vec<String>,
}

impl RedisProtocol {
    // Note: Ugly counting but meh
    pub fn parse_input(input: &[u8]) -> Result<Vec<RedisMessage>> {
        let mut output = vec![];
        let parts = Self::split_messages(input);

        for part in parts {
            let size = part.len();
            let inner = input
                .split(|&b| b == b'\n')
                .filter_map(|b| b.strip_suffix(b"\r"))
                .collect::<Vec<&[u8]>>();
            let mut iter = inner.into_iter();
            while let Some(lead) = iter.next() {
                //

                if lead[0] != b'*' {
                    anyhow::bail!("expected an array, got {}", lead[0]);
                }

                let arr_size = String::from_utf8(lead[1..].to_vec())?.parse::<usize>()?;
                let mut items = Vec::new();

                for _ in 0..arr_size {
                    // TODO: Nested arrays
                    // Note: For arrays, there should always be two more entries
                    // One for the tag and one for the item
                    let _item_tag = iter.next().unwrap();
                    let item = iter.next().unwrap();

                    items.push(String::from_utf8(item.to_vec())?.to_lowercase());
                }

                let (command, args) = (RedisCommand::from(&items[0]), items[1..].to_vec());
                output.push(RedisMessage {
                    size,
                    command,
                    args,
                });
            }
        }

        Ok(output)
    }

    pub fn split_messages(input: &[u8]) -> Vec<Vec<u8>> {
        let mut messages = vec![];
        let mut iter = input.into_iter().peekable();

        let mut inner = vec![];
        while let Some(byte) = iter.next() {
            if byte == &b'*' {
                let peeked = iter.peek().unwrap();
                if **peeked == b'\r' {
                    inner.push(*iter.next().unwrap());
                    inner.push(*iter.next().unwrap());
                    messages.push(inner.clone());
                    inner.clear();
                } else {
                    if !inner.is_empty() {
                        messages.push(inner.clone());
                        inner.clear();
                    }
                    inner.push(*byte);
                }
            } else {
                inner.push(*byte);
            }
        }

        messages.push(inner);
        messages
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

    pub fn integer(input: usize) -> String {
        format!(":{input}\r\n")
    }

    pub fn null_string() -> String {
        String::from("$-1\r\n")
    }

    pub fn ok() -> String {
        String::from("+OK\r\n")
    }
}

#[cfg(test)]
mod prototests {
    use super::*;

    #[test]
    fn handles_multi_array_messages() {
        let first = RedisProtocol::array(&["SET", "key1", "banana"]);
        let second = RedisProtocol::array(&["SET", "key2", "strawberry"]);
        let third = RedisProtocol::array(&["SET", "key3", "pineapple"]);

        let mut messages = vec![];
        messages.extend_from_slice(&first.as_bytes());
        messages.extend_from_slice(&second.as_bytes());
        messages.extend_from_slice(&third.as_bytes());

        let split = RedisProtocol::split_messages(&messages);

        let string = String::from_utf8(split[0].clone()).unwrap();
        assert_eq!(string, first);
        let string = String::from_utf8(split[1].clone()).unwrap();
        assert_eq!(string, second);
        let string = String::from_utf8(split[2].clone()).unwrap();
        assert_eq!(string, third);
    }
}
