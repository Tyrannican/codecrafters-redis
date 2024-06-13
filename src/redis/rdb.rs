use anyhow::Result;
use std::io::Write;

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn empty_rdb() -> Result<Vec<u8>> {
    let rdb_file = hex::decode(EMPTY_RDB)?;
    let mut content = vec![];
    content.write(format!("${}\r\n", rdb_file.len()).as_bytes())?;
    content.write(&rdb_file)?;

    Ok(content)
}
