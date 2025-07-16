use anyhow::Result;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

struct ConnectionHandler {
    stream: TcpStream,
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn handle_connection(&mut self) -> Result<()> {
        loop {
            let mut buf = [0; 4096];
            let n = self.stream.read(&mut buf)?;
            if n == 0 {
                break;
            }

            self.stream.write_all(b"+PONG\r\n")?;
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let Ok(stream) = stream else {
            eprintln!("error establishing connection to client");
            continue;
        };

        let mut handler = ConnectionHandler::new(stream);
        handler.handle_connection()?;
    }

    Ok(())
}
