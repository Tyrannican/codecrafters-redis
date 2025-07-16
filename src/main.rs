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

    pub async fn handle_connection(&mut self) -> Result<()> {
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

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let Ok(stream) = stream else {
            eprintln!("error establishing connection to client");
            continue;
        };

        let mut handler = ConnectionHandler::new(stream);
        tokio::task::spawn(async move {
            if let Err(err) = handler.handle_connection().await {
                eprintln!("connection error occurred: {err:#?}");
            };
        });
    }

    Ok(())
}
