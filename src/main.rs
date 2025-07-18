use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use kanal::{unbounded_async, AsyncSender};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

mod redis;
use redis::{
    protocol::{RespProtocol, Value},
    Node, Request,
};

struct ConnectionHandler {
    stream: Framed<TcpStream, RespProtocol>,
    node_channel: AsyncSender<Request>,
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream, node_channel: AsyncSender<Request>) -> Self {
        Self {
            stream: Framed::new(stream, RespProtocol),
            node_channel,
        }
    }

    pub async fn handle_connection(&mut self) -> Result<()> {
        let (tx, rx) = unbounded_async::<Vec<Value>>();

        loop {
            while let Some(frame) = self.stream.next().await {
                match frame {
                    Ok(value) => {
                        self.node_channel.send((value, tx.clone())).await?;
                        let Ok(resp) = rx.recv().await else {
                            self.stream
                                .send(Value::error("error occurred receiving value"))
                                .await?;

                            continue;
                        };

                        for value in resp {
                            self.stream.send(value).await?;
                        }
                    }
                    Err(e) => {
                        eprintln!("{e:#?}");
                        break;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let (tx, rx) = unbounded_async::<Request>();
    let mut node = Node::new(rx);

    tokio::task::spawn(async move { node.process().await });

    loop {
        if let Ok(stream) = listener.accept().await {
            let (stream, _addr) = stream;
            let mut handler = ConnectionHandler::new(stream, tx.clone());
            tokio::task::spawn(async move {
                if let Err(err) = handler.handle_connection().await {
                    eprintln!("connection error occurred: {err:#?}");
                };
            });
        }
    }
}
