use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use kanal::{unbounded_async, AsyncSender};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use uuid::Uuid;

mod redis;
use redis::{
    protocol::{RespProtocol, Value},
    RedisServer, Request, ServerRole,
};

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,

    #[arg(long)]
    pub replicaof: Option<String>,
}

struct ConnectionHandler {
    id: Bytes,
    stream: Framed<TcpStream, RespProtocol>,
    request_channel: AsyncSender<Request>,
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream, node_channel: AsyncSender<Request>) -> Self {
        Self {
            id: Bytes::from(Uuid::new_v4().to_string()),
            stream: Framed::new(stream, RespProtocol),
            request_channel: node_channel,
        }
    }

    pub async fn handle_connection(&mut self) -> Result<()> {
        let (tx, rx) = unbounded_async::<Vec<Value>>();

        loop {
            while let Some(frame) = self.stream.next().await {
                match frame {
                    Ok(value) => {
                        self.request_channel
                            .send((value, self.id.clone(), tx.clone()))
                            .await?;

                        let Ok(response) = rx.recv().await else {
                            self.stream
                                .send(Value::error("error occurred receiving value".into()))
                                .await?;

                            continue;
                        };

                        for r in response {
                            self.stream.send(r).await?;
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

fn determine_server_role(replica: Option<String>) -> ServerRole {
    match replica {
        Some(s) => {
            let Some((addr, port)) = s.split_once(" ") else {
                eprintln!("expected replica address");
                std::process::exit(-1);
            };

            let addr = if addr == "localhost" {
                "127.0.0.1".to_string()
            } else {
                addr.to_owned()
            };

            let Ok(port) = port.parse::<u16>() else {
                eprintln!("error parsing master port for replica");
                std::process::exit(-1);
            };

            ServerRole::Replica((addr, port))
        }
        None => ServerRole::Master,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let addr = format!("0.0.0.0:{}", args.port);
    let listener = TcpListener::bind(addr).await.unwrap();
    let (tx, rx) = unbounded_async::<Request>();

    let role = determine_server_role(args.replicaof);
    let mut server = RedisServer::new(role);
    server.start(rx);

    loop {
        if let Ok(stream) = listener.accept().await {
            let (stream, _) = stream;
            let mut handler = ConnectionHandler::new(stream, tx.clone());
            tokio::task::spawn(async move {
                if let Err(err) = handler.handle_connection().await {
                    eprintln!("connection error occurred: {err:#?}");
                };
            });
        }
    }
}
