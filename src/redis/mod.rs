use bytes::Bytes;
use kanal::{unbounded_async, AsyncReceiver, AsyncSender};

pub mod protocol;
use protocol::{RedisError, Value};

pub type Request = (Value, AsyncSender<Vec<Value>>);

pub struct Node {
    rx: AsyncReceiver<Request>,
}

impl Node {
    pub fn new(rx: AsyncReceiver<Request>) -> Self {
        Self { rx }
    }

    pub async fn process(&mut self) -> Result<(), RedisError> {
        while let Ok((req, callback)) = self.rx.recv().await {
            let Value::Array(args) = req else {
                return Err(RedisError::UnexpectedValue);
            };

            let Value::String(cmd_bytes) = &args[0] else {
                return Err(RedisError::UnexpectedValue);
            };

            match str::from_utf8(&cmd_bytes[..])
                .map_err(|_| RedisError::StringConversion)?
                .to_lowercase()
                .as_str()
            {
                "ping" => callback
                    .send(vec![Value::SimpleString("PONG".into())])
                    .await
                    .unwrap(),

                "echo" => {
                    let Value::String(arg) = &args[1] else {
                        return Err(RedisError::UnexpectedValue);
                    };

                    callback
                        .send(vec![Value::String(arg.clone())])
                        .await
                        .map_err(|_| RedisError::ChannelSendError)?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
