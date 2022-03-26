use std::{net::SocketAddr, fmt::Display, collections::HashMap};
use serde::{Serialize, Deserialize};
use tokio::net::TcpStream;
use tokio_util::codec::{BytesCodec, Framed, Decoder};
use futures::{ sink::SinkExt, stream::StreamExt };
use crate::peer;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message {
    Ping,
    Heartbeat,
    ListPeersRequest,
    ListPeersResponse(HashMap<peer::Identity, SocketAddr>),
    AddMe(AddMePayload),
    Error(String),
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Ping => write!(f, "Ping"),
            Message::Heartbeat => write!(f, "Heartbeat"),
            Message::ListPeersRequest => write!(f, "ListPeersRequest"),
            Message::ListPeersResponse(map) => write!(f, "ListPeersResponse {:?}", map),
            Message::AddMe(add) => write!(f, "AddMe {}", add),
            Message::Error(s) => write!(f, "Error: {}", s),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddMePayload {
    pub identity: peer::Identity,
    pub listen_addr: SocketAddr,
}

impl Display for AddMePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.identity, self.listen_addr)
    }
}

#[derive(Debug)]
pub enum Error {
    SerializationError(bincode::Error),
    IOError(std::io::Error),
    StreamEnded,
}

#[derive(Debug)]
pub struct Connection {
    // stream: TcpStream,
    framed_stream: Framed<TcpStream, BytesCodec>,
}

impl Connection {
    pub async fn new(addr: SocketAddr) -> std::io::Result<Self> {
        
        let stream = TcpStream::connect(addr).await?;
        // TODO add tls
        let framed_stream = BytesCodec::new().framed(stream);
        Ok(Connection{framed_stream})
    }

    pub fn from_stream(stream: TcpStream) -> Self {
        let framed_stream = BytesCodec::new().framed(stream);
        Connection{framed_stream}
    }

    pub async fn send_message(&mut self, m: Message) -> Result<(), Error> {
        let bytes = bincode::serialize(&m)
            .map_err(Error::SerializationError)?;
        let bytes = bytes::Bytes::from(bytes);
        self.framed_stream.send(bytes).await
            .map_err(Error::IOError)?;
        Ok(())
    }
    
    pub async fn recv_message(&mut self) -> Result<Message, Error> {
        let result = match self.framed_stream.next().await {
            Some(v) => v,
            None => return Err(Error::StreamEnded),
        };
        let bytes = result.map_err(Error::IOError)?;
        let bytes = &bytes[..];
        let m = bincode::deserialize(bytes)
            .map_err(Error::SerializationError)?;
        Ok(m)
    }
}