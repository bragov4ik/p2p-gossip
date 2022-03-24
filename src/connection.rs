use std::{
    collections::HashMap, net::SocketAddr,
    sync::{ Mutex, Arc }, fmt::Display
};
use serde::{ Serialize, de::DeserializeOwned };
use tokio::{
    net::UdpSocket,
    sync::{ mpsc }
};

/// Manager allows to add new connections for its associated handler.
/// 
/// It is intended to be cloned where making new connections is required.
/// 
/// # Examples
/// See <TODO>
#[derive(Clone)]
struct Manager<M> {
    write_chan_sender: mpsc::Sender<SendRequest<M>>,
    read_chans: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<M>>>>,
}

impl<M> Manager<M> {
    fn add_connection(
        self, addr: SocketAddr, read_buffer: usize
    ) -> Result<(mpsc::Sender<SendRequest<M>>, mpsc::Receiver<M>)> {
        let mut read_chans_map = self.read_chans.lock()
            .map_err(|_| {Error::ChannelsPoisoned})?;
        if read_chans_map.contains_key(&addr) {
            return Err(Error::AddrBusy(addr))
        }
        let (new_read_s, new_read_r) = mpsc::channel(read_buffer);
        read_chans_map.insert(addr, new_read_s);
        Ok((self.write_chan_sender.clone(), new_read_r))
    }
}

/// Handler processes communication channels to multiple hosts through one UDP port, distributing
/// messages to separate channels.
struct Handler<M>
where
    M: Serialize + DeserializeOwned {
    sock: UdpSocket,
    write_channel: mpsc::Receiver<SendRequest<M>>,
    manager: Manager<M>,
}

/// SendRequest is an addressed message intended to be sent to `dest`.
/// 
/// It is needed because `Handler` cannot distinguish between producers of the request in
/// `write_channel`.
/// 
/// # Usage
/// It is supposed to be given to the Handler through write channel. The Handler then will attemt
/// to send `payload` to `dest`.
pub struct SendRequest<M> {
    dest: SocketAddr,
    payload: M,
}

impl<M> Handler<M>
where
    M: Serialize + DeserializeOwned {
    fn read_chans(&self)
    -> Result<std::sync::MutexGuard<'_, HashMap<SocketAddr, mpsc::Sender<M>>>> {
        self.manager.read_chans.lock()
            .map_err(|_| {Error::ChannelsPoisoned})
    }
}

impl<M> Handler<M>
where
    M: Serialize + DeserializeOwned {
    /// TODO write about buf size
    /// TODO write that uses json
    /// TODO write that does not split big messages so limited max message size (enough for now)
    pub async fn run_buf<const B: usize>(mut self) -> Result<()> {
        let mut buf = [0u8; B];
        loop {
            let read_fut = self.sock.recv_from(&mut buf);
            let write_fut = self.write_channel.recv();
            tokio::select! {
                result = read_fut => {
                    if let Err(e) = self.handle_read(result, &buf).await {
                        match e {
                            Error::NoConnection(_) | Error::IOError(_) | 
                            Error::ReadChannelClosed(_) | Error::SerdeError(_) => 
                                log::warn!("Error during handling receiving from network: {}", e),
                            // These errors should not be returned from read, thus unexpected
                            Error::AddrBusy(_) | Error::WriteChannelClosed | 
                            Error::UDPFewBytesSent(_) =>
                                log::error!("Unexpected error during handling read \
                                from network: {}", e),
                            // Poisoned data is meant not to be used normally later
                            Error::ChannelsPoisoned => return Err(e),
                        };
                    };
                },
                result = write_fut => {
                    if let Err(e) = self.handle_write(result).await {
                        match e {
                            Error::IOError(_) | Error::WriteChannelClosed |
                            Error::SerdeError(_) | Error::UDPFewBytesSent(_) => 
                                log::warn!("Error during handling send request to network: {}", e),
                            // These errors should not be returned from write, thus unexpected
                            Error::AddrBusy(_) | Error::NoConnection(_) |
                            Error::ReadChannelClosed(_) | Error::ChannelsPoisoned =>
                                log::error!("Unexpected error during handling send request \
                                to network: {}", e),
                        };
                    };
                },
            }
        }
    }

    async fn handle_read(
        &self, result: std::io::Result<(usize, SocketAddr)>, buf: &[u8]
    ) -> Result<()> {
        let (bytes_read, src_addr) = result.map_err(Error::IOError)?;

        // Deserialize message
        let msg: M = serde_json::from_slice(&buf[..bytes_read])
            .map_err(Error::SerdeError)?;

        // Put the message into corresponding channel
        let mut read_chans_map = self.read_chans()?;
        let result = read_chans_map
            .get_mut(&src_addr);
        let channel = match result {
            Some(v) => v,
            None => return Err(Error::NoConnection(src_addr)),
        };
        channel.send(msg).await
            .map_err(|_| Error::ReadChannelClosed(src_addr))?;

        Ok(())
    }

    async fn handle_write(
        &self, result: Option<SendRequest<M>>
    ) -> Result<()> {
        let SendRequest{ dest, payload: msg } = match result {
            Some(v) => v,
            None => return Err(Error::WriteChannelClosed),
        };

        // Serialize message
        let bytes = serde_json::to_vec(&msg)
            .map_err(Error::SerdeError)?;
        let n_written = self.sock.send_to(&bytes, dest).await
            .map_err(Error::IOError)?;
        if n_written != bytes.len() {
            return Err(
                Error::UDPFewBytesSent(format!("expected {}, sent {}", bytes.len(), n_written))
            )
        }
        Ok(())
    }    
}

pub enum Error {
    /// This address is already connected
    AddrBusy(SocketAddr),
    /// Nobody is assigned to this address, thus can't handle message
    NoConnection(SocketAddr),
    /// Attemted to send message to a closed channel
    ReadChannelClosed(SocketAddr),
    /// Channel for writing (getting udp send requests) is closed
    WriteChannelClosed,
    /// Channel storage has been poisoned
    ChannelsPoisoned,
    /// Not all bytes were sent, probably the message was too large for UDP
    UDPFewBytesSent(String),
    /// Problems with IO
    IOError(std::io::Error),
    /// Problems with serialization or deserialization
    SerdeError(serde_json::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::AddrBusy(addr) =>
                write!(f, "Attempted to connect to {}, however this address is busy", addr),
            Error::NoConnection(addr) =>
                write!(f, "Can't handle message from {}: noone is assigned to it", addr),
            Error::ReadChannelClosed(addr) =>
                write!(f, "Can't handle message from {}, channel is closed", addr),
            Error::WriteChannelClosed =>
                write!(f, "Channel for writing (getting udp send requests) is closed"),
            Error::ChannelsPoisoned =>
                write!(f, "Channel storage mutex was poisoned"),
            Error::UDPFewBytesSent(s) =>
                write!(f, "UDP sent less bytes than intended: {}", s),
            Error::IOError(err) => err.fmt(f),
            Error::SerdeError(err) => err.fmt(f),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;