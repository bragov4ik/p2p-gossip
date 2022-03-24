//! UDP packet distribution based on peer address.
//! 
//! This module contains tools that provide kind of multiplexing for one UDP port and socket.
//! 
//! It allows to communicate with multiple different addresses through one UDP port and routes
//! messages to channels associated with the addresses.
//! 
//! Channels handles can be obtained from [self::ChannelManager] using function `add_connection`.
//! Then they can be passed to some asynchronous job that will talk to the remote host through them.
//! 
//! # Examples  
//! 
//! Two `ConnectionManager`s on 2 randomly-assigned ports connect two handlers to each other.
//! ```no_run
//! use std::net::SocketAddr;
//! use udppm::{ ConnectionManager, ChannelManager, WriteChannel, ReadChannel, SendRequest };
//! use tokio::{time::{sleep, Duration}, sync::mpsc::error::TryRecvError::{Empty, Disconnected}};
//! 
//! async fn handle(
//!     w_channel: WriteChannel<String>, mut r_channel: ReadChannel<String>, dest: SocketAddr
//! ) {
//!     loop {
//!         loop {
//!             match r_channel.try_recv() {
//!                 Ok(msg) => println!("{} - {}", dest, msg),
//!                 Err(e) => {
//!                     match e { Empty => break, Disconnected => return, };
//!                 },
//!             }
//!         }
//!         sleep(Duration::from_secs(2)).await;
//!         let payload = "Hello, World!".to_string();
//!         w_channel.send(SendRequest{ dest, payload }).await.ok();
//!     }
//! }
//! 
//! async fn manager_test(h: ConnectionManager<String>, m: ChannelManager<String>) {
//!     let addr = h.local_addr().unwrap();
//!     // Obtain handles for writing and reading
//!     let (w, r) = 
//!         m.add_connection(addr, 32).unwrap();
//!     let fut = handle(w, r, addr);
//!     let h_fut = h.run_buf::<256>();
//!     tokio::join!(fut, h_fut);
//! }
//! 
//! async fn init_manager() -> (ConnectionManager<String>, ChannelManager<String>) {
//!     let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
//!     // Create managers with UDP socket bound to `addr`
//!     let (h, m): (ConnectionManager<String>, ChannelManager<String>) = 
//!         udppm::managers(addr, 32).await.unwrap();
//!     return (h, m)
//! }
//! 
//! #[tokio::main]
//! async fn main() {
//!     
//!     let (h1, m1) = init_manager().await;
//!     let (h2, m2) = init_manager().await;
//!     tokio::join!(
//!         manager_test(h1, m1),
//!         manager_test(h2, m2),
//!     );
//! }
//! ```

use std::{
    collections::HashMap, net::SocketAddr,
    sync::{ Mutex, Arc }, fmt::Display
};
use serde::{ Serialize, de::DeserializeOwned };
use tokio::{
    net::UdpSocket,
    sync::{ mpsc }
};

/// `ChannelManager` allows to add new connections for its associated `ConnectionManager`.
/// 
///  It is intended to be cloned where making new connections is required.
/// 
/// # Example
/// Creating a ChannelManager and using its copy when original is moved out
/// ```ignore
/// let addr: std::net::SocketAddr = "127.0.0.1:50000".parse().unwrap();
/// let (_, chan_man) = udppm::managers::<_, String>(addr, 32).await.unwrap();
/// let chan_man2 = chan_man.clone();
/// transfer_ownership(chan_man);
/// let (w_chan, r_chan) = chan_man2.add_connection(addr, 32).unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct ChannelManager<M>
where
    M: Clone
{
    /// WriteChannel object to clone when new handle is needed
    write_channel_sender: WriteChannel<M>,
    /// Mutexed map of addresses to channels - used to distribute incoming packets to
    /// corresponding channels.
    read_channels: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<M>>>>,
}

/// Channel used to write messages to the associated address
pub type WriteChannel<M> = mpsc::Sender<SendRequest<M>>;

/// Channel for reading incoming messages from the  associated address
pub type ReadChannel<M> = mpsc::Receiver<M>;

impl<M> ChannelManager<M>
where
    M: Clone
{
    /// Produces new channel handles for connecting to `addr`.
    /// 
    /// `WriteChannel<M>` is used to send data (`<M>`) to `addr`, whereas `ReadChannel<M>`
    /// allows receiving messages from `addr`.
    /// 
    /// # Example
    /// 
    /// Creating a ChannelManager and producing 2 pairs of channels.
    /// ```ignore
    /// let addr: std::net::SocketAddr = "127.0.0.1:50000".parse().unwrap();
    /// let remote_addr: std::net::SocketAddr = "8.8.8.8:53".parse().unwrap();
    /// let (_, chan_man) = udppm::managers::<_, String>(addr, 32).await.unwrap();
    /// let (w_chan, r_chan) = chan_man.add_connection(remote_addr, 32).unwrap();
    /// ```
    pub fn add_connection(
        self, addr: SocketAddr, read_buffer: usize
    ) -> Result<(WriteChannel<M>, ReadChannel<M>)> {
        let mut read_chans_map = self.read_channels.lock()
            .map_err(|_| {Error::ChannelsPoisoned})?;
        if read_chans_map.contains_key(&addr) {
            return Err(Error::AddressBusy(addr))
        }
        let (new_read_s, new_read_r) = mpsc::channel(read_buffer);
        read_chans_map.insert(addr, new_read_s);
        Ok((self.write_channel_sender.clone(), new_read_r))
    }
}

/// `ConnectionManager` processes communication channels to multiple hosts through one UDP port,
/// distributing messages of type M to separate channels.
/// 
/// The messages can be of any type that satisfies [serde::Serialize] and
/// [serde::de::DeserializeOwned] traits to allow transfer through the network. Also, [Clone] trait
/// is needed to allow cloning of ChannelManager.
/// 
/// `ConnectionManager` can be created using `managers` function together with associated
/// `ChannelManager` that is a copy of channel manager inside the `ConnectionManager`. This allows
/// to add new channels for handling messages from some addresses without interrupting process
/// of the `ConnectionManager` (i.e. `run_buf`).
///  
/// # Example
/// Creating and launching `ConnectionManager` with 256 bytes buffer for UDP packet reading.
/// ```no_run
/// #[tokio::main]
/// async fn main() {
///     let addr: std::net::SocketAddr = "127.0.0.1:50000".parse().unwrap();
///     let (con, chan) = udppm::managers::<_, String>(addr, 32).await.unwrap();
///     // Should run until some error happens
///     con.run_buf::<256>().await.unwrap();
/// }
/// ```
pub struct ConnectionManager<M>
where
    M: Serialize + DeserializeOwned + Clone {
    sock: UdpSocket,
    // Handle to receive requests for sending messages to some address in network
    write_channel_receiver: mpsc::Receiver<SendRequest<M>>,
    channel_manager: ChannelManager<M>,
}

/// SendRequest is an addressed message intended to be sent to `dest`.
/// 
/// It is needed because `ConnectionManager` cannot distinguish between producers of the request in
/// `write_channel`.
/// 
/// # Usage
/// It is supposed to be given to the `ConnectionManager` through write channel. The
/// `ConnectionManager` then will attemt to send `payload` to `dest`.
/// 
/// # Example
/// Passing a send request to `WriteChannel`
/// ```no_run
/// use std::net::SocketAddr;
/// 
/// #[tokio::main]
/// async fn main() {
///     let local_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
///     let remote_addr: SocketAddr = "8.8.8.8:53".parse().unwrap();
///     // In practice, `_con` should be running eventually to process the request
///     let (_con, chan) = udppm::managers::<_, String>(local_addr, 32).await.unwrap();
///     let (w_chan, _) = chan.add_connection(local_addr, 32).unwrap();
///     w_chan.send(
///         udppm::SendRequest{ dest: remote_addr, payload: "Hi!".to_string() }
///     ).await.ok();
/// }
/// ```
pub struct SendRequest<M> {
    pub dest: SocketAddr,
    pub payload: M,
}

impl<M> ConnectionManager<M>
where
    M: Serialize + DeserializeOwned + Clone {

    /// Returns the local address that this `ConnectionManager` is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        return self.sock.local_addr();
    }

    // Obtains read_channels HashMap with MutexGuard. Created to avoid repetition of
    // this code snippet.
    fn read_chans(&self)
    -> Result<std::sync::MutexGuard<'_, HashMap<SocketAddr, mpsc::Sender<M>>>> {
        self.channel_manager.read_channels.lock()
            .map_err(|_| {Error::ChannelsPoisoned})
    }
}

// Distribution logic
impl<M> ConnectionManager<M>
where
    M: Serialize + DeserializeOwned + Clone {
    /// Start handling the message distribution from/to UDP socket to/from channels.
    /// 
    /// Messages are represented by `M` generic type that is serialized/deserialized using
    /// `bincode` when going through the UDP socket.
    /// 
    /// Buffer size for reading from the UDP socket is specified using `B` generic constant
    /// parameter. Due to the nature of UDP and absence of packet segmentation, 508 bytes is a
    /// reasonable maximum for the buffer size.
    /// 
    /// # Limitations
    /// 
    /// Messages segmentation is not implemented, so sending large structures would likely to be
    /// unsuccessful. Also, it is a reason to use bincode, since it makes result more compact.
    /// 
    /// 
    pub async fn run_buf<const B: usize>(mut self) -> Result<()> {
        let mut buf = [0u8; B];
        loop {
            let read_fut = self.sock.recv_from(&mut buf);
            let write_fut = self.write_channel_receiver.recv();
            tokio::select! {
                result = read_fut => {
                    if let Err(e) = self.handle_read(result, &buf).await {
                        match e {
                            Error::NoConnection(_) | Error::IOError(_) | 
                            Error::ReadChannelClosed(_) | Error::SerdeError(_) => 
                                log::warn!("Error during handling receiving from network: {}", e),
                            // These errors should not be returned from read, thus unexpected
                            Error::AddressBusy(_) | Error::WriteChannelClosed | 
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
                            Error::AddressBusy(_) | Error::NoConnection(_) |
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
        let msg: M = bincode::deserialize(&buf[..bytes_read])
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
        let bytes = bincode::serialize(&msg)
            .map_err(Error::SerdeError)?;

        // Send it
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


/// Create `ConnectionManager` and corresponding `ChannelManager`.
/// 
/// `ConnectionManager` distributes messages received from UDP socket at `addr` and routes data
/// to/from different hosts into corresponding channels.
/// 
/// [ChannelManager] is responsible for creation of handles to communicate through the connection
/// manager. See its docs for details.
/// 
/// # Example
/// Creates managers for exchanging `String` values
/// ```
/// #[tokio::main]
/// async fn main() {
///     let addr: std::net::SocketAddr = "127.0.0.1:50000".parse().unwrap();
///     let (con, chan) = udppm::managers::<_, String>(addr, 32).await.unwrap();
///     assert_eq!(con.local_addr().unwrap(), addr);
/// }
/// ```
/// 
pub async fn managers<A, M>(addr: A, write_buffer: usize) -> Result<(ConnectionManager<M>, ChannelManager<M>)>
where
    A: tokio::net::ToSocketAddrs,
    M: Serialize + DeserializeOwned + Clone,
{
    let sock = UdpSocket::bind(addr).await
        .map_err(Error::IOError)?;
    let (write_channel_sender, write_channel) = 
        mpsc::channel::<SendRequest<M>>(write_buffer);
    let read_channels = Arc::new(Mutex::new(HashMap::new()));
    let channel_manager: ChannelManager<M> = ChannelManager { write_channel_sender, read_channels };
    Ok((
        ConnectionManager {
            sock, write_channel_receiver: write_channel, channel_manager: channel_manager.clone()
        }, channel_manager
    ))
}

#[derive(Debug)]
pub enum Error {
    /// This address is already connected
    AddressBusy(SocketAddr),
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
    SerdeError(bincode::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::AddressBusy(addr) =>
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
