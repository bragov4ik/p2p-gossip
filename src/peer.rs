use std::{net::SocketAddr, collections::HashMap, sync::{Arc, Mutex}};
use tokio::{time::{Duration, Instant}, sync::mpsc, net::TcpStream};
use tokio_rustls::{TlsStream, rustls::{ServerConfig, ClientConfig, RootCertStore}};

use crate::{connection::{self, ConnectInfo}, authentication::Identity};
use connection::{ Connection, Message };

#[derive(Debug, Clone)]
pub struct Info {
    pub status: Status,
    pub last_address: SocketAddr,
}

impl Info {
    pub fn new(last_address: SocketAddr) -> Self {
        Info { status: Status::Alive, last_address }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Alive,
    Dead,
}

#[derive(Debug)]
pub enum Error {
    ConnectionError(connection::Error),
    UnexpectedMessage(Message),
    MutexPoisoned(MutexPoisoned),
    // Channel understandable name
    ChannelClosed(String),
}

#[derive(Debug)]
pub enum CreationError {
    MutexPoisoned(MutexPoisoned),
    PeerInfoAbsent,
}

#[derive(Debug)]
pub struct MutexPoisoned {}

pub type Shared<T> = Arc<Mutex<T>>;

// TODO add timeouts where applicable
#[derive(Debug)]
pub struct Peer
{
    config: Config,

    // For receiving and checking
    last_active: Instant,

    peer_info: Shared<Info>,
    peer_id: Arc<Identity>,
    self_listen_port: u16,
    self_id: Arc<Identity>,

    // New connections to the peer, if received on listen port
    new_connections: mpsc::Receiver<(Arc<Identity>, Connection<TcpStream>)>,

    // Notifications to network about new discovered peers
    new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,
    
    // All known peers in the network (except local one)
    peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub ping_period: Duration,
    pub hb_period: Duration,
    pub hb_timeout: Duration,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(seconds) ping_period: {}\theartbeat_period: {}\t heartbeat_timeout: {}",
            self.ping_period.as_secs(), self.hb_period.as_secs(), self.hb_timeout.as_secs())
    }
}

impl Peer {
    /// Produces Peer with associated channel for shutting it down
    /// must insert info about this peer in `peers_info`
    pub fn new(
        peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
        config: Config,
        peer_id: Arc<Identity>,
        self_listen_port: u16,
        self_id: Arc<Identity>,
        new_connections: mpsc::Receiver<(Arc<Identity>, Connection<TcpStream>)>,
        new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,
    ) -> Result<Self, CreationError> {
        let peer_info = {
            let info_map = peers_info.lock()
                .map_err(|_| {CreationError::MutexPoisoned(MutexPoisoned{})})?;
            info_map.get(&peer_id)
                .map(|a| a.clone())
        };
        let peer_info = match peer_info {
            Some(i) => i,
            None => return Err(CreationError::PeerInfoAbsent),
        };
        let peer = Peer {
            config,
            last_active: Instant::now(),
            peer_info,
            peer_id,
            self_listen_port,
            self_id,
            new_connections,
            new_auth_addr,
            peers_info
        };
        Ok(peer)
    }

    // Handle communication with a particular peer through a connection (if any)
    // tries to connect if no connection given
    #[tracing::instrument(skip_all)]
    pub async fn handle_peer(&mut self, mut conn_opt: Option<Connection<TcpStream>>,) -> Result<(), Error>{
        use tokio::time::interval;

        let mut ping_interval = interval(self.config.ping_period);
        let mut hb_send_interval = interval(self.config.hb_period);
        let mut hb_recv_interval = interval(self.config.hb_timeout);

        tracing::debug!("Start handling a peer {}", *self.peer_id);
        loop {
            if let Some(conn) = &mut conn_opt {
                tracing::debug!("Asking for list of peers after reconnect, if any");
                if let Ok(peer_listen_addr) = self.get_listen_addr() {
                    tracing::info!("Connected to {}/{}", self.peer_id, peer_listen_addr);
                }
                else {
                    tracing::error!("Mutex peer_info poisoned, unable to proceed");
                    return Err(Error::MutexPoisoned(MutexPoisoned{}))
                }
                if let Err(e) = conn.send_message(Message::ListPeersRequest).await {
                    tracing::warn!("Couldn't send list peers request: {:?}", e);
                }
            }
            loop {
                let conn = match &mut conn_opt {
                    Some(c) => c,
                    None => break, // Skip to connection establishment
                };
                let recv = conn.recv_message();
                let res = tokio::select! {
                    recv_res = recv => {
                        match recv_res {
                            Ok(m) => self.handle_message(m, conn).await,
                            Err(e) => Err(Error::ConnectionError(e)),
                        }
                    }
                    _ = ping_interval.tick() => {
                        conn.send_message(Message::Ping).await
                            .map_err(Error::ConnectionError)
                    }
                    _ = hb_send_interval.tick() => {
                        conn.send_message(Message::Heartbeat).await
                            .map_err(Error::ConnectionError)
                    }
                    _ = hb_recv_interval.tick() => {
                        self.check_heartbeat(conn).await
                    }
                };
                if let Err(e) = res {
                    match &e {
                        Error::ConnectionError(conn_e) => {
                            match conn_e {
                                connection::Error::SerializationError(e) => 
                                    tracing::warn!("{}", e),
                                connection::Error::IOError(e) => tracing::warn!("{}", e),
                                connection::Error::StreamEnded => {
                                    tracing::error!("Connection closed");
                                    break;
                                },
                            }
                        },
                        Error::UnexpectedMessage(m) => {
                            tracing::warn!("Received unexpected message {}, ignoring", m);
                        },
                        Error::MutexPoisoned(_) => {
                            return Err(e);
                        },
                        Error::ChannelClosed(_) => {
                            return Err(e);
                        },
                    }
                }
            }
            loop {
                let info = self.get_info_copy()
                    .map_err(Error::MutexPoisoned)?;
                let res = Self::reconnect(
                    info,
                    self.peer_id.clone(),
                    self.self_id.clone(),
                    self.self_listen_port,
                    &mut self.new_connections,
                ).await;
                match res {
                    Ok((conn, new_listen)) => {
                        tracing::debug!("(Re)connected to {}/{}", self.peer_id, new_listen);
                        conn_opt = Some(conn);
                        self.update_listen_addr(new_listen)
                            .map_err(Error::MutexPoisoned)?;
                        break;
                    },
                    Err(Error::MutexPoisoned(_)) => { return Err(res.unwrap_err()) },
                    Err(e) => tracing::warn!("Error while reconnecting; trying again: {:?}", e),
                }
            }
        }
    }

    #[tracing::instrument(skip(new_connections))]
    pub async fn reconnect(
        peer_info: Info, peer_id: Arc<Identity>, self_auth: Arc<Identity>, self_listen_port: u16,
        new_connections: &mut mpsc::Receiver<(Arc<Identity>, Connection<TcpStream>)>
    ) -> Result<(Connection<TcpStream>, SocketAddr), Error> {
        loop {
            let peer_listen_addr = peer_info.last_address;

            let try_connect = TcpStream::connect(peer_listen_addr);
            let receive_connection = new_connections.recv();
            // We either reconnect by ourselves or receive new connection from the Network
            // (the peer connected through the listen port)
            let res = tokio::select! {
                connect_res = try_connect => {
                    match connect_res {
                        Ok(stream) => {
                            tracing::trace!("Trying to connect to {:?}", peer_listen_addr);
                            let peer_con_addr = stream.peer_addr()
                                .map_err(connection::Error::IOError)
                                .map_err(Error::ConnectionError)?;
                            match Self::exchange_pair(
                                self_auth.clone(), self_listen_port, stream
                            ).await {
                                Ok((peer_id_received, peer_listen_port, conn)) => {
                                    if *peer_id_received == *peer_id {
                                        let mut peer_listen_addr = peer_con_addr;
                                        peer_listen_addr.set_port(peer_listen_port);
                                        return Ok((conn, peer_listen_addr))
                                    }
                                    else {
                                        tracing::info!("Peer's identity didn't match");
                                        continue;
                                    }
                                },
                                Err(e) =>
                                    tracing::warn!("Error while authenticating: {:?}", e),
                            };
                            Ok(())
                        },
                        Err(e) => {
                            Err(Error::ConnectionError(connection::Error::IOError(e)))
                        },
                    }
                },
                receive_opt = receive_connection => {
                    match receive_opt {
                        Some((peer_id_received, conn)) => {
                            // Double-check the identity, just in case
                            if *peer_id_received != *peer_id {
                                tracing::error!("Received connection with wrong identity from the
                                    network which shouldn't happen, ignoring");
                                continue;
                            }
                            let peer_addr = conn.inner_ref().peer_addr()
                                .map_err(connection::Error::IOError)
                                .map_err(Error::ConnectionError)?;
                            // Connection was already authenticated, everything is Ok
                            return Ok((conn, peer_addr))
                        },
                        None => {
                            return Err(Error::ChannelClosed("new connections".to_string()))
                        },
                    }
                }
            };
            if let Err(e) = res {
                tracing::warn!("Error while reconnecting to {:?}: {:?}", peer_listen_addr, e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue
            }
        }
    }

    // async fn auth_server() {
    //     let tls_config = ServerConfig::builder()
    //         .with_safe_defaults()
    //         .with_no_client_auth()
    //         .
    // }

    // async fn auth_client(
    //     self_auth: Arc<Identity>, self_listen_port: u16, stream: TcpStream
    // ) -> Result<(Arc<Identity>, u16, Connection<TlsStream<TcpStream>>), Error> {
    //     let (peer_id, peer_listen_port, conn) = Self::exchange_pair(
    //         self_auth, self_listen_port, stream
    //     ).await?;
    //     let crypto = rustls::ClientConfig::builder()
    //         .with_safe_defaults()
    //         .with_custom_certificate_verifier();
    //     Ok(())
    // }

    // #[tracing::instrument(skip(stream))]
    pub async fn exchange_pair(
        self_auth: Arc<Identity>, self_listen_port: u16, stream: TcpStream
    ) -> Result<(Arc<Identity>, u16, Connection<TcpStream>), Error> {
        
        let mut conn = Connection::from_stream(stream);
        // For logging only
        let peer_addr = conn.inner_ref().peer_addr();
        tracing::trace!("Sending auth info to {:?}..", peer_addr);

        conn.send_message(Message::Pair(
            ConnectInfo{
                identity: (*self_auth),
                listen_port: self_listen_port,
            }
        )).await
            .map_err(Error::ConnectionError)?;

        tracing::trace!("Info sent to {:?}!", peer_addr);
        tracing::trace!("Waiting for auth info from {:?}..", peer_addr);

        let m = conn.recv_message().await
            .map_err(Error::ConnectionError)?;
        if let connection::Message::Pair(info) = m {
            tracing::trace!("Received auth info from {:?}!", peer_addr);
            let identity = Arc::new(info.identity);
            Ok((identity, info.listen_port, conn))
        }
        else {
            tracing::info!("Received {:?} from {:?}, but expected auth info", m, peer_addr);
            conn.send_message(
                connection::Message::Error("Expected `AddMe` message as first one".to_string())
            ).await
                .map_err(Error::ConnectionError)?;
            Err(Error::UnexpectedMessage(m))
        }
    }

    #[tracing::instrument(skip_all)]
    async fn handle_message(
        &mut self, m: Message, conn: &mut Connection<TcpStream>
    ) -> Result<(), Error> {
        tracing::debug!("Received (from {}): {:?}", *self.peer_id, m);
        match m {
            Message::Ping => {
                let info = self.get_info_copy()
                    .map_err(Error::MutexPoisoned)?;
                tracing::info!("{}/{} - {}", self.peer_id, info.last_address, m);
            },
            Message::Heartbeat => (), // We update timer on each activity after match
            Message::ListPeersRequest => {
                tracing::trace!("Listing peers..");
                let list = self.list_peers()
                    .map_err(Error::MutexPoisoned)?
                    .into_iter().collect();
                tracing::trace!("Peers listed, sending the list...");
                conn.send_message(Message::ListPeersResponse(list)).await
                    .map_err(Error::ConnectionError)?;
                tracing::trace!("The list has been sent to {}", *self.peer_id);
            },
            Message::ListPeersResponse(map) => {
                tracing::trace!(
                    "Received list of peers from {}, looking for new entries",
                    *self.peer_id
                );
                let known_peers = self.known_peers()
                    .map_err(Error::MutexPoisoned)?;
                for (auth, peer_listen_addr) in map {
                    if !known_peers.contains(&auth) && auth != *self.self_id {
                        if let Err(_) = self.new_auth_addr.send(
                            (Arc::new(auth), peer_listen_addr)
                        ).await {
                            return Err(Error::ChannelClosed("new auth".to_owned()));
                        };
                    }
                }
            },
            Message::Pair(_) => return Err(Error::UnexpectedMessage(m.clone())), // unexpected?

            Message::Error(s) => tracing::error!("Peer sent error: {}", s),
        };
        self.last_active = Instant::now();
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn check_heartbeat(&mut self, conn: &mut Connection<TcpStream>) -> Result<(), Error> {
        let elapsed = self.last_active.elapsed();
        if elapsed > self.config.hb_timeout {
            // Dead
            self.update_status(Status::Dead)
                .map_err(Error::MutexPoisoned)?;
        }
        else {
            // Alive
            let info = self.get_info_copy()
                .map_err(Error::MutexPoisoned)?;
            if info.status == Status::Dead {
                // Peer transitions from Dead to Alive state, so we ask it for new entries
                // in case the network was split
                conn.send_message(Message::ListPeersRequest).await
                    .map_err(Error::ConnectionError)?;
                self.update_status(Status::Alive)
                    .map_err(Error::MutexPoisoned)?;
            }
        }
        Ok(())
    }

    fn list_peers(&self) -> Result<Vec<(Identity, SocketAddr)>, MutexPoisoned> {
        let peers_info = self.peers_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        
        // Get pairs `(peer_auth, address)` or return `MutexPoisoned` error if at least one
        // `Info` mutex was poisoned
        peers_info.iter()
            .map(|(s, i)| {
                i.lock()
                    .map(|i| {
                        (
                            (**s).clone(),
                            i.last_address,
                        )
                    })
                    .map_err(|_| {MutexPoisoned{}})
            })
            .collect::<Result<Vec<(Identity, SocketAddr)>, MutexPoisoned>>()
    }

    fn known_peers(&self) -> Result<std::collections::HashSet<Identity>, MutexPoisoned> {
        let peers_info = self.peers_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        
        Ok(peers_info.keys().map(|k| {(**k).clone()}).collect())
    }

    // was made to isolate lock and not hold it across awaits
    fn get_info_copy(&self) -> Result<Info, MutexPoisoned> {
        let info = self.peer_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        Ok(info.clone())
    }

    // same, lock isolation to avoid deadlock
    fn update_status(&self, new_status: Status) -> Result<(), MutexPoisoned> {
        let mut info = self.peer_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        info.status = new_status;
        Ok(())
    }
    
    // same, lock isolation to avoid deadlock
    fn update_listen_addr(&self, addr: SocketAddr) -> Result<(), MutexPoisoned> {
        let mut info = self.peer_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        info.last_address = addr;
        Ok(())
    }

    // same
    fn get_listen_addr(&self) -> Result<SocketAddr, MutexPoisoned> {
        let info = self.peer_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        Ok(info.last_address)
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;
    use tracing::{warn, Level};
    use super::*;

    #[derive(Debug)]
    enum Error {
        IOError(std::io::Error),
        PeerCreationError(CreationError),
    }

    fn insert_info(
        peer_id: Arc<Identity>,
        peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
        peer_listen_addr: SocketAddr,
    ) -> Result<Option<Shared<Info>>, super::MutexPoisoned> {
        let mut info_map = peers_info.lock()
            .map_err(|_| {super::MutexPoisoned{}})?;
        let new_info = Arc::new(Mutex::new(Info::new(peer_listen_addr)));
        tracing::debug!("Updated successfully");
        Ok(info_map.insert(peer_id, new_info))
    }

    async fn test_peer(
        stream: TcpStream,
        peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
        peer_id: Arc<Identity>,
        self_id: Arc<Identity>,
        peer_listen_port: u16,
        self_listen_port: u16,
    ) -> Result<(), Error> {
        let conn = Connection::from_stream(stream);
        let config = Config { 
            ping_period: Duration::from_secs(1),
            hb_period: Duration::from_secs(1),
            hb_timeout: Duration::from_secs(3) 
        };
        let stub_channels = (
            mpsc::channel(1).0, mpsc::channel(1).1
        );
        let peer_listen_addr = SocketAddr::new(
            conn.inner_ref().peer_addr().map_err(Error::IOError)?.ip(), 
            peer_listen_port
        );
        insert_info(peer_id.clone(), peers_info.clone(), peer_listen_addr).unwrap();
        let mut peer = Peer::new(
            peers_info,
            config,
            peer_id,
            self_listen_port,
            self_id,
            stub_channels.1,
            stub_channels.0
        ).map_err(Error::PeerCreationError)?;

        // For now wait and check if they see each other as alive hard to come up with automated
        // tests for such stuff with these tools
        // Should timeout
        assert!(tokio::time::timeout(
            Duration::from_secs(3),peer.handle_peer(Some(conn))
        ).await.is_err());
        assert_eq!(peer.get_info_copy().unwrap().status, Status::Alive);
        Ok(())
    }

    async fn accept_test_peer(
        listener: TcpListener,
        peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
        peer_id: Arc<Identity>,
        self_id: Arc<Identity>,
        peer_listen_port: u16,
    ) -> Result<(), Error> {
        let stream = listener.accept().await
            .map_err(Error::IOError)?;
        test_peer(
            stream.0,
            peers_info,
            peer_id,
            self_id,
            peer_listen_port,
            listener.local_addr().unwrap().port(),
        ).await
    }

    async fn connect_test_peer(
        connect_to: SocketAddr,
        peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
        peer_id: Arc<Identity>,
        self_id: Arc<Identity>,
        self_listen_port: u16,
    ) -> Result<(), Error> {
        let stream = TcpStream::connect(connect_to).await
            .map_err(Error::IOError)?;
        test_peer(stream, peers_info, peer_id, self_id, connect_to.port(), self_listen_port).await
    }

    fn init_testing() -> 
        Shared<HashMap<Arc<Identity>, Shared<Info>>>
    {
        let peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>> = 
            Arc::new(Mutex::new(HashMap::new()));
        peers_info
    }

    fn init_debugging() {
        if let Err(e) = tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .try_init() 
        {
            warn!("Couldn't init tracing_subscriber: {}", e);
        }
    }

    #[tokio::test]
    async fn two_simple_peers() {
        init_debugging();
        let peers_info = init_testing();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let peer1_listen_port = listen_addr.port();
        let peer1_id = Identity::new(b"1");
        let peer2_id = Identity::new(b"2");
        let peer1_test = connect_test_peer(
            listen_addr,
            peers_info.clone(),
            peer2_id.clone(),
            peer1_id.clone(),
            0
        );
        let peer2_test = accept_test_peer(
            listener,
            peers_info.clone(),
            peer1_id.clone(),
            peer2_id.clone(),
            peer1_listen_port,
        );
        let (res1, res2) = tokio::join!(peer1_test, peer2_test);
        res1.unwrap();
        res2.unwrap();
    }
}
