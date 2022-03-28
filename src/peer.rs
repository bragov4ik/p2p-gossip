use rustls::{Certificate, PrivateKey};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use crate::{
    auth::{self, Identity},
    connection::{self},
    utils::MutexPoisoned, network::PeerInfoMap,
};
use connection::Connection;

#[derive(Debug, Clone)]
pub struct Info {
    pub status: Status,
    pub last_address: SocketAddr,
}

impl Info {
    pub fn new(last_address: SocketAddr) -> Self {
        Info {
            status: Status::Alive,
            last_address,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Alive,
    Dead,
}

#[derive(Debug)]
pub enum Error {
    Connection(connection::Error),
    UnexpectedMessage(Message),
    MutexPoisoned(MutexPoisoned),
    // Channel understandable name
    ChannelClosed(String),
    Tls(rustls::Error),
}

#[derive(Debug)]
pub enum CreationError {
    MutexPoisoned(MutexPoisoned),
    PeerInfoAbsent,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Message {
    Ping,
    Heartbeat,
    ListPeersRequest,
    ListPeersResponse(Vec<(Identity, SocketAddr)>),
    Pair(ConnectInfo),
    Error(String),
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Ping => write!(f, "Ping"),
            Message::Heartbeat => write!(f, "Heartbeat"),
            Message::ListPeersRequest => write!(f, "ListPeersRequest"),
            Message::ListPeersResponse(map) => write!(f, "ListPeersResponse {:?}", map),
            Message::Pair(add) => write!(f, "AddMe {}", add),
            Message::Error(s) => write!(f, "Error: {}", s),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ConnectInfo {
    pub identity: Identity,
    pub listen_port: u16,
}

impl Display for ConnectInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.identity)
    }
}

#[derive(Debug)]
pub struct Peer {
    config: Config,

    // For receiving and checking
    last_active: Instant,

    peer_info: Arc<Mutex<Info>>,
    peer_id: Arc<Identity>,
    self_listen_port: u16,
    self_id: Arc<Identity>,

    // New connections to the peer, if received on listen port
    new_connections: mpsc::Receiver<Connection<TlsStream<TcpStream>>>,

    // Notifications to network about new discovered peers
    new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,

    // All known peers in the network (except local one)
    peers_info: Arc<Mutex<PeerInfoMap>>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub ping_period: Duration,
    pub hb_period: Duration,
    pub hb_timeout: Duration,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(seconds) ping_period: {}\theartbeat_period: {}\t heartbeat_timeout: {}",
            self.ping_period.as_secs(),
            self.hb_period.as_secs(),
            self.hb_timeout.as_secs()
        )
    }
}

impl Peer {
    /// Produces Peer with associated channel for shutting it down
    /// must insert info about this peer in `peers_info`
    pub fn new(
        peers_info: Arc<Mutex<PeerInfoMap>>,
        config: Config,
        peer_id: Arc<Identity>,
        self_listen_port: u16,
        self_id: Arc<Identity>,
        new_connections: mpsc::Receiver<Connection<TlsStream<TcpStream>>>,
        new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,
    ) -> Result<Self, CreationError> {
        let peer_info = {
            let info_map = peers_info
                .lock()
                .map_err(|_| CreationError::MutexPoisoned(MutexPoisoned {}))?;
            info_map.get(&peer_id).cloned()
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
            peers_info,
        };
        Ok(peer)
    }

    // Handle communication with a particular peer through a connection (if any)
    // tries to connect if no connection given
    #[tracing::instrument(skip_all)]
    pub async fn handle_peer(
        &mut self,
        mut conn_opt: Option<Connection<TlsStream<TcpStream>>>,
        self_private_key: PrivateKey,
        self_cert: Certificate,
    ) -> Result<(), Error> {
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
                } else {
                    tracing::error!("Mutex peer_info poisoned, unable to proceed");
                    return Err(Error::MutexPoisoned(MutexPoisoned {}));
                }
                if let Err(e) = conn.send_message(Message::ListPeersRequest).await {
                    tracing::warn!("Couldn't send list peers request: {:?}", e);
                }
            }
            // Skip to connection establishment if none
            while let Some(conn) = &mut conn_opt {
                let res = tokio::select! {
                    recv_res = conn.recv_message() => {
                        match recv_res {
                            Ok(m) => self.handle_message(m, conn).await,
                            Err(e) => Err(Error::Connection(e)),
                        }
                    }
                    _ = ping_interval.tick() => {
                        conn.send_message(Message::Ping).await
                            .map_err(Error::Connection)
                    }
                    _ = hb_send_interval.tick() => {
                        conn.send_message(Message::Heartbeat).await
                            .map_err(Error::Connection)
                    }
                    _ = hb_recv_interval.tick() => {
                        self.check_heartbeat(conn).await
                    }
                };
                if let Err(e) = res {
                    match &e {
                        Error::Connection(conn_e) => match conn_e {
                            connection::Error::Serialization(e) => tracing::warn!("{}", e),
                            connection::Error::IO(e) => tracing::warn!("{}", e),
                            connection::Error::StreamEnded => {
                                tracing::error!("Connection closed");
                                break;
                            }
                        },
                        Error::UnexpectedMessage(m) => {
                            tracing::warn!("Received unexpected message {}, ignoring", m);
                        }
                        Error::Tls(e) => {
                            tracing::warn!("Tls error: {}", e);
                        }
                        Error::MutexPoisoned(_) => {
                            return Err(e);
                        }
                        Error::ChannelClosed(_) => {
                            return Err(e);
                        }
                    }
                }
            }
            loop {
                let info = self.get_info_copy().map_err(Error::MutexPoisoned)?;
                let res = Self::reconnect(
                    info,
                    self.peer_id.clone(),
                    self.self_id.clone(),
                    self.self_listen_port,
                    self_private_key.clone(),
                    self_cert.clone(),
                    &mut self.new_connections,
                )
                .await;
                match res {
                    Ok((conn, new_listen)) => {
                        tracing::debug!("(Re)connected to {}/{}", self.peer_id, new_listen);
                        conn_opt = Some(conn);
                        self.update_listen_addr(new_listen)
                            .map_err(Error::MutexPoisoned)?;
                        break;
                    }
                    Err(Error::MutexPoisoned(_)) => return Err(res.unwrap_err()),
                    Err(e) => tracing::warn!("Error while reconnecting; trying again: {:?}", e),
                }
            }
        }
    }

    #[tracing::instrument(skip(new_connections))]
    async fn reconnect(
        peer_info: Info,
        peer_id: Arc<Identity>,
        self_auth: Arc<Identity>,
        self_listen_port: u16,
        self_private_key: PrivateKey,
        self_cert: Certificate,
        new_connections: &mut mpsc::Receiver<Connection<TlsStream<TcpStream>>>,
    ) -> Result<(Connection<TlsStream<TcpStream>>, SocketAddr), Error> {
        loop {
            let peer_listen_addr = peer_info.last_address;

            // We either reconnect by ourselves or receive new connection from the Network
            // (the peer connected through the listen port)
            let res = tokio::select! {
                connect_res = TcpStream::connect(peer_listen_addr) => {
                    match connect_res {
                        Ok(stream) => {
                            tracing::trace!("Trying to connect to {:?}", peer_listen_addr);
                            let peer_con_addr = stream.peer_addr()
                                .map_err(connection::Error::IO)
                                .map_err(Error::Connection)?;
                            match Self::auth_existing_client(
                                self_auth.clone(), peer_id.clone(), self_listen_port, stream,
                                self_private_key.clone(), self_cert.clone()
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
                            Err(Error::Connection(connection::Error::IO(e)))
                        },
                    }
                },
                receive_opt = new_connections.recv() => {
                    match receive_opt {
                        Some(conn) => {
                            let peer_addr = conn.get_ref().get_ref().0.peer_addr()
                                .map_err(connection::Error::IO)
                                .map_err(Error::Connection)?;
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
                tracing::warn!(
                    "Error while reconnecting to {:?}: {:?}",
                    peer_listen_addr,
                    e
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
    }

    pub async fn auth_new_client(
        self_auth: Arc<Identity>,
        self_listen_port: u16,
        stream: TcpStream,
        private_key: PrivateKey,
        cert: Certificate,
    ) -> Result<(Arc<Identity>, u16, Connection<TlsStream<TcpStream>>), Error> {
        Self::auth_client(self_auth, None, self_listen_port, stream, private_key, cert).await
    }

    pub async fn auth_existing_client(
        self_auth: Arc<Identity>,
        peer_auth: Arc<Identity>,
        self_listen_port: u16,
        stream: TcpStream,
        private_key: PrivateKey,
        cert: Certificate,
    ) -> Result<(Arc<Identity>, u16, Connection<TlsStream<TcpStream>>), Error> {
        Self::auth_client(
            self_auth,
            Some(peer_auth),
            self_listen_port,
            stream,
            private_key,
            cert,
        )
        .await
    }

    async fn auth_client(
        self_auth: Arc<Identity>,
        peer_auth: Option<Arc<Identity>>,
        self_listen_port: u16,
        stream: TcpStream,
        private_key: PrivateKey,
        cert: Certificate,
    ) -> Result<(Arc<Identity>, u16, Connection<TlsStream<TcpStream>>), Error> {
        let (peer_auth_received, peer_listen_port, conn) =
            Self::exchange_pair(self_auth, self_listen_port, stream).await?;
        let peer_auth = match peer_auth {
            Some(a) => a,
            None => peer_auth_received,
        };
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(auth::PeerVerifier::new((*peer_auth).clone()))
            .with_single_cert(vec![cert], private_key)
            .map_err(Error::Tls)?;
        let config = TlsConnector::from(Arc::new(config));
        let example_com = "example.com".try_into().unwrap();
        let stream = config
            .connect(example_com, conn.into_inner())
            .await
            .map_err(connection::Error::IO)
            .map_err(Error::Connection)?;
        let stream = tokio_rustls::TlsStream::Client(stream);
        let conn = Connection::from_stream(stream);
        Ok((peer_auth, peer_listen_port, conn))
    }

    pub async fn auth_server(
        self_auth: Arc<Identity>,
        self_listen_port: u16,
        stream: TcpStream,
        private_key: PrivateKey,
        cert: Certificate,
    ) -> Result<(Arc<Identity>, u16, Connection<TlsStream<TcpStream>>), Error> {
        let (peer_id, peer_listen_port, conn) =
            Self::exchange_pair(self_auth, self_listen_port, stream).await?;
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(auth::PeerVerifier::new((*peer_id).clone()))
            .with_single_cert(vec![cert], private_key)
            .map_err(Error::Tls)?;
        let config = TlsAcceptor::from(Arc::new(config));
        let stream = config
            .accept(conn.into_inner())
            .await
            .map_err(connection::Error::IO)
            .map_err(Error::Connection)?;
        let stream = tokio_rustls::TlsStream::Server(stream);
        let conn = Connection::from_stream(stream);
        Ok((peer_id, peer_listen_port, conn))
    }

    #[tracing::instrument(skip(stream))]
    async fn exchange_pair(
        self_auth: Arc<Identity>,
        self_listen_port: u16,
        stream: TcpStream,
    ) -> Result<(Arc<Identity>, u16, Connection<TcpStream>), Error> {
        let mut conn = Connection::from_stream(stream);
        // For logging only
        let peer_addr = conn.get_ref().peer_addr();
        tracing::trace!("Sending auth info to {:?}..", peer_addr);

        conn.send_message(Message::Pair(ConnectInfo {
            identity: (*self_auth).clone(),
            listen_port: self_listen_port,
        }))
        .await
        .map_err(Error::Connection)?;

        tracing::trace!("Info sent to {:?}!", peer_addr);
        tracing::trace!("Waiting for auth info from {:?}..", peer_addr);

        let m = conn.recv_message().await.map_err(Error::Connection)?;
        if let Message::Pair(info) = m {
            tracing::trace!("Received auth info from {:?}!", peer_addr);
            let identity = Arc::new(info.identity);
            Ok((identity, info.listen_port, conn))
        } else {
            tracing::info!(
                "Received {:?} from {:?}, but expected auth info",
                m,
                peer_addr
            );
            conn.send_message(Message::Error(
                "Expected `AddMe` message as first one".to_string(),
            ))
            .await
            .map_err(Error::Connection)?;
            Err(Error::UnexpectedMessage(m))
        }
    }

    #[tracing::instrument(skip_all)]
    async fn handle_message(
        &mut self,
        m: Message,
        conn: &mut Connection<TlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        tracing::debug!("Received (from {}): {:?}", *self.peer_id, m);
        match m {
            Message::Ping => {
                let info = self.get_info_copy().map_err(Error::MutexPoisoned)?;
                tracing::info!("{}/{} - {}", self.peer_id, info.last_address, m);
            }
            Message::Heartbeat => (), // We update timer on each activity after match
            Message::ListPeersRequest => {
                tracing::trace!("Listing peers..");
                let list = self
                    .list_peers()
                    .map_err(Error::MutexPoisoned)?
                    .into_iter()
                    .collect();
                tracing::trace!("Peers listed, sending the list...");
                conn.send_message(Message::ListPeersResponse(list))
                    .await
                    .map_err(Error::Connection)?;
                tracing::trace!("The list has been sent to {}", *self.peer_id);
            }
            Message::ListPeersResponse(map) => {
                tracing::trace!(
                    "Received list of peers from {}, looking for new entries",
                    *self.peer_id
                );
                let known_peers = self.known_peers().map_err(Error::MutexPoisoned)?;
                for (auth, peer_listen_addr) in map {
                    if !known_peers.contains(&auth) && auth != *self.self_id 
                        && self.new_auth_addr
                            .send((Arc::new(auth), peer_listen_addr))
                            .await.is_err()
                    {
                        return Err(Error::ChannelClosed("new auth".to_owned()));
                    }
                }
            }
            Message::Pair(_) => return Err(Error::UnexpectedMessage(m.clone())), // unexpected?

            Message::Error(s) => tracing::error!("Peer sent error: {}", s),
        };
        self.last_active = Instant::now();
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn check_heartbeat(
        &mut self,
        conn: &mut Connection<TlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        let elapsed = self.last_active.elapsed();
        if elapsed > self.config.hb_timeout {
            // Dead
            self.update_status(Status::Dead)
                .map_err(Error::MutexPoisoned)?;
        } else {
            // Alive
            let info = self.get_info_copy().map_err(Error::MutexPoisoned)?;
            if info.status == Status::Dead {
                // Peer transitions from Dead to Alive state, so we ask it for new entries
                // in case the network was split
                conn.send_message(Message::ListPeersRequest)
                    .await
                    .map_err(Error::Connection)?;
                self.update_status(Status::Alive)
                    .map_err(Error::MutexPoisoned)?;
            }
        }
        Ok(())
    }

    fn list_peers(&self) -> Result<Vec<(Identity, SocketAddr)>, MutexPoisoned> {
        let peers_info = self.peers_info.lock().map_err(|_| MutexPoisoned {})?;

        // Get pairs `(peer_auth, address)` or return `MutexPoisoned` error if at least one
        // `Info` mutex was poisoned
        peers_info
            .iter()
            .map(|(s, i)| {
                i.lock()
                    .map(|i| ((**s).clone(), i.last_address))
                    .map_err(|_| MutexPoisoned {})
            })
            .collect::<Result<Vec<(Identity, SocketAddr)>, MutexPoisoned>>()
    }

    fn known_peers(&self) -> Result<std::collections::HashSet<Identity>, MutexPoisoned> {
        let peers_info = self.peers_info.lock().map_err(|_| MutexPoisoned {})?;

        Ok(peers_info.keys().map(|k| (**k).clone()).collect())
    }

    // was made to isolate lock and not hold it across awaits
    fn get_info_copy(&self) -> Result<Info, MutexPoisoned> {
        let info = self.peer_info.lock().map_err(|_| MutexPoisoned {})?;
        Ok(info.clone())
    }

    // same, lock isolation to avoid deadlock
    fn update_status(&self, new_status: Status) -> Result<(), MutexPoisoned> {
        let mut info = self.peer_info.lock().map_err(|_| MutexPoisoned {})?;
        info.status = new_status;
        Ok(())
    }

    // same, lock isolation to avoid deadlock
    fn update_listen_addr(&self, addr: SocketAddr) -> Result<(), MutexPoisoned> {
        let mut info = self.peer_info.lock().map_err(|_| MutexPoisoned {})?;
        info.last_address = addr;
        Ok(())
    }

    // same
    fn get_listen_addr(&self) -> Result<SocketAddr, MutexPoisoned> {
        let info = self.peer_info.lock().map_err(|_| MutexPoisoned {})?;
        Ok(info.last_address)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::{gen_private_key_cert, init_debugging};
    use tokio::net::TcpListener;
    use tracing::Level;

    use super::*;

    #[derive(Debug)]
    enum Error {
        IOError(std::io::Error),
        PeerCreationError(CreationError),
    }

    fn insert_info(
        peer_id: Arc<Identity>,
        peers_info: Arc<Mutex<PeerInfoMap>>,
        peer_listen_addr: SocketAddr,
    ) -> Result<Option<Arc<Mutex<Info>>>, super::MutexPoisoned> {
        let mut info_map = peers_info.lock().map_err(|_| super::MutexPoisoned {})?;
        let new_info = Arc::new(Mutex::new(Info::new(peer_listen_addr)));
        tracing::debug!("Updated successfully");
        Ok(info_map.insert(peer_id, new_info))
    }

    async fn test_peer(
        conn: Connection<TlsStream<TcpStream>>,
        peers_info: Arc<Mutex<PeerInfoMap>>,
        peer_id: Arc<Identity>,
        self_id: Arc<Identity>,
        peer_listen_port: u16,
        self_listen_port: u16,
        self_key: PrivateKey,
        self_cert: Certificate,
    ) -> Result<(), Error> {
        let config = Config {
            ping_period: Duration::from_secs(1),
            hb_period: Duration::from_secs(1),
            hb_timeout: Duration::from_secs(3),
        };
        let stub_channels = (mpsc::channel(1).0, mpsc::channel(1).1);
        let peer_listen_addr = SocketAddr::new(
            conn.get_ref()
                .get_ref()
                .0
                .peer_addr()
                .map_err(Error::IOError)?
                .ip(),
            peer_listen_port,
        );
        insert_info(peer_id.clone(), peers_info.clone(), peer_listen_addr).unwrap();
        let mut peer = Peer::new(
            peers_info,
            config,
            peer_id,
            self_listen_port,
            self_id,
            stub_channels.1,
            stub_channels.0,
        )
        .map_err(Error::PeerCreationError)?;

        // For now wait and check if they see each other as alive hard to come up with automated
        // tests for such stuff with these tools
        // Should timeout
        assert!(tokio::time::timeout(
            Duration::from_secs(3),
            peer.handle_peer(Some(conn), self_key, self_cert)
        )
        .await
        .is_err());
        assert_eq!(peer.get_info_copy().unwrap().status, Status::Alive);
        Ok(())
    }

    async fn accept_test_peer(
        listener: TcpListener,
        peers_info: Arc<Mutex<PeerInfoMap>>,
        self_id: Arc<Identity>,
        self_key: PrivateKey,
        self_cert: Certificate,
    ) -> Result<(), Error> {
        let stream = listener.accept().await.map_err(Error::IOError)?;
        let (peer_id, peer_listen_port, conn) = Peer::auth_server(
            self_id.clone(),
            listener.local_addr().unwrap().port(),
            stream.0,
            self_key.clone(),
            self_cert.clone(),
        )
        .await
        .unwrap();
        test_peer(
            conn,
            peers_info,
            peer_id,
            self_id,
            peer_listen_port,
            listener.local_addr().unwrap().port(),
            self_key,
            self_cert,
        )
        .await
    }

    async fn connect_test_peer(
        connect_to: SocketAddr,
        peers_info: Arc<Mutex<PeerInfoMap>>,
        peer_id: Arc<Identity>,
        self_id: Arc<Identity>,
        self_key: PrivateKey,
        self_cert: Certificate,
        self_listen_port: u16,
    ) -> Result<(), Error> {
        let stream = TcpStream::connect(connect_to)
            .await
            .map_err(Error::IOError)?;
        let (peer_id, peer_listen_port, conn) = Peer::auth_existing_client(
            self_id.clone(),
            peer_id,
            self_listen_port,
            stream,
            self_key.clone(),
            self_cert.clone(),
        )
        .await
        .unwrap();
        test_peer(
            conn,
            peers_info,
            peer_id,
            self_id,
            peer_listen_port,
            self_listen_port,
            self_key,
            self_cert,
        )
        .await
    }

    fn init_testing() -> Arc<Mutex<PeerInfoMap>> {
        let peers_info: Arc<Mutex<PeerInfoMap>> =
            Arc::new(Mutex::new(HashMap::new()));
        peers_info
    }

    #[tokio::test]
    async fn two_simple_peers() {
        init_debugging(Level::INFO);
        let peers_info = init_testing();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let (peer1_cert, peer1_key) = gen_private_key_cert();
        let (peer2_cert, peer2_key) = gen_private_key_cert();
        let peer1_id = Identity::new(&peer1_cert.0[..]);
        let peer2_id = Identity::new(&peer2_cert.0[..]);
        let peer1_test = connect_test_peer(
            listen_addr,
            peers_info.clone(),
            peer2_id.clone(),
            peer1_id.clone(),
            peer1_key.clone(),
            peer1_cert.clone(),
            0,
        );
        let peer2_test = accept_test_peer(
            listener,
            peers_info.clone(),
            peer1_id.clone(),
            peer2_key.clone(),
            peer2_cert.clone(),
        );
        let (res1, res2) = tokio::join!(peer1_test, peer2_test);
        res1.unwrap();
        res2.unwrap();
    }
}
