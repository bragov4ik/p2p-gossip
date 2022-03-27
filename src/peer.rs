use std::{net::SocketAddr, collections::HashMap, sync::{Arc, Mutex}};
use tokio::{time::{Duration, Instant}, sync::mpsc, net::TcpStream};

use crate::connection::{self, AuthInfo};
use connection::{ Connection, Message };

pub type Identity = u64;

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
pub struct MutexPoisoned {}

pub type Shared<T> = Arc<Mutex<T>>;

// TODO add timeouts where applicable
#[derive(Debug)]
pub struct Peer
{
    config: Config,

    // For receiving and checking
    last_active: Instant,

    // Info about handled peer, shared
    peer_info: Shared<Info>,
    peer_identity: Arc<Identity>,

    // Info about local peer
    self_auth: AuthInfo,

    // New connections to the peer, if received on listen port
    new_connections: mpsc::Receiver<(AuthInfo, Connection<TcpStream>)>,

    // Notifications to network about new discovered peers
    new_auth: mpsc::Sender<AuthInfo>,
    
    // All known peers in the network (except local one)
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
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
    // Produces Peer with associated channel for shutting it down
    pub fn new(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        peer_listen_addr: SocketAddr,
        peer_id: Arc<Identity>,
        self_info: AuthInfo,
        new_connections: mpsc::Receiver<(AuthInfo, Connection<TcpStream>)>,
        new_peers: mpsc::Sender<AuthInfo>,
    ) -> Result<Self, Error> {
        let peer_info = Arc::new(Mutex::new(
            Info::new(peer_listen_addr)
        ));
        let peer = Peer {
            config,
            last_active: Instant::now(),
            peer_info,
            peer_identity: peer_id,
            self_auth: self_info,
            new_connections,
            new_auth: new_peers,
            peers_info
        };
        Ok(peer)
    }

    // Handle communication with a particular peer through a connection (if any)
    // tries to connect if no connection given
    pub async fn handle_peer(&mut self, mut conn_opt: Option<Connection<TcpStream>>,) -> Result<(), Error>{
        use tokio::time::interval;

        let mut ping_interval = interval(self.config.ping_period);
        let mut hb_send_interval = interval(self.config.hb_period);
        let mut hb_recv_interval = interval(self.config.hb_timeout);

        log::debug!("Start handling a peer {}", *self.peer_identity);

        loop {
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
                                    log::warn!("{}", e),
                                connection::Error::IOError(e) => log::warn!("{}", e),
                                connection::Error::StreamEnded => {
                                    log::error!("Connection closed");
                                    break;
                                },
                            }
                        },
                        Error::UnexpectedMessage(m) => {
                            log::warn!("Received unexpected message {}, ignoring", m);
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
                    (*self.peer_identity).clone(),
                    self.self_auth.clone(),
                    &mut self.new_connections,
                ).await;
                match res {
                    Ok((conn, new_listen)) => {
                        log::info!("Reconnected successfully, continuing operation");
                        conn_opt = Some(conn);
                        self.update_listen_addr(new_listen)
                            .map_err(Error::MutexPoisoned)?;
                        break;
                    },
                    Err(Error::MutexPoisoned(_)) => { return Err(res.unwrap_err()) },
                    Err(e) => log::warn!("Error while reconnecting; trying again: {:?}", e),
                }
            }
        }
    }

    pub async fn reconnect(
        peer_info: Info, peer_identity: u64, self_auth: AuthInfo,
        new_connections: &mut mpsc::Receiver<(AuthInfo, Connection<TcpStream>)>
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
                            let mut conn = Connection::from_stream(stream);
                            match Self::exchange_auth(self_auth.clone(), &mut conn).await {
                                Ok(info) => {
                                    if info.identity == peer_identity {
                                        return Ok((conn, info.listen_addr))
                                    }
                                    else {
                                        log::info!("Peer's identity didn't match");
                                        continue;
                                    }
                                },
                                Err(e) =>
                                    log::warn!("Error while authenticating: {:?}", e),
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
                        Some((auth_info, conn)) => {
                            // Double-check the identity, just in case
                            if auth_info.identity != peer_identity {
                                log::error!("Received connection with wrong identity from the
                                    network which shouldn't happen, ignoring");
                                continue;
                            }
                            // Connection was already authenticated, everything is Ok
                            return Ok((conn, auth_info.listen_addr))
                        },
                        None => {
                            return Err(Error::ChannelClosed("new connections".to_string()))
                        },
                    }
                }
            };
            if let Err(e) = res {
                log::warn!("Error while reconnecting to {:?}: {:?}", peer_listen_addr, e);
                panic!();
                continue
            }
        }
    }

    pub async fn exchange_auth(
        self_auth: AuthInfo, conn: &mut Connection<TcpStream>
    ) -> Result<AuthInfo, Error> {
        // For logging only
        let peer_addr = conn.inner_ref().peer_addr();
        log::trace!("Sending auth info to {:?}..", peer_addr);

        conn.send_message(Message::Authenticate(self_auth.clone())).await
            .map_err(Error::ConnectionError)?;

        log::trace!("Info sent to {:?}!", peer_addr);
        log::trace!("Waiting for auth info from {:?}..", peer_addr);

        let m = conn.recv_message().await
            .map_err(Error::ConnectionError)?;
        if let connection::Message::Authenticate(info) = m {
            log::trace!("Received auth info from {:?}!", peer_addr);
            Ok(info)
        }
        else {
            log::info!("Received {:?} from {:?}, but expected auth info", m, peer_addr);
            conn.send_message(
                connection::Message::Error("Expected `AddMe` message as first one".to_string())
            ).await
                .map_err(Error::ConnectionError)?;
            Err(Error::UnexpectedMessage(m))
        }
    }

    async fn handle_message(
        &mut self, m: Message, conn: &mut Connection<TcpStream>
    ) -> Result<(), Error> {
        log::debug!("Received (from {}): {:?}", *self.peer_identity, m);
        match m {
            Message::Ping => {
                let info = self.get_info_copy()
                    .map_err(Error::MutexPoisoned)?;
                log::info!("{}/{} - {}", self.peer_identity, info.last_address, m);
            },
            Message::Heartbeat => (), // We update timer on each activity after match
            Message::ListPeersRequest => {
                log::trace!("Listing peers..");
                let map = self.list_peers()
                    .map_err(Error::MutexPoisoned)?
                    .into_iter().collect();
                log::trace!("Peers listed, sending the list...");
                conn.send_message(Message::ListPeersResponse(map)).await
                    .map_err(Error::ConnectionError)?;
                log::trace!("The list has been sent to {}", *self.peer_identity);
            },
            Message::ListPeersResponse(map) => {
                log::trace!(
                    "Received list of peers from {}, looking for new entries",
                    *self.peer_identity
                );
                let known_peers = self.known_peers()
                    .map_err(Error::MutexPoisoned)?;
                for auth in map {
                    if !known_peers.contains(&auth.identity) {
                        if let Err(_) = self.new_auth.send(auth).await {
                            return Err(Error::ChannelClosed("new auth".to_owned()));
                        };
                    }
                }
            },
            Message::Authenticate(_) => return Err(Error::UnexpectedMessage(m.clone())), // unexpected?

            Message::Error(s) => log::error!("Peer sent error: {}", s),
        };
        self.last_active = Instant::now();
        Ok(())
    }

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

    fn list_peers(&self) -> Result<Vec<AuthInfo>, MutexPoisoned> {
        let peers_info = self.peers_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        
        // Get pairs `(peer_id, address)` or return `MutexPoisoned` error if at least one
        // `Info` mutex was poisoned
        peers_info.iter()
            .map(|(s, i)| {
                i.lock()
                    .map(|i| {
                        AuthInfo{
                            identity: s.to_owned(),
                            listen_addr: i.last_address
                        }
                    })
                    .map_err(|_| {MutexPoisoned{}})
            })
            .collect::<Result<Vec<AuthInfo>, MutexPoisoned>>()
    }

    fn known_peers(&self) -> Result<std::collections::HashSet<Identity>, MutexPoisoned> {
        let peers_info = self.peers_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        
        Ok(peers_info.keys().map(|k| {k.to_owned()}).collect())
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
}

