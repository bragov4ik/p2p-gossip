use std::{net::SocketAddr, collections::HashMap, sync::{Arc, Mutex}};
use tokio::{time::{Duration, Instant}, sync::mpsc, net::TcpStream};

use crate::connection;
use connection::{ Connection, Message };

pub type Identity = u64;

#[derive(Debug, Clone)]
pub struct Info {
    pub id: Identity,
    pub status: Status,
    pub last_address: SocketAddr,
}

impl Info {
    fn new(id: Identity, last_address: SocketAddr) -> Self {
        Info { id, status: Status::Alive, last_address }
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
enum AuthenticationError {
    WrongCredentials,
    PeerError(Error),
}

#[derive(Debug)]
pub struct MutexPoisoned {}

pub type Shared<T> = Arc<Mutex<T>>;

// TODO add timeouts where applicable
#[derive(Debug)]
pub struct Peer
{
    conn: Connection<TcpStream>,
    config: Config,

    // For receiving and checking
    last_active: Instant,

    // Info about handled peer, shared
    peer_info: Shared<Info>,

    // Info about local peer
    self_info: Arc<connection::AuthInfo>,

    // New connections to the peer, if received on listen port
    new_connections: mpsc::Receiver<(connection::AuthInfo, Connection<TcpStream>)>,

    // All known peers in the network (except local one)
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub ping_period: Duration,
    pub hb_period: Duration,
    pub hb_timeout: Duration,
}

impl Peer {
    // Produces Peer with associated channel for shutting it down
    pub fn new_with_connection_update(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        conn: Connection<TcpStream>,
        addr: SocketAddr,
        id: Identity,
        self_info: Arc<connection::AuthInfo>,
        new_connections: mpsc::Receiver<(connection::AuthInfo, Connection<TcpStream>)>,
    ) -> Result<Self, Error> {
        let other_info = Arc::new(Mutex::new(Info::new(id, addr)));
        let peer = Peer {
            conn,
            config,
            last_active: Instant::now(),
            peer_info: other_info,
            self_info,
            new_connections,
            peers_info
        };
        Ok(peer)
    }

    // Handle communication with a particular peer
    pub async fn handle_peer(&mut self) -> Result<(), Error>{
        use tokio::time::interval;

        let mut ping_interval = interval(self.config.ping_period);
        let mut hb_send_interval = interval(self.config.hb_period);
        let mut hb_recv_interval = interval(self.config.hb_timeout);

        loop {
            loop {
                let recv = self.conn.recv_message();
                let res = tokio::select! {
                    recv_res = recv => {
                        match recv_res {
                            Ok(m) => self.handle_message(m).await,
                            Err(e) => Err(Error::ConnectionError(e)),
                        }
                    }
                    _ = ping_interval.tick() => {
                        self.conn.send_message(Message::Ping).await
                            .map_err(Error::ConnectionError)
                    }
                    _ = hb_send_interval.tick() => {
                        self.conn.send_message(Message::Heartbeat).await
                            .map_err(Error::ConnectionError)
                    }
                    _ = hb_recv_interval.tick() => {
                        self.check_heartbeat().await
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
                        Error::UnexpectedMessage(_) => todo!(),
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
                let res = self.reconnect().await;
                match res {
                    Ok(_) => log::info!("Reconnected successfully, continuing operation"),
                    Err(Error::MutexPoisoned(_)) => { return res },
                    Err(e) => log::warn!("Error while reconnecting; trying again: {:?}", e),
                }
            }
        }
    }

    async fn reconnect(&mut self) -> Result<(), Error> {
        loop {
            let peer_listen_addr = self.get_info_copy()
                .map_err(Error::MutexPoisoned)?
                .last_address;

            let try_connect = TcpStream::connect(peer_listen_addr);
            let receive_connection = self.new_connections.recv();
            // We either reconnect by ourselves or receive new connection from the Network
            // (the peer connected through the listen port)
            let res = tokio::select! {
                connect_res = try_connect => {
                    match connect_res {
                        Ok(stream) => {
                            let conn = Connection::from_stream(stream);
                            match self.exchange_auth(conn).await {
                                Ok(()) => return Ok(()),
                                Err(AuthenticationError::WrongCredentials) => {
                                    log::info!("Peer's identity didn't match");
                                    continue;
                                },
                                Err(AuthenticationError::PeerError(e)) =>
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
                            if auth_info.identity != self.self_info.identity {
                                log::error!("Received connection with wrong identity from the
                                    network which shouldn't happen, ignoring");
                                continue;
                            }
                            self.conn = conn;
                            self.update_listen_addr(auth_info.listen_addr)
                                .map_err(Error::MutexPoisoned)?;
                            // Connection was already identified, everything is Ok(())
                            Ok(())
                        },
                        None => {
                            return Err(Error::ChannelClosed("new connections".to_string()))
                        },
                    }
                }
            };
            if let Err(e) = res {
                log::warn!("Error while reconnecting: {:?}", e);
                continue
            }
        }
    }

    async fn exchange_auth(&mut self, mut conn: Connection<TcpStream>) -> Result<(), AuthenticationError> {
        self.conn.send_message(Message::Authenticate((*self.self_info).clone())).await
            .map_err(Error::ConnectionError)
            .map_err(AuthenticationError::PeerError)?;
        let m = self.conn.recv_message().await
            .map_err(Error::ConnectionError)
            .map_err(AuthenticationError::PeerError)?;
        if let connection::Message::Authenticate(info) = m {
            if info.identity == self.get_info_copy()
                .map_err(Error::MutexPoisoned)
                .map_err(AuthenticationError::PeerError)?.id
            {
                self.update_listen_addr(info.listen_addr)
                    .map_err(Error::MutexPoisoned)
                    .map_err(AuthenticationError::PeerError)?;
                Ok(())
            }
            else {
                Err(AuthenticationError::WrongCredentials)
            }
        }
        else {
            conn.send_message(
                connection::Message::Error("Expected `AddMe` message as first one".to_string())
            ).await
                .map_err(Error::ConnectionError)
                .map_err(AuthenticationError::PeerError)?;
            Err(AuthenticationError::PeerError(Error::UnexpectedMessage(m)))
        }
    }

    async fn handle_message(&mut self, m: Message) -> Result<(), Error> {
        match m {
            Message::Ping => {
                let info = self.get_info_copy()
                    .map_err(Error::MutexPoisoned)?;
                println!("{}/{} - {}", info.id, info.last_address, m);
            },
            Message::Heartbeat => (), // We update timer on each activity after match
            Message::ListPeersRequest => {
                let map = self.list_peers()
                    .map_err(Error::MutexPoisoned)?
                    .into_iter().collect();
                self.conn.send_message(Message::ListPeersResponse(map)).await
                    .map_err(Error::ConnectionError)?;
            },
            Message::ListPeersResponse(map) => {
                let mut peers_info = self.peers_info.lock()
                    .map_err(|_| {Error::MutexPoisoned(MutexPoisoned{})})?;
                for (identity, addr) in map {
                    if !peers_info.contains_key(&identity) {
                        peers_info.insert( // TODO xyunya
                            identity, 
                            Arc::new(Mutex::new(Info::new(identity, addr)))
                        );
                    }
                }
            },
            Message::Authenticate(_) => return Err(Error::UnexpectedMessage(m.clone())), // unexpected?

            Message::Error(s) => log::error!("Peer sent error: {}", s),
        };
        self.last_active = Instant::now();
        Ok(())
    }

    async fn check_heartbeat(&mut self) -> Result<(), Error> {
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
                self.conn.send_message(Message::ListPeersRequest).await
                    .map_err(Error::ConnectionError)?;
                self.update_status(Status::Alive)
                    .map_err(Error::MutexPoisoned)?;
            }
        }
        Ok(())
    }

    fn list_peers(&mut self) -> Result<Vec<(Identity, SocketAddr)>, MutexPoisoned>{
        let peers_info = self.peers_info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        
        // Get pairs `(peer_id, address)` or return `MutexPoisoned` error if at least one
        // `Info` mutex was poisoned
        peers_info.iter()
            .map(|(s, i)| {
                i.lock()
                    .map(|i| {(s.to_owned(), i.last_address)})
                    .map_err(|_| {MutexPoisoned{}})
            })
            .collect::<Result<Vec<(Identity, SocketAddr)>, MutexPoisoned>>()
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

