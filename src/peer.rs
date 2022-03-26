use std::{net::SocketAddr, fmt::Display, collections::HashMap, sync::{Arc, Mutex}};
use serde::{Serialize, Deserialize};
use tokio::{time::{Duration, Instant}, sync::{mpsc, watch}};

use crate::connection;
use connection::{ Connection, Message };

pub type Identity = u64;

#[derive(Debug, Clone)]
pub struct Info {
    pub id: Identity,
    pub last_activity: Instant,
    pub status: Status,
    pub last_address: SocketAddr,
}

impl Info {
    fn new(id: Identity, last_address: SocketAddr) -> Self {
        // TODO see maybe other values
        Info { id, last_activity: Instant::now(), status: Status::Alive, last_address }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Status {
    Alive,
    Dead,
}

#[derive(Debug)]
pub enum Error {
    ConnectionError(connection::Error),
    UnexpectedMessage(Message),
    MutexPoisoned // gg
}

pub type Shared<T> = Arc<Mutex<T>>;

// TODO add timeouts where applicable
pub struct Peer {
    conn: Connection,
    config: Config,

    // For receiving and checking
    heartbeat_last_received: Instant,

    // Info about this peer, shared
    info: Shared<Info>,

    // All known peers in the network
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
}

#[derive(Clone)]
pub struct Config {
    ping_period: Duration,
    hb_period: Duration,
    hb_timeout: Duration,
}

impl Peer {

    // Produces Peer and channel for shutting it down
    pub async fn new(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        conn: Connection,
        addr: SocketAddr,
        id: Identity,
    ) -> Result<(Self, mpsc::Sender<()>), Error> {
        let other_info = Arc::new(Mutex::new(Info::new(id, addr)));
        let (shutdown_sender, shutdown_receiver) = mpsc::channel::<()>(1);
        let peer = Peer {
            conn,
            config,
            heartbeat_last_received: Instant::now(),
            info: other_info,
            shutdown_receiver,
            peers_info
        };
        Ok((peer, shutdown_sender))
    }

    // Handle communication with a particular peer
    pub async fn handle_peer(&mut self) {
        use tokio::time::interval;
        let mut ping_interval = interval(self.config.ping_period);
        let mut hb_send_interval = interval(self.config.hb_period);
        let mut hb_recv_interval = interval(self.config.hb_timeout);
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
                    self.ping().await
                }
                _ = hb_send_interval.tick() => {
                    self.send_heartbeat().await
                }
                _ = hb_recv_interval.tick() => {
                    self.check_heartbeat().await
                }
            };
            if let Err(e) = res {
                match e {
                    Error::ConnectionError(e) => {
                        match e {
                            connection::Error::SerializationError(e) => 
                                log::warn!("{}", e),
                            connection::Error::IOError(e) => log::warn!("{}", e),
                            connection::Error::StreamEnded => {
                                log::error!("Connection closed"); // TODO: try to reconnect
                                return;
                            },
                        }
                    },
                    Error::UnexpectedMessage(_) => todo!(),
                    Error::MutexPoisoned => {
                        log::error!("Some mutex was poisoned, can't function without it");
                        return;
                    },
                }
            }
        }
    }

    pub fn info(&self) -> Shared<Info> {
        self.info.clone()
    }

    // TODO: make some periodic msg class to unify these 2

    async fn ping(&mut self) -> Result<(), Error> {
        self.conn.send_message(Message::Ping).await
            .map_err(Error::ConnectionError)?;
        Ok(())
    }

    async fn send_heartbeat(&mut self) -> Result<(), Error> {
        self.conn.send_message(Message::Heartbeat).await
            .map_err(Error::ConnectionError)?;
        Ok(())
    }

    async fn check_heartbeat(&mut self) -> Result<(), Error> {
        let elapsed = self.heartbeat_last_received.elapsed();
        let mut info = self.info.lock()
            .map_err(|_| {Error::MutexPoisoned})?;
        if elapsed > self.config.hb_timeout {
            // Dead
            info.status = Status::Dead;
        }
        else {
            // Alive
            if info.status == Status::Dead {
                // Peer transitions from Dead to Alive state, so we ask it for new entries
                // in case the network was split
                self.conn.send_message(Message::ListPeersRequest).await
                    .map_err(Error::ConnectionError)?;
            }
        }
        Ok(())
    }

    fn list_peers(&mut self) -> Result<Vec<(Identity, SocketAddr)>, Error>{
        let peers_info = self.peers_info.lock()
            .map_err(|_| {Error::MutexPoisoned})?;
        
        // Get pairs `(peer_id, address)` or return `MutexPoisoned` error if at least one
        // `Info` mutex was poisoned
        peers_info.iter()
            .map(|(s, i)| {
                i.lock()
                    .map(|i| {(s.to_owned(), i.last_address)})
                    .map_err(|_| {Error::MutexPoisoned })
            })
            .collect::<Result<Vec<(Identity, SocketAddr)>, Error>>()
    }

    async fn handle_message(&mut self, m: Message) -> Result<(), Error> {
        match m {
            Message::Ping => {
                let mut info = self.info.lock()
                    .map_err(|_| {Error::MutexPoisoned})?;
                println!("{}/{} - {}", info.id, info.last_address, m);
            },
            Message::Heartbeat => {
                self.heartbeat_last_received = Instant::now()
            },
            Message::ListPeersRequest => {
                let map = self.list_peers()?.into_iter().collect();
                self.conn.send_message(Message::ListPeersResponse(map)).await
                    .map_err(Error::ConnectionError)?;
            },
            Message::ListPeersResponse(map) => {
                let mut peers_info = self.peers_info.lock()
                    .map_err(|_| {Error::MutexPoisoned})?;
                for (identity, addr) in map {
                    if !peers_info.contains_key(&identity) {
                        peers_info.insert(
                            identity, 
                            Arc::new(Mutex::new(Info::new(identity, addr)))
                        );
                    }
                }
            },
            Message::AddMe(_) => return Err(Error::UnexpectedMessage(m.clone())), // unexpected?

            Message::Error(s) => log::error!("Peer sent error: {}", s),
        };
        Ok(())
    }
}

