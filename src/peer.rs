use std::{net::SocketAddr, collections::HashMap, sync::{Arc, Mutex}};
use tokio::{time::{Duration, Instant}, sync::mpsc, io::{AsyncRead, AsyncWrite}};

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
}

#[derive(Debug)]
pub struct MutexPoisoned {}

pub type Shared<T> = Arc<Mutex<T>>;

// TODO add timeouts where applicable
#[derive(Debug)]
pub struct Peer<T>
where
    T: AsyncRead + AsyncWrite + Sized + std::marker::Unpin
{
    conn: Connection<T>,
    config: Config,

    // For receiving and checking
    last_active: Instant,

    // Info about handled peer, shared
    info: Shared<Info>,

    // Info about local peer
    self_info: Arc<connection::LocalInfo>,

    // In case handled Peer (by identity) was found on different address, the address to be sent
    // here to try to reconnect there
    new_addresses: mpsc::Receiver<SocketAddr>,

    // All known peers in the network (except local one)
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub ping_period: Duration,
    pub hb_period: Duration,
    pub hb_timeout: Duration,
}

impl<T> Peer<T>
where
    T: AsyncRead + AsyncWrite + Sized  + std::marker::Unpin
{
    pub fn new(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        conn: Connection<T>,
        addr: SocketAddr,
        id: Identity,
        self_info: Arc<connection::LocalInfo>,
    ) -> Result<Self, Error> {
        let (_, stub) = mpsc::channel(1);
        Self::new_with_address_update(peers_info, config, conn, addr, id, self_info, stub)
    }

    // Produces Peer with associated channel for shutting it down
    pub fn new_with_address_update(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        conn: Connection<T>,
        addr: SocketAddr,
        id: Identity,
        self_info: Arc<connection::LocalInfo>,
        new_addresses: mpsc::Receiver<SocketAddr>,
    ) -> Result<Self, Error> {
        let other_info = Arc::new(Mutex::new(Info::new(id, addr)));
        let peer = Peer {
            conn,
            config,
            last_active: Instant::now(),
            info: other_info,
            self_info,
            new_addresses,
            peers_info
        };
        Ok(peer)
    }

    // Handle communication with a particular peer
    pub async fn handle_peer(&mut self) -> Result<(), Error>{
        use tokio::time::interval;
        // First we need to introduce ourselves
        self.conn.send_message(
            Message::Authenticate((*self.self_info).clone())).await
                .map_err(Error::ConnectionError)?;

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
                                log::error!("Connection closed"); // TODO: try to reconnect
                                return Err(e);
                            },
                        }
                    },
                    Error::UnexpectedMessage(_) => todo!(),
                    Error::MutexPoisoned(_) => {
                        log::error!("Some mutex was poisoned, can't function without it");
                        return Err(e);
                    },
                }
            }
        }
    }

    async fn reconnect(&self) -> Result<(), Error> {
        
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
        let info = self.info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        Ok(info.clone())
    }

    // same, lock isolation to avoid deadlock
    fn update_status(&self, new_status: Status) -> Result<(), MutexPoisoned> {
        let mut info = self.info.lock()
            .map_err(|_| {MutexPoisoned{}})?;
        info.status = new_status;
        Ok(())
    }
}

