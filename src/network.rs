use std::{collections::HashMap, sync::{Arc, Mutex}, net::SocketAddr};

use tokio::{sync::mpsc, net::{TcpListener, TcpStream}};

use crate::{ peer, connection };
use connection::Connection;
use peer::{ Peer, Shared };

// TODO rename
#[derive(Clone)]
struct PeerWrapper {
    peer: Shared<Peer>,
}

struct WrapperPoisoned { }

enum WrapperTryLockError {
    WouldBlock,
    Poisoned(WrapperPoisoned),
}

impl PeerWrapper {
    pub fn new(peer: Peer) -> Self {
        PeerWrapper { peer: Arc::new(Mutex::new(peer)) }
    }

    pub fn is_running(&self) -> Result<bool, WrapperPoisoned> {
        match self.peer.try_lock() {
            Ok(a) => Ok(false),
            Err(e) => match e {
                std::sync::TryLockError::Poisoned(_) => Err(WrapperPoisoned{}),
                std::sync::TryLockError::WouldBlock => Ok(true),
            },
        }
    }

    pub async fn run(&mut self) -> Result<(), WrapperPoisoned> {
        match self.peer.lock() {
            Ok(mut p) => Ok(p.handle_peer().await),
            Err(e) => Err(WrapperPoisoned{}),
        }
    }

    pub async fn try_run(&mut self) -> Result<(), WrapperTryLockError> {
        match self.peer.try_lock() {
            Ok(mut p) => Ok(p.handle_peer().await),
            Err(e) => match e {
                std::sync::TryLockError::Poisoned(_) => Err(
                    WrapperTryLockError::Poisoned(WrapperPoisoned{})
                ),
                std::sync::TryLockError::WouldBlock => Err(WrapperTryLockError::WouldBlock),
            },
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), WrapperPoisoned> {
        todo!()
    }
}

struct Network {
    // These 2 Hash maps should be consistent
    peers: HashMap<peer::Identity, Peer>,
    peers_info: Shared<HashMap<peer::Identity, Shared<peer::Info>>>,
    new_peers_receiver: mpsc::Receiver<peer::Info>,
    new_peers_sender: mpsc::Sender<peer::Info>,
}

#[derive(Debug)]
pub enum Error {
    PeerError(peer::Error),
}

impl Network {
    async fn new() -> Self {
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        let peers = HashMap::new();
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(64);
        Network{peers_info, peers, new_peers_receiver, new_peers_sender}
    }

    async fn start(
        self, peer_config: peer::Config, listen_addr: SocketAddr
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(listen_addr).await?;
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    self.manage_new_con(stream, addr, peer_config)
                },
                Err(e) => {
                    log::warn!("Couldn't accept incoming connection: {}", e);
                    continue;
                },
            };
            // let res = match new_con.await {
            //     Ok(sock) => {
                    
            //     },
            //     Err(e) => {
            //         Err(e)
            //             .map_err(connection::Error::IOError)
            //             .map_err(peer::Error::ConnectionError)
            //             .map_err(Error::PeerError)
            //     },
            // };
            match res {
                Ok(peer) => {
                    todo!()
                },
                Err(Error::PeerError(e)) => {
                    match e {
                        peer::Error::ConnectionError(_) | peer::Error::UnexpectedMessage(_) => {
                            log::warn!("{:?}", e);
                        },
                        peer::Error::MutexPoisoned => {
                            log::error!("Some mutex was poisoned, unable to continue");
                            return Ok(())
                        },
                    }
                },
            };
        }
        Ok(())
    }

    async fn manage_new_con(
        &mut self, stream: TcpStream, addr: SocketAddr, peer_config: peer::Config
    ) -> Result<(), Error> {
        let conn = Connection::from_stream(stream);
        let peer = self.initiate_peer(conn, peer_config).await?;
        let aboba = self.add_to_network(peer).await;
        Ok(())
    }

    // Based on the identity either replace some Peer or add new one
    async fn add_to_network(&mut self, peer: Peer) -> Result<(), Error> {
        let info_mutex = peer.info();
        let peer_info = info_mutex.lock()
            .map_err(|_| {peer::Error::MutexPoisoned})
            .map_err(Error::PeerError)?;
        let peer_id = peer_info.id;
        match self.peers.get(&peer_id) {
            Some(old_handler) => {
                // Stop assigned Peer and add new one instead
                old_handler.
            },
            None => todo!(),
        }
        Ok(())
    }

    // Get identity and create Peer
    async fn initiate_peer(
        &self, mut conn: Connection, peer_config: peer::Config
    ) -> Result<Peer, Error> {
        // First message in the connection should be AddMe with info
        let m = conn.recv_message().await
            .map_err(peer::Error::ConnectionError)
            .map_err(Error::PeerError)?;
        if let connection::Message::AddMe(info) = m {
            Peer::new(
                self.peers_info.clone(),
                peer_config,
                conn,
                info.listen_addr,
                info.identity,
            ).await
                .map_err(Error::PeerError)
        }
        else {
            conn.send_message(
                connection::Message::Error("Expected `AddMe` message as first one".to_string())
            ).await
                .map_err(peer::Error::ConnectionError)
                .map_err(Error::PeerError)?;
            Err(Error::PeerError(peer::Error::UnexpectedMessage(m)))
        }
    }
}