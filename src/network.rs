//! Gossip network
//!
//! Defines entry points for starting a network node, manages handlers for each peer in the
//! network, handles incoming connections.
//!
//! # Quick start
//! ```ignore
//! let net = network::Network::new(
//!     private_key, cert, listen_addr, config
//! );
//! net.start_listen()
//! ```
//! To start the network node first create `Network` with appropriate configuration, then
//! use either [`Network::start_listen()`] or [`Network::start_listen()`].
//!
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use rustls::{Certificate, PrivateKey};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::TlsStream;

use crate::{
    auth::Identity,
    connection::{self, Connection},
    peer::{self, Config, Info, Peer},
    utils::MutexPoisoned,
};

struct ConnectionNotifier {
    new_connections_sender: mpsc::Sender<(Arc<Identity>, Connection<TlsStream<TcpStream>>)>,
}

impl ConnectionNotifier {
    /// Create peer with associated notifier
    /// must insert info about this peer in `peers_info`
    fn peer_notifier(
        peers_info: Arc<Mutex<PeerInfoMap>>,
        config: Config,
        peer_id: Arc<Identity>,
        self_listen_port: u16,
        self_id: Arc<Identity>,
        new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,
    ) -> Result<(Peer, Self), peer::CreationError> {
        // TODO config the number
        let (new_connections_sender, new_connections) = mpsc::channel(32);
        let peer = Peer::new(
            peers_info,
            config,
            peer_id,
            self_listen_port,
            self_id,
            new_connections,
            new_auth_addr,
        )?;
        Ok((
            peer,
            ConnectionNotifier {
                new_connections_sender,
            },
        ))
    }

    async fn notify_new_connection(
        &mut self,
        id: Arc<Identity>,
        conn: Connection<TlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        self.new_connections_sender
            .send((id, conn))
            .await
            .map_err(Error::MpscSendAddr)
    }
}

pub type PeerInfoMap = HashMap<Arc<Identity>, Arc<Mutex<Info>>>;

pub struct Network {
    peer_config: Config,

    self_private_key: PrivateKey,
    self_cert: Certificate,

    self_id: Arc<Identity>,
    listen_bind: SocketAddr,

    // Should be consistent with notifiers
    peers_info: Arc<Mutex<PeerInfoMap>>,

    // If a connection for some peer was established, it is sent through the corresponding notifier
    notifiers: HashMap<Arc<Identity>, ConnectionNotifier>,

    new_auth_addr_receiver: mpsc::Receiver<(Arc<Identity>, SocketAddr)>,
    new_auth_addr_sender: mpsc::Sender<(Arc<Identity>, SocketAddr)>,

    // New Peers handlers scheduled for run
    new_peers_receiver: mpsc::Receiver<(Peer, Option<Connection<TlsStream<TcpStream>>>)>,
    new_peers_sender: mpsc::Sender<(Peer, Option<Connection<TlsStream<TcpStream>>>)>,
}

#[derive(Debug)]
pub enum Error {
    Peer(peer::Error),
    PeerCreation(peer::CreationError),
    MpscSendAddr(mpsc::error::SendError<(Arc<Identity>, Connection<TlsStream<TcpStream>>)>),
    MpscSendPeer(mpsc::error::SendError<(Peer, Option<Connection<TlsStream<TcpStream>>>)>),
}

impl Network {
    pub fn new(
        self_private_key: PrivateKey,
        self_cert: Certificate,
        listen_addr: SocketAddr,
        peer_config: Config,
    ) -> Self {
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        // Ensure immutability and avoid copying if it grows later
        let self_id = Identity::new(&self_cert.0[..]);
        let notifiers = HashMap::new();
        // TODO config the numbers
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(64);
        let (new_auth_sender, new_auth_receiver) = mpsc::channel(64);
        Network {
            peers_info,
            self_private_key,
            self_cert,
            self_id,
            listen_bind: listen_addr,
            peer_config,
            notifiers,
            new_auth_addr_sender: new_auth_sender,
            new_auth_addr_receiver: new_auth_receiver,
            new_peers_receiver,
            new_peers_sender,
        }
    }

    pub async fn start_listen(mut self) -> std::io::Result<()> {
        tracing::debug!("Starting to listen on addr {:?}", self.listen_bind);
        let listener = TcpListener::bind(self.listen_bind).await?;
        loop {
            tokio::select! {
                listen_res = listener.accept() => {
                    let stream = match listen_res {
                        Ok((stream, _addr)) => {
                            stream
                        },
                        Err(e) => {
                            tracing::warn!("Couldn't accept incoming connection: {}", e);
                            continue;
                        },
                    };
                    tracing::debug!("New incoming connection from {:?}", stream.peer_addr());
                    let handle_new_con_res = self.handle_new_con(
                        stream, self.peer_config.clone(), true
                    ).await;
                    match handle_new_con_res {
                        Err(Error::Peer(peer::Error::Connection(_) | peer::Error::Tls(_) |
                        peer::Error::UnexpectedMessage(_))) => tracing::warn!("{:?}", handle_new_con_res),
                        Err(Error::Peer(peer::Error::MutexPoisoned(_))) => {
                            tracing::error!("Some mutex was poisoned, unable to continue");
                            return Ok(())
                        },
                        Err(Error::Peer(peer::Error::ChannelClosed(s))) => {
                            tracing::error!("Channel {} was closed,
                                peer can't be handled without it", s);
                            return Ok(())
                        }
                        Err(_) => tracing::warn!("{:?}", handle_new_con_res),
                        Ok(_) => (),
                    }
                }
                auth_opt = self.new_auth_addr_receiver.recv() => {
                    match auth_opt {
                        Some((peer_id, peer_listen_addr)) => {
                            if let Err(e) = self.add_to_network(
                                peer_id, peer_listen_addr, self.peer_config.clone(), None
                            ).await {
                                tracing::warn!("Error adding peer by auth info: {:?}", e);
                            }
                        },
                        None => tracing::error!("New auth was closed unexpectedly, can't work properly"),
                    }
                }
                peer_opt = self.new_peers_receiver.recv() => {
                    match peer_opt {
                        Some((mut peer, conn)) => {
                            let self_private_key = self.self_private_key.clone();
                            let self_clone = self.self_cert.clone();
                            tokio::spawn(async move {
                                peer.handle_peer(
                                    conn,
                                    self_private_key,
                                    self_clone,
                                ).await
                            });
                        },
                        None => tracing::error!("New peers was closed unexpectedly, can't work properly"),
                    }
                }
            };
        }
    }

    pub async fn start_connect(mut self, connect_addr: SocketAddr) -> std::io::Result<()> {
        tracing::trace!("Connecting to {}...", connect_addr);
        let init_connection = TcpStream::connect(connect_addr).await?;
        tracing::trace!(
            "Established TCP connection with {:?}",
            init_connection.peer_addr()
        );
        if let Err(e) = self
            .handle_new_con(init_connection, self.peer_config.clone(), false)
            .await
        {
            tracing::error!("Could not join network through {}: {:?}", connect_addr, e);
        };
        self.start_listen().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(peer_addr=?stream.peer_addr().map(|a| a.to_string())))]
    async fn handle_new_con(
        &mut self,
        stream: TcpStream,
        peer_config: Config,
        incoming: bool,
    ) -> Result<(), Error> {
        let peer_con_addr = stream
            .peer_addr()
            .map_err(connection::Error::IO)
            .map_err(peer::Error::Connection)
            .map_err(Error::Peer)?;
        let (peer_id, peer_listen_port, conn) = match incoming {
            true => peer::Peer::auth_server(
                self.self_id.clone(),
                self.listen_bind.port(),
                stream,
                self.self_private_key.clone(),
                self.self_cert.clone(),
            )
            .await
            .map_err(Error::Peer)?,
            false => peer::Peer::auth_new_client(
                self.self_id.clone(),
                self.listen_bind.port(),
                stream,
                self.self_private_key.clone(),
                self.self_cert.clone(),
            )
            .await
            .map_err(Error::Peer)?,
        };
        let peer_listen_addr = SocketAddr::new(peer_con_addr.ip(), peer_listen_port);
        self.add_to_network(peer_id, peer_listen_addr, peer_config, Some(conn))
            .await?;
        Ok(())
    }

    /// Based on the identity either send Peer the connection or add a new peer handler
    /// peer_addr - address at which peer's listener's at
    #[tracing::instrument(skip(self, peer_listen_addr, peer_config, conn_opt), fields(peer_listen_addr=%peer_listen_addr))]
    async fn add_to_network(
        &mut self,
        peer_id: Arc<Identity>,
        peer_listen_addr: SocketAddr,
        peer_config: Config,
        conn_opt: Option<Connection<TlsStream<TcpStream>>>,
    ) -> Result<(), Error> {
        tracing::debug!("Adding peer to network");
        if peer_id == self.self_id {
            tracing::debug!("Trying to add myself to the network, aborting");
            return Ok(());
        }
        if self.notifiers.contains_key(&peer_id) {
            tracing::debug!("Peer is already known");
            if let Some(conn) = conn_opt {
                match self.insert_info(peer_id.clone(), peer_listen_addr) {
                    Ok(Some(_)) => tracing::warn!(
                        "Inconsistency between `notifiers` and `peers_info`\
                        which probably happened from some previous error, this may lead to wrong \
                        behaviour."
                    ),
                    Ok(None) => (),
                    Err(e) => return Err(Error::Peer(peer::Error::MutexPoisoned(e))),
                }
                tracing::debug!("Sending new connection to handler");
                let notifier = self
                    .notifiers
                    .get_mut(&peer_id)
                    .expect("Key disappeared right after checking for contains_key, very strange");
                notifier.notify_new_connection(peer_id, conn).await?;
            } else {
                tracing::debug!("No connection was provieded, ignoring");
            }
        } else {
            tracing::debug!("Peer is unknown, creating a new handler");
            match self.insert_info(peer_id.clone(), peer_listen_addr) {
                Ok(Some(_)) => tracing::warn!(
                    "Inconsistency between `notifiers` and `peers_info`\
                    which probably happened from some previous error, this may lead to wrong \
                    behaviour."
                ),
                Ok(None) => tracing::debug!("Successfully added new peer's info"),
                Err(e) => return Err(Error::Peer(peer::Error::MutexPoisoned(e))),
            }
            let (peer, notifier) = ConnectionNotifier::peer_notifier(
                self.peers_info.clone(),
                peer_config,
                peer_id.clone(),
                self.listen_bind.port(),
                self.self_id.clone(),
                self.new_auth_addr_sender.clone(),
            )
            .map_err(Error::PeerCreation)?;
            self.notifiers.insert(peer_id, notifier);
            tracing::debug!("Scheduling the handler for execution");
            self.new_peers_sender
                .send((peer, conn_opt))
                .await
                .map_err(Error::MpscSendPeer)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, peer_listen_addr), fields(peer_listen_addr=%peer_listen_addr))]
    fn insert_info(
        &mut self,
        peer_identity: Arc<Identity>,
        peer_listen_addr: SocketAddr,
    ) -> Result<Option<Arc<Mutex<Info>>>, MutexPoisoned> {
        tracing::debug!("Updating listen address");
        let mut info_map = self.peers_info.lock().map_err(|_| MutexPoisoned {})?;
        let new_info = Arc::new(Mutex::new(Info::new(peer_listen_addr)));
        tracing::debug!("Updated successfully");
        Ok(info_map.insert(peer_identity, new_info))
    }
}
