//! Gossip network
//! 
//! Defines entry points for starting a network node, manages handlers for each peer in the
//! network, handles incoming connections.
//! 
//! # Quick start
//! To start the network node first create `Network` with appropriate configuration:
//! ```ignore
//! let net = network::Network::new(identity, listen_addr, config);
//! ```
//! 
use std::{collections::HashMap, sync::{Arc, Mutex}, net::SocketAddr};

use tokio::{sync::mpsc, net::{TcpListener, TcpStream}};
use tokio_rustls::{ TlsConnector, rustls::{ClientConfig, ServerConfig} };

use crate::{
    peer::{self, Config, Info, Peer, Shared},
    connection::{self, Connection}, authentication::Identity
};

struct ConnectionNotifier {
    new_connections_sender: mpsc::Sender<(Arc<Identity>, Connection<TcpStream>)>,
}

impl ConnectionNotifier {
    /// Create peer with associated notifier
    /// must insert info about this peer in `peers_info`
    pub fn peer_notifier(
        peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,
        config: Config,
        peer_id: Arc<Identity>,
        self_listen_port: u16,
        self_id: Arc<Identity>,
        new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,
    ) -> Result<(Peer, Self), peer::CreationError> {
        // TODO config the number
        let (new_connections_sender, new_connections) = mpsc::channel(32);
        let peer = Peer::new(
            peers_info, config, peer_id, self_listen_port, self_id, new_connections, new_auth_addr
        )?;
        Ok((peer, ConnectionNotifier{new_connections_sender}))
    }

    pub async fn notify_new_connection(
        &mut self, id: Arc<Identity>, conn: Connection<TcpStream>,
    ) -> Result<(), Error> {
        self.new_connections_sender.send((id, conn)).await
            .map_err(Error::MpscSendAddrError)
    }
}

pub struct Network {
    peer_config: Config,
    
    self_id: Arc<Identity>,
    listen_bind: SocketAddr,

    // Should be consistent with notifiers
    peers_info: Shared<HashMap<Arc<Identity>, Shared<Info>>>,

    // If a connection for some peer was established, it is sent through the corresponding notifier
    notifiers: HashMap<Arc<Identity>, ConnectionNotifier>,

    new_auth_addr_receiver: mpsc::Receiver<(Arc<Identity>, SocketAddr)>,
    new_auth_addr_sender: mpsc::Sender<(Arc<Identity>, SocketAddr)>,

    // New Peers handlers scheduled for run
    new_peers_receiver: mpsc::Receiver<(Peer, Option<Connection<TcpStream>>)>,
    new_peers_sender: mpsc::Sender<(Peer, Option<Connection<TcpStream>>)>,
}

#[derive(Debug)]
pub enum Error {
    PeerError(peer::Error),
    PeerCreationError(peer::CreationError),
    MpscSendAddrError(mpsc::error::SendError<(Arc<Identity>, Connection<TcpStream>)>),
    MpscSendPeerError(mpsc::error::SendError<(Peer, Option<Connection<TcpStream>>)>)
}

impl Network {
    pub fn new(identity: Identity, listen_addr: SocketAddr, peer_config: Config) -> Self {
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        // Ensure immutability and avoid copying if it grows later
        let self_id = Arc::new(identity);
        let notifiers = HashMap::new();
        // TODO config the numbers
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(64);
        let (new_auth_sender, new_auth_receiver) = mpsc::channel(64);
        Network{
            peers_info, self_id, listen_bind: listen_addr, peer_config, notifiers, 
            new_auth_addr_sender: new_auth_sender, new_auth_addr_receiver: new_auth_receiver, new_peers_receiver, new_peers_sender,
        }
    }

    fn config_tls() {
        let mut config = ServerConfig::builder()
            .with_safe_defaults()
            .with_custom;
        let connector = TlsConnector::from(Arc::new(config));
    }

    pub async fn start_listen(
        mut self,  
    ) -> std::io::Result<()> {
        tracing::debug!("Starting to listen on addr {:?}", self.listen_bind);
        let listener = TcpListener::bind(self.listen_bind).await?;
        loop {
            let new_peer = self.new_peers_receiver.recv();
            let new_auth = self.new_auth_addr_receiver.recv();
            tokio::select! {
                listen_res = listener.accept() => {
                    match listen_res {
                        Ok((stream, _addr)) => {
                            tracing::debug!("New incoming connection from {:?}", stream.peer_addr());
                            if let Err(Error::PeerError(e)) = self.handle_new_con(
                                stream, self.peer_config.clone()
                            ).await {
                                match e {
                                    peer::Error::ConnectionError(_) |
                                    peer::Error::UnexpectedMessage(_) => tracing::warn!("{:?}", e),
                                    peer::Error::MutexPoisoned(_) => {
                                        tracing::error!("Some mutex was poisoned, unable to continue");
                                        return Ok(())
                                    },
                                    peer::Error::ChannelClosed(s) => {
                                        tracing::error!("Channel {} was closed,
                                            peer can't be handled without it", s);
                                        return Ok(())
                                    }
                                }
                            };
                        },
                        Err(e) => tracing::warn!("Couldn't accept incoming connection: {}", e),
                    };
                }
                auth_opt = new_auth => {
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
                peer_opt = new_peer => {
                    match peer_opt {
                        Some((mut peer, conn)) => {
                            tokio::spawn(async move {peer.handle_peer(conn).await});
                        },
                        None => tracing::error!("New peers was closed unexpectedly, can't work properly"),
                    }
                }
            };
        }
    }

    pub async fn start_connect(
        mut self, connect_addr: SocketAddr
    ) -> std::io::Result<()> {
        tracing::trace!("Connecting to {}...", connect_addr);
        let init_connection = TcpStream::connect(connect_addr).await?;
        tracing::trace!("Established TCP connection with {:?}", init_connection.peer_addr());
        if let Err(e) = self.handle_new_con(
            init_connection, self.peer_config.clone()
        ).await {
            tracing::error!("Could not join network through {}: {:?}", connect_addr, e);
        };
        self.start_listen().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(peer_addr=?stream.peer_addr().map(|a| a.to_string())))]
    async fn handle_new_con(
        &mut self, stream: TcpStream, peer_config: Config
    ) -> Result<(), Error> {
        let peer_con_addr = stream.peer_addr()
            .map_err(connection::Error::IOError)
            .map_err(peer::Error::ConnectionError)
            .map_err(Error::PeerError)?;
        let mut conn = Connection::from_stream(stream);
        let (peer_id, peer_listen_port) = peer::Peer::exchange_pair(
            self.self_id.clone(), self.listen_bind.port(), &mut conn
        ).await
            .map_err(Error::PeerError)?;
        let peer_listen_addr = SocketAddr::new(peer_con_addr.ip(), peer_listen_port);
        self.add_to_network(peer_id, peer_listen_addr, peer_config, Some(conn)).await?;
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
        conn_opt: Option<Connection<TcpStream>>
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
                    Ok(Some(_)) => tracing::warn!("Inconsistency between `notifiers` and `peers_info`\
                        which probably happened from some previous error, this may lead to wrong \
                        behaviour."),
                    Ok(None) => (),
                    Err(e) =>
                        return Err(Error::PeerError(peer::Error::MutexPoisoned(e))),
                }
                tracing::debug!("Sending new connection to handler");
                let notifier = self.notifiers.get_mut(&peer_id).expect(
                    "Key disappeared right after checking for contains_key, very strange"
                );
                notifier.notify_new_connection(peer_id, conn).await?;
            }
            else {
                tracing::debug!("No connection was provieded, ignoring");
            }
        }
        else {
            tracing::debug!("Peer is unknown, creating a new handler");
            match self.insert_info(peer_id.clone(), peer_listen_addr) {
                Ok(Some(_)) => tracing::warn!("Inconsistency between `notifiers` and `peers_info`\
                    which probably happened from some previous error, this may lead to wrong \
                    behaviour."),
                Ok(None) => tracing::debug!("Successfully added new peer's info"),
                Err(e) =>
                    return Err(Error::PeerError(peer::Error::MutexPoisoned(e))),
            }
            let (peer, notifier) = ConnectionNotifier::peer_notifier(
                self.peers_info.clone(),
                peer_config,
                peer_id.clone(),
                self.listen_bind.port(),
                self.self_id.clone(),
                self.new_auth_addr_sender.clone()
            ).map_err(Error::PeerCreationError)?;
            self.notifiers.insert(
                peer_id,
                notifier
            );
            tracing::debug!("Scheduling the handler for execution");
            self.new_peers_sender.send((peer, conn_opt)).await
                .map_err(Error::MpscSendPeerError)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, peer_listen_addr), fields(peer_listen_addr=%peer_listen_addr))]
    fn insert_info(
        &mut self,
        peer_identity: Arc<Identity>,
        peer_listen_addr: SocketAddr,
    ) -> Result<Option<Shared<Info>>, peer::MutexPoisoned> {
        tracing::debug!("Updating listen address");
        let mut info_map = self.peers_info.lock()
            .map_err(|_| {peer::MutexPoisoned{}})?;
        let new_info = Arc::new(Mutex::new(Info::new(peer_listen_addr)));
        tracing::debug!("Updated successfully");
        Ok(info_map.insert(peer_identity, new_info))
    }
}