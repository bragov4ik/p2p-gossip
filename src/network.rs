//! Gossip network
//!
//! Defines entry points for starting a network node, manages handlers for each peer in the
//! network, handles incoming connections.
//! 
//! To be more precise, its responsibilites include
//! * Directing connections to existing handlers
//! * Creating handlers if needed
//! * Communication with not authenticated handlers (i.e. without defined [`Identity`])
//! 
//! If there is a confirmed identity, communication is delegated to [`Peer`].
//!
//! To start the network node first create [`Network`] with appropriate configuration, then
//! use either [`Network::start_listen()`] or [`Network::start_listen()`].
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

/// Convenient peer notifier about new established connections to it
struct ConnectionNotifier {
    new_connections_sender: mpsc::Sender<Connection<TlsStream<TcpStream>>>,
}

impl ConnectionNotifier {
    /// Create peer handler with associated notifier.
    /// Caller must insert info about this peer in `peers_info` beforehand.
    fn peer_notifier(
        peers_info: Arc<Mutex<PeerInfoMap>>,
        config: Config,
        peer_id: Arc<Identity>,
        self_listen_port: u16,
        self_id: Arc<Identity>,
        new_auth_addr: mpsc::Sender<(Arc<Identity>, SocketAddr)>,
    ) -> Result<(Peer, Self), peer::CreationError> {
        // 32 should be enough, maybe move to some config file later
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

    /// Send notification to the peer handler about a new connection
    async fn notify_new_connection(
        &mut self,
        conn: Connection<TlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        self.new_connections_sender
            .send(conn)
            .await
            .map_err(Error::MpscSendAddr)
    }
}

pub type PeerInfoMap = HashMap<Arc<Identity>, Arc<Mutex<Info>>>;

pub struct Network {
    // Behaviour of peer handler
    peer_config: Config,
    
    self_private_key: PrivateKey,
    self_cert: Certificate,

    self_id: Arc<Identity>,

    listen_bind: SocketAddr,

    // All known peers' information. Should be consistent with `notifiers`
    peers_info: Arc<Mutex<PeerInfoMap>>,

    // Notifiers for sending connections to existing handlers
    notifiers: HashMap<Arc<Identity>, ConnectionNotifier>,

    // Handles for obtaining info of peers discovered from others' peer lists
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
    MpscSendAddr(mpsc::error::SendError<Connection<TlsStream<TcpStream>>>),
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
        // Ensure immutability and avoid copying if it is changed later
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

    /// Start the node without initially connecting to anyone.
    /// 
    /// Waits for incoming connection on specified port.
    /// If new peer connects, adds it to list of known peers and creates
    /// a handle for it.
    /// If existing peer connects, forwards the connection to the existing
    /// handle.
    pub async fn start_listen(mut self) -> std::io::Result<()> {
        tracing::debug!("Starting to listen on addr {:?}", self.listen_bind);
        let listener = TcpListener::bind(self.listen_bind).await?;
        loop {
            tokio::select! {
                listen_res = listener.accept() => {
                    // New connection from listener
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
                        peer::Error::UnexpectedMessage(_))) => tracing::debug!("{:?}", handle_new_con_res),
                        Err(Error::Peer(peer::Error::MutexPoisoned(_))) => {
                            tracing::error!("Some mutex was poisoned, unable to continue");
                            return Ok(())
                        },
                        Err(Error::Peer(peer::Error::ChannelClosed(s))) => {
                            tracing::error!("Channel {} was closed,
                                peer can't be handled without it", s);
                            return Ok(())
                        }
                        Err(_) => tracing::debug!("{:?}", handle_new_con_res),
                        Ok(_) => (),
                    }
                }
                auth_opt = self.new_auth_addr_receiver.recv() => {
                    // Discovered new peers from other's known peers lists
                    match auth_opt {
                        Some((peer_id, peer_listen_addr)) => {
                            if let Err(e) = self.add_to_network(
                                peer_id, peer_listen_addr, self.peer_config.clone(), None
                            ).await {
                                tracing::debug!("Error adding peer by auth info: {:?}", e);
                            }
                        },
                        None => tracing::error!("New auth was closed unexpectedly, can't work properly"),
                    }
                }
                peer_opt = self.new_peers_receiver.recv() => {
                    // Peer handle is scheduled for run
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

    /// Start the node with joining a network through peer at `connect_addr`.
    /// 
    /// After joining operates normally as in `start_listen`
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

    /// Handles new connection.
    /// 
    /// Authenticates the peer and adds it to the network on success.
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
        // We distinguish between incoming/outcoming since rustls has a concept of
        // server and client. Initiator of the connection is considered as client
        // and acceptor is a server.
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

    /// Add the peer to network
    /// 
    /// Has two use cases:
    /// * *Known peer* Sending new connection to existing peer handler
    /// * *New peer* Create new handler with connection (if provided) and schedule it for execution
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
                match Self::insert_info(self.peers_info.clone(), peer_id.clone(), peer_listen_addr) {
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
                notifier.notify_new_connection(conn).await?;
            } else {
                tracing::debug!("No connection was provided, ignoring");
            }
        } else {
            tracing::debug!("Peer is unknown, creating a new handler");
            match Self::insert_info(self.peers_info.clone(), peer_id.clone(), peer_listen_addr) {
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

    /// Put information about given peer into `peers_info` storage and
    /// return previous value
    #[tracing::instrument(skip(peers_info, peer_listen_addr), fields(peer_listen_addr=%peer_listen_addr))]
    fn insert_info(
        peers_info: Arc<Mutex<PeerInfoMap>>,
        peer_identity: Arc<Identity>,
        peer_listen_addr: SocketAddr,
    ) -> Result<Option<Arc<Mutex<Info>>>, MutexPoisoned> {
        tracing::debug!("Updating listen address");
        let mut info_map = peers_info.lock().map_err(|_| MutexPoisoned {})?;
        let new_info = Arc::new(Mutex::new(Info::new(peer_listen_addr)));
        tracing::debug!("Updated successfully");
        Ok(info_map.insert(peer_identity, new_info))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tracing::Level;

    use crate::utils::{gen_cert_private_key, init_debugging};

    use super::*;

    #[test]
    fn test_insert_info() {
        init_debugging(Level::ERROR);
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        let id = Identity::new(b"1");
        let addr = "1.2.3.4:5".parse().unwrap();
        let a = Network::insert_info(
            peers_info.clone(), id.clone(), addr
        ).unwrap();
        assert!(a.is_none());
        let map = peers_info.lock().unwrap();
        let info_got = map.get(&id).unwrap();
        assert!(info_got.lock().unwrap().last_address == addr);
    }

    #[tokio::test]
    async fn test_network_runs() {
        init_debugging(Level::ERROR);
        let (cert, key) = gen_cert_private_key();
        let listen_addr = "127.0.0.1:0".parse().unwrap();
        let config = peer::Config {
            ping_period: Duration::from_secs(5),
            hb_period: Duration::from_secs(1),
            hb_timeout: Duration::from_secs(3),
        };
        let net = Network::new(key, cert, listen_addr, config);

        assert!(tokio::time::timeout(
            Duration::from_secs(3),
            net.start_listen(),
        )
        .await
        .is_err());
    }
}