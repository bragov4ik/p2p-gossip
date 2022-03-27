use std::{collections::HashMap, sync::{Arc, Mutex}, net::SocketAddr};

use tokio::{sync::mpsc, net::{TcpListener, TcpStream}};

use crate::{
    peer::{self, Config, Identity, Info, Peer, Shared},
    connection::{self, AuthInfo, Connection}
};

struct ConnectionNotifier {
    new_connections_sender: mpsc::Sender<(AuthInfo, Connection<TcpStream>)>,
}

impl ConnectionNotifier {
    /// Create peer with associated notifier
    pub fn peer_notifier(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        addr: SocketAddr,
        id: Identity,
        self_info: connection::AuthInfo,
        new_auth: mpsc::Sender<AuthInfo>,
    ) -> Result<(Peer, Self), peer::Error> {
        // TODO config the number
        let (new_connections_sender, new_connections) = mpsc::channel(32);
        let id_arc = Arc::new(id);
        let peer = Peer::new(
            peers_info, config, addr, id_arc, self_info, new_connections, new_auth
        )?;
        Ok((peer, ConnectionNotifier{new_connections_sender}))
    }

    pub async fn notify_new_connection(
        &mut self, listen_addr: AuthInfo, conn: Connection<TcpStream>,
    ) -> Result<(), Error> {
        self.new_connections_sender.send((listen_addr, conn)).await
            .map_err(Error::MpscSendAddrError)
    }
}

pub struct Network {
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
    self_info: connection::AuthInfo,
    listen_bind: SocketAddr,
    peer_config: Config,

    // If a connection for some peer was established, it is sent through the corresponding notifier
    notifiers: HashMap<Identity, ConnectionNotifier>,

    new_auth_receiver: mpsc::Receiver<AuthInfo>,
    new_auth_sender: mpsc::Sender<AuthInfo>,

    // New Peers handlers scheduled for run
    new_peers_receiver: mpsc::Receiver<(Peer, Option<Connection<TcpStream>>)>,
    new_peers_sender: mpsc::Sender<(Peer, Option<Connection<TcpStream>>)>,
}

#[derive(Debug)]
pub enum Error {
    PeerError(peer::Error),
    MpscSendAddrError(mpsc::error::SendError<(AuthInfo, Connection<TcpStream>)>),
    MpscSendPeerError(mpsc::error::SendError<(Peer, Option<Connection<TcpStream>>)>)
}

impl Network {
    pub fn new(identity: Identity, listen_addr: SocketAddr, peer_config: Config) -> Self {
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        let self_info = connection::AuthInfo {identity, listen_addr,};
        let notifiers = HashMap::new();
        // TODO config the numbers
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(64);
        let (new_auth_sender, new_auth_receiver) = mpsc::channel(64);
        Network{
            peers_info, self_info, listen_bind: listen_addr, peer_config, notifiers, 
            new_auth_sender, new_auth_receiver, new_peers_receiver, new_peers_sender,
        }
    }

    pub async fn start(
        mut self,  
    ) -> std::io::Result<()> {
        log::debug!("Starting to listen on addr {:?}", self.listen_bind);
        let listener = TcpListener::bind(self.listen_bind).await?;
        self.self_info.listen_addr = listener.local_addr()?;
        log::debug!("Local address {:?}", self.self_info.listen_addr);
        loop {
            let new_peer = self.new_peers_receiver.recv();
            let new_auth = self.new_auth_receiver.recv();
            tokio::select! {
                listen_res = listener.accept() => {
                    match listen_res {
                        Ok((stream, _addr)) => {
                            log::debug!("New incoming connection from {:?}", stream.peer_addr());
                            if let Err(Error::PeerError(e)) = self.handle_new_con(
                                stream, self.peer_config.clone()
                            ).await {
                                match e {
                                    peer::Error::ConnectionError(_) |
                                    peer::Error::UnexpectedMessage(_) => log::warn!("{:?}", e),
                                    peer::Error::MutexPoisoned(_) => {
                                        log::error!("Some mutex was poisoned, unable to continue");
                                        return Ok(())
                                    },
                                    peer::Error::ChannelClosed(s) => {
                                        log::error!("Channel {} was closed,
                                            peer can't be handled without it", s);
                                        return Ok(())
                                    }
                                }
                            };
                        },
                        Err(e) => log::warn!("Couldn't accept incoming connection: {}", e),
                    };
                }
                auth_opt = new_auth => {
                    match auth_opt {
                        Some(auth) => {
                            if let Err(e) = self.add_to_network(auth, self.peer_config.clone(), None).await {
                                log::warn!("Error adding peer by auth info: {:?}", e);
                            }
                        },
                        None => log::error!("New auth was closed unexpectedly, can't work properly"),
                    }
                }
                peer_opt = new_peer => {
                    match peer_opt {
                        Some((mut peer, conn)) => {
                            tokio::spawn(async move {peer.handle_peer(conn).await});
                        },
                        None => log::error!("New peers was closed unexpectedly, can't work properly"),
                    }
                }
            };
        }
    }

    pub async fn start_connect(
        mut self, connect_addr: SocketAddr
    ) -> std::io::Result<()> {
        log::trace!("Connecting to {}...", connect_addr);
        let init_connection = TcpStream::connect(connect_addr).await?;
        log::trace!("Established TCP connection with {:?}", init_connection.peer_addr());
        if let Err(e) = self.handle_new_con(
            init_connection, self.peer_config.clone()
        ).await {
            log::error!("Could not join network through {}: {:?}", connect_addr, e);
        };
        self.start().await?;
        Ok(())
    }

    async fn handle_new_con(
        &mut self, stream: TcpStream, peer_config: Config
    ) -> Result<(), Error> {
        let mut conn = Connection::from_stream(stream);
        let peer_auth = peer::Peer::exchange_auth(
            self.self_info.clone(), &mut conn
        ).await
            .map_err(Error::PeerError)?;
        self.add_to_network(peer_auth, peer_config, Some(conn)).await?;
        Ok(())
    }

    // Based on the identity either send Peer the connection or add a new peer handler
    async fn add_to_network(
        &mut self,
        auth: AuthInfo,
        peer_config: Config,
        conn_opt: Option<Connection<TcpStream>>
    ) -> Result<(), Error> {
        log::debug!("Adding peer (auth {:?}, connection {:?}) to network", auth, conn_opt);
        match self.notifiers.get_mut(&auth.identity) {
            Some(notifier) => {
                log::debug!("Peer {} is already known", &auth.identity);
                if let Some(conn) = conn_opt {
                    log::debug!("Sending new connection to handler");
                    notifier.notify_new_connection(auth, conn).await?;
                }
                else {
                    log::debug!("No connection was provieded, ignoring");
                }
            },
            None => {
                log::debug!("Peer {} is unknown, creating a new handler", &auth.identity);
                let (peer, notifier) = ConnectionNotifier::peer_notifier(
                    self.peers_info.clone(),
                    peer_config,
                    auth.listen_addr,
                    auth.identity,
                    self.self_info.clone(),
                    self.new_auth_sender.clone()
                ).map_err(Error::PeerError)?;
                self.notifiers.insert(
                    auth.identity,
                    notifier
                );
                log::trace!("Scheduling the handler for execution");
                self.new_peers_sender.send((peer, conn_opt)).await
                    .map_err(Error::MpscSendPeerError)?;
            },
        };
        Ok(())
    }
}