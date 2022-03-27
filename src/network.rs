use std::{collections::HashMap, sync::{Arc, Mutex}, net::SocketAddr};

use tokio::{sync::mpsc, net::{TcpListener, TcpStream}};

use crate::{
    peer::{self, Config, Identity, Info, Peer, Shared},
    connection::{self, LocalInfo, Connection}
};

struct ConnectionNotifier {
    new_address_sender: mpsc::Sender<(SocketAddr, Connection<TcpStream>)>,
}

impl ConnectionNotifier {
    /// Create peer with associated notifier
    pub fn peer_notifier(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        conn: Connection<TcpStream>,
        addr: SocketAddr,
        id: Identity,
        self_info: Arc<connection::LocalInfo>,
    ) -> Result<(Peer, Self), peer::Error> {
        // TODO config the number
        let (new_address_sender, new_address_receiver) = mpsc::channel(32);
        let peer = Peer::new_with_connection_update(
            peers_info, config, conn, addr, id, self_info, new_address_receiver
        )?;
        Ok((peer, ConnectionNotifier{new_address_sender}))
    }

    pub async fn notify_new_connection(
        &mut self, listen_addr: SocketAddr, conn: Connection<TcpStream>,
    ) -> Result<(), Error> {
        self.new_address_sender.send((listen_addr, conn)).await
            .map_err(Error::MpscSendAddrError)
    }
}

pub struct Network {
    notifiers: HashMap<Identity, ConnectionNotifier>,
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
    self_info: Arc<connection::LocalInfo>,
    // New Peers handlers scheduled for run
    new_peers_receiver: mpsc::Receiver<Peer>,
    new_peers_sender: mpsc::Sender<Peer>,
}

#[derive(Debug)]
pub enum Error {
    PeerError(peer::Error),
    MpscSendAddrError(mpsc::error::SendError<(SocketAddr, Connection<TcpStream>)>),
    MpscSendPeerError(mpsc::error::SendError<Peer>)
}

impl Network {
    pub fn new(identity: Identity, listen_addr: SocketAddr) -> Self {
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        let peers = HashMap::new();
        // TODO config the number
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(64);
        let self_info = Arc::new(connection::LocalInfo {
            identity,
            listen_addr,
        });
        Network{notifiers: peers, peers_info, self_info, new_peers_receiver, new_peers_sender}
    }

    pub async fn start(
        mut self, peer_config: Config 
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.self_info.listen_addr).await?;
        loop {
            let new_peer = self.new_peers_receiver.recv();
            tokio::select! {
                listen_res = listener.accept() => {
                    match listen_res {
                        Ok((stream, _addr)) => {
                            if let Err(Error::PeerError(e)) = self.manage_new_con(
                                stream, peer_config.clone()
                            ).await {
                                match e {
                                    peer::Error::ConnectionError(_) |
                                    peer::Error::UnexpectedMessage(_) => {
                                        log::warn!("{:?}", e);
                                    },
                                    peer::Error::MutexPoisoned(_) => {
                                        log::error!("Some mutex was poisoned, unable to continue");
                                        return Ok(())
                                    },
                                }
                            };
                        },
                        Err(e) => {
                            log::warn!("Couldn't accept incoming connection: {}", e);
                            continue;
                        },
                    };
                }
                peer_opt = new_peer => {
                    match peer_opt {
                        Some(mut peer) => {
                            tokio::spawn(async move {peer.handle_peer().await});
                        },
                        None => {
                            log::error!("New peers was closed unexpectedly, can't accept new peers");
                            continue;
                        },
                    }
                }
            };
        }
    }

    // pub async fn start_connect(
    //     mut self, peer_config: Config, listen_addr: SocketAddr, connect_addr: SocketAddr
    // ) -> std::io::Result<()> {

    // }

    async fn manage_new_con(
        &mut self, stream: TcpStream, peer_config: Config
    ) -> Result<(), Error> {
        let mut conn = Connection::from_stream(stream);
        let add_me_req = self.initiate_peer(&mut conn).await?;
        self.add_to_network(add_me_req, peer_config, conn).await?;
        Ok(())
    }

    // Get identity of the newcomer
    async fn initiate_peer(
        &self, conn: &mut Connection<TcpStream>
    ) -> Result<LocalInfo, Error> {
        // First message in the connection should be AddMe with info
        let m = conn.recv_message().await
            .map_err(peer::Error::ConnectionError)
            .map_err(Error::PeerError)?;
        if let connection::Message::Authenticate(info) = m {
            Ok(info)
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

    // Based on the identity either notify Peer about new address or add new one
    async fn add_to_network(
        &mut self,
        id_addr: LocalInfo,
        peer_config: Config,
        conn: Connection<TcpStream>
    ) -> Result<(), Error> {
        match self.notifiers.get_mut(&id_addr.identity) {
            Some(notifier) => {
                notifier.notify_new_connection(id_addr.listen_addr, conn).await?;
            },
            None => {
                let (peer, notifier) = ConnectionNotifier::peer_notifier(
                    self.peers_info.clone(),
                    peer_config,
                    conn,
                    id_addr.listen_addr,
                    id_addr.identity,
                    self.self_info.clone(),
                ).map_err(Error::PeerError)?;
                self.notifiers.insert(
                    id_addr.identity,
                    notifier
                );
                self.new_peers_sender.send(peer).await
                    .map_err(Error::MpscSendPeerError)?;
            },
        };
        Ok(())
    }
}