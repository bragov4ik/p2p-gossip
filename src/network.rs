use std::{collections::HashMap, sync::{Arc, Mutex}, net::SocketAddr};

use tokio::{sync::mpsc, net::{TcpListener, TcpStream}, io::{AsyncRead, AsyncWrite}};

use crate::{
    peer::{self, Config, Identity, Info, Peer, Shared},
    connection::{self, AddMePayload, Connection}
};

struct AddressNotifier {
    new_address_sender: mpsc::Sender<SocketAddr>,
}

impl AddressNotifier {
    /// Create peer with associated notifier
    pub fn peer_notifier<T>(
        peers_info: Shared<HashMap<Identity, Shared<Info>>>,
        config: Config,
        conn: Connection<T>,
        addr: SocketAddr,
        id: Identity,
    ) -> Result<(Peer<T>, Self), peer::Error>
    where
        T: AsyncRead + AsyncWrite + Sized + std::marker::Unpin
    {
        // TODO config the number
        let (new_address_sender, new_address_receiver) = mpsc::channel(32);
        let peer = Peer::new_with_address_update(
            peers_info, config, conn, addr, id, new_address_receiver
        )?;
        Ok((peer, AddressNotifier{new_address_sender}))
    }

    pub async fn notify_new_address(
        &mut self, new_addr: SocketAddr
    ) -> Result<(), Error> {
        self.new_address_sender.send(new_addr).await
            .map_err(Error::MpscSendAddrError)
    }
}

struct Network {
    notifiers: HashMap<Identity, AddressNotifier>,
    peers_info: Shared<HashMap<Identity, Shared<Info>>>,
    // New Peers handlers scheduled for run
    new_peers_receiver: mpsc::Receiver<Peer<TcpStream>>,
    new_peers_sender: mpsc::Sender<Peer<TcpStream>>,
}

#[derive(Debug)]
pub enum Error {
    PeerError(peer::Error),
    MpscSendAddrError(mpsc::error::SendError<SocketAddr>),
    MpscSendPeerError(mpsc::error::SendError<Peer<TcpStream>>)
}

impl Network {
    pub async fn new() -> Self {
        let peers_info = Arc::new(Mutex::new(HashMap::new()));
        let peers = HashMap::new();
        // TODO config the number
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(64);
        Network{peers_info, notifiers: peers, new_peers_receiver, new_peers_sender}
    }

    pub async fn start(
        mut self, peer_config: Config, listen_addr: SocketAddr
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(listen_addr).await?;
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
    ) -> Result<AddMePayload, Error> {
        // First message in the connection should be AddMe with info
        let m = conn.recv_message().await
            .map_err(peer::Error::ConnectionError)
            .map_err(Error::PeerError)?;
        if let connection::Message::AddMe(info) = m {
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
        id_addr: AddMePayload,
        peer_config: Config,
        conn: Connection<TcpStream>
    ) -> Result<(), Error> {
        match self.notifiers.get_mut(&id_addr.identity) {
            Some(notifier) => {
                notifier.notify_new_address(id_addr.listen_addr).await?;
            },
            None => {
                let (peer, notifier) = AddressNotifier::peer_notifier(
                    self.peers_info.clone(),
                    peer_config,
                    conn,
                    id_addr.listen_addr,
                    id_addr.identity,
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