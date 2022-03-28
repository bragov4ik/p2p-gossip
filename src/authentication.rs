use std::{sync::Arc, collections::hash_map::DefaultHasher, hash::Hasher, fmt::Display};
use rustls::{client::ServerCertVerified, Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Serialize, Deserialize)]
pub struct Identity{
    pubkey_hash: u64
}

impl Identity {
    pub fn new(pubkey: &[u8]) -> Arc<Self> {
        let pubkey_hash = Self::compute_u64(pubkey);
        Arc::new(Self{pubkey_hash})
    }

    pub fn compute_u64(pubkey: &[u8]) -> u64 {
        let mut s = DefaultHasher::new();
        for b in pubkey {
            s.write_u8(*b);
        }
        s.finish()
    }

    pub fn as_u64(&self) -> u64 {
        return self.pubkey_hash
    }

    pub fn compare_key(&self, pubkey: &[u8]) -> bool {
        let pubkey_hash = Self::compute_u64(pubkey);
        pubkey_hash == self.pubkey_hash
    }
}

impl Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        u64::fmt(&self.pubkey_hash, f)
    }
}

struct PeerVerification {
    peer_id: Identity,
}

impl PeerVerification {
    fn new(peer_id: Identity) -> Arc<Self> {
        Arc::new(Self{peer_id})
    }
}

impl rustls::client::ServerCertVerifier for PeerVerification {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> Result<ServerCertVerified, Error> {
        if self.peer_id.compare_key(&end_entity.0) {
            Ok(ServerCertVerified::assertion())
        }
        else {
            Err(Error::InvalidCertificateData(
                format!(
                    "Certificate hash {} didn't match identity of peer {}",
                    Identity::compute_u64(&end_entity.0), self.peer_id.as_u64())
            ))
        }
    }
}