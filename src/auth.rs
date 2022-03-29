use rustls::{client::ServerCertVerified, server::ClientCertVerified, Error};
use serde::{Deserialize, Serialize};
use std::{collections::hash_map::DefaultHasher, fmt::Display, hash::Hasher, sync::Arc};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize)]
pub struct Identity {
    pubkey_hash: u64,
}

impl Identity {
    pub fn new(pubkey: &[u8]) -> Arc<Self> {
        let pubkey_hash = Self::compute_u64(pubkey);
        Arc::new(Self { pubkey_hash })
    }

    pub fn compute_u64(pubkey: &[u8]) -> u64 {
        let mut s = DefaultHasher::new();
        for b in pubkey {
            s.write_u8(*b);
        }
        s.finish()
    }

    pub fn as_u64(&self) -> u64 {
        self.pubkey_hash
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

pub struct PeerVerifier {
    peer_id: Identity,
}

impl PeerVerifier {
    pub fn new(peer_id: Identity) -> Arc<Self> {
        Arc::new(Self { peer_id })
    }
}

impl rustls::client::ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<ServerCertVerified, Error> {
        if self.peer_id.compare_key(&end_entity.0) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::InvalidCertificateData(format!(
                "Certificate hash {} didn't match identity of peer {}",
                Identity::compute_u64(&end_entity.0),
                self.peer_id.as_u64()
            )))
        }
    }
}

impl rustls::server::ClientCertVerifier for PeerVerifier {
    fn client_auth_root_subjects(&self) -> Option<rustls::DistinguishedNames> {
        Some(vec![])
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _now: std::time::SystemTime,
    ) -> Result<rustls::server::ClientCertVerified, Error> {
        if self.peer_id.compare_key(&end_entity.0) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(Error::InvalidCertificateData(format!(
                "Certificate hash {} didn't match identity of peer {}",
                Identity::compute_u64(&end_entity.0),
                self.peer_id.as_u64()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use rustls::{client::ServerCertVerifier, Certificate, server::ClientCertVerifier};
    use tracing::Level;

    use crate::utils::{gen_cert_private_key, init_debugging};

    use super::*;

    #[test]
    fn test_identity() {
        init_debugging(Level::ERROR);
        let (cert1, _) = gen_cert_private_key();
        let (cert2, _) = gen_cert_private_key();
        let id = Identity::new(&cert1.0);
        assert!(id.compare_key(&cert1.0));
        assert!(!id.compare_key(&cert2.0));
        assert_ne!(Identity::compute_u64(&cert1.0), Identity::compute_u64(&cert2.0));
    }

    fn test_verify_server(verifier: Arc<PeerVerifier>, cert: Certificate) -> bool {
        verifier.verify_server_cert(
            &cert,
            &vec![],
            &"example.com".try_into().unwrap(),
            &mut (&[]).iter().copied(),
            &vec![],
            std::time::SystemTime::now()
        ).is_ok()
    }

    fn test_verify_client(verifier: Arc<PeerVerifier>, cert: Certificate) -> bool {
        verifier.verify_client_cert(
            &cert,
            &vec![],
            std::time::SystemTime::now(),
        ).is_ok()
    }

    #[test]
    fn test_verifier() {
        init_debugging(Level::ERROR);
        let (cert1, _) = gen_cert_private_key();
        let (cert2, _) = gen_cert_private_key();
        let id = Identity::new(&cert1.0);
        let verifier = PeerVerifier::new((*id).clone());
        assert!(test_verify_server(verifier.clone(), cert1.clone()));
        assert!(!test_verify_server(verifier.clone(), cert2.clone()));
        assert!(test_verify_client(verifier.clone(), cert1));
        assert!(!test_verify_client(verifier.clone(), cert2));
    }
}