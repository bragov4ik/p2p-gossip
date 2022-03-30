use rand::{distributions::Alphanumeric, Rng};
use rustls::{Certificate, PrivateKey};
use tracing::Level;

#[derive(Debug)]
pub struct MutexPoisoned {}

pub fn gen_cert_private_key() -> (Certificate, PrivateKey) {
    let gen = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = Certificate(gen.serialize_der().unwrap());
    let key = PrivateKey(gen.serialize_private_key_der());
    (cert, key)
}

pub fn gen_random_message() -> String {
    let str = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    str   
}

#[allow(dead_code)]
pub fn init_debugging(lvl: Level) {
    if let Err(e) = tracing_subscriber::fmt().with_max_level(lvl).try_init() {
        tracing::debug!("Couldn't init tracing_subscriber: {}", e);
    }
}
