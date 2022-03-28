use std::sync::{Arc, Mutex};

use rustls::{Certificate, PrivateKey};
use tracing::Level;

#[derive(Debug)]
pub struct MutexPoisoned {}

pub type Shared<T> = Arc<Mutex<T>>;

pub fn gen_private_key_cert() -> (Certificate, PrivateKey) {
    let gen = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = Certificate(gen.serialize_der().unwrap());
    let key = PrivateKey(gen.serialize_private_key_der());
    (cert, key)
}

pub fn init_debugging(lvl: Level) {
    if let Err(e) = tracing_subscriber::fmt()
        .with_max_level(lvl)
        .try_init() 
    {
        tracing::warn!("Couldn't init tracing_subscriber: {}", e);
    }
}
