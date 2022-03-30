use clap::Parser;
use rustls::{Certificate, PrivateKey};
use std::{
    fs::File,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tokio::time::Duration;

use crate::utils::gen_cert_private_key;

mod auth;
mod connection;
mod network;
mod peer;
mod utils;

#[derive(Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(long, default_value = "5")]
    period: u64,
    #[clap(long, default_value = "8080")]
    port: u16,
    #[clap(long)]
    connect: Option<SocketAddr>,
    #[clap(long)]
    key: Option<PathBuf>,
    #[clap(long)]
    cert: Option<PathBuf>,
}

fn read_cert(cert_path: &Path) -> std::io::Result<Option<Certificate>> {
    let file = File::open(cert_path)?;
    let mut file_reader = BufReader::new(file);
    let cert: Vec<Certificate> = rustls_pemfile::certs(&mut file_reader)
        .map(|mut vec| vec.drain(..).map(Certificate).collect())?;
    Ok(cert.get(0).cloned())
}

fn read_private_key(key_path: &Path) -> std::io::Result<Option<PrivateKey>> {
    let file = File::open(key_path)?;
    let mut file_reader = BufReader::new(file);
    let key: Vec<PrivateKey> = rustls_pemfile::rsa_private_keys(&mut file_reader)
        .map(|mut vec| vec.drain(..).map(PrivateKey).collect())?;
    Ok(key.get(0).cloned())
}

fn get_private_key_cert(
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
) -> std::io::Result<Option<(Certificate, PrivateKey)>> {
    if let (Some(cp), Some(kp)) = (cert_path, key_path) {
        let cert = read_cert(&cp)?;
        let key = read_private_key(&kp)?;
        if let (Some(k), Some(c)) = (key, cert) {
            tracing::debug!("Read key/cert successfully!");
            return Ok(Some((c, k)));
        } else {
            return Ok(None);
        }
    }
    tracing::debug!("Saved key/cert not found, generating new");
    Ok(Some(gen_cert_private_key()))
}

#[tokio::main]
async fn main() {
    // Log configuration
    tracing_subscriber::fmt::init();

    // Application configuration
    let args = Args::parse();
    let read_res = get_private_key_cert(args.cert.clone(), args.key.clone());
    let (cert, private_key) = match read_res {
        Ok(Some(key)) => key,
        Ok(None) => {
            tracing::error!(
                "Found no keys in {:?} or certs in {:?}",
                args.key,
                args.cert
            );
            return;
        }
        Err(e) => {
            tracing::error!("Couldn't open {:?} or {:?}: {:?}", args.key, args.cert, e);
            return;
        }
    };
    let listen_addr = SocketAddr::new("0.0.0.0".parse().unwrap(), args.port);
    let config = peer::Config {
        ping_period: Duration::from_secs(args.period),
        hb_period: Duration::from_secs(1),
        hb_timeout: Duration::from_secs(3),
    };

    println!("Launching peer, listening on {}", listen_addr);
    tracing::info!("Launching peer");
    tracing::info!("\tlisten_addr: {}", listen_addr);
    tracing::trace!("\tidentity: {}", auth::Identity::compute_u64(&cert.0[..]));
    tracing::trace!("\tpeer_config: {}", config);
    let net = network::Network::new(private_key, cert, listen_addr, config);

    let res = match args.connect {
        Some(remote_addr) => net.start_connect(remote_addr).await,
        None => net.start_listen().await,
    };
    match res {
        Ok(_) => println!("Shutting down.."),
        Err(e) => {
            tracing::error!("Error running the peer: {}", e);
            println!("Error running the peer {}.\nShutting down..", e)
        }
    }
}
