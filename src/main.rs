use std::net::SocketAddr;
use tokio::time::Duration;
use clap::Parser;

mod connection;
mod peer;
mod network;

#[derive(Parser)]
#[clap(author, version, about)]
struct Args {
    period: u64,
    port: u16,
    connect: Option<SocketAddr>,
}

#[tokio::main]
async fn main() {
    // Log configuration
    simple_logger::SimpleLogger::new().init().unwrap();

    // Application configuration
    let args = Args::parse();
    let identity = rand::random::<u64>();
    let listen_addr = SocketAddr::new("0.0.0.0".parse().unwrap(), args.port);
    let net = network::Network::new(identity, listen_addr);
    let config = peer::Config{
        ping_period: Duration::from_secs(args.period),
        hb_period: Duration::from_secs(1),
        hb_timeout: Duration::from_secs(3),
    };
    let res = match args.connect {
        Some(remote_addr) => {
            todo!()
        },
        None => {
            net.start(config).await
        },
    };
}
