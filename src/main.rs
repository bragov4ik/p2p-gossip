use std::net::SocketAddr;
use authentication::Identity;
use tokio::time::Duration;
use clap::Parser;

mod connection;
mod peer;
mod network;
mod authentication;

#[derive(Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(long, default_value="5")]
    period: u64,
    #[clap(long, default_value="8080")]
    port: u16,
    #[clap(long)]
    connect: Option<SocketAddr>,
    // #[clap(long)]
    // id: Option<Identity>,
}

#[tokio::main]
async fn main() {
    // Log configuration
    tracing_subscriber::fmt::init();

    // Application configuration
    let args = Args::parse();
    // let identity = match args.id { TODO generate/import
    //     Some(id) => id,
    //     None => rand::random::<u64>(),
    // };
    let listen_addr = SocketAddr::new("0.0.0.0".parse().unwrap(), args.port);
    let config = peer::Config{
        ping_period: Duration::from_secs(args.period),
        hb_period: Duration::from_secs(1),
        hb_timeout: Duration::from_secs(3),
    };

    tracing::info!("Launching peer");
    tracing::trace!("\tidentity: {}", identity);
    tracing::trace!("\tlisten_addr: {}", listen_addr);
    tracing::trace!("\tpeer_config: {}", config);
    let net = network::Network::new(identity, listen_addr, config);

    let res = match args.connect {
        Some(remote_addr) => {
            net.start_connect(remote_addr).await
        },
        None => {
            net.start_listen().await
        },
    };
    match res {
        Ok(_) => println!("Shutting down.."),
        Err(e) => {
            tracing::error!("Error running the peer: {}", e);
            println!("Error running the peer {}.\nShutting down..", e)
        },
    }
}
