use std::net::SocketAddr;
use udppm::{ ConnectionManager, ChannelManager, WriteChannel, ReadChannel, SendRequest };
use tokio::{time::{sleep, Duration}, sync::mpsc::error::TryRecvError::{Empty, Disconnected}};

async fn handle(
    w_chan: WriteChannel<String>, mut r_chan: ReadChannel<String>, dest: SocketAddr
) {
    loop {
        loop {
            match r_chan.try_recv() {
                Ok(msg) => println!("{} - {}", dest, msg),
                Err(e) => {
                    match e { Empty => break, Disconnected => return, };
                },
            }
        }
        sleep(Duration::from_secs(2)).await;
        let payload = "Hello, World!".to_string();
        w_chan.send(SendRequest{ dest, payload }).await.ok();
    }
}

async fn manager_test(con: ConnectionManager<String>, chan: ChannelManager<String>) {
    let addr = con.local_addr().unwrap();
    let (w, r) = 
        chan.add_connection(addr, 32).unwrap();
    let fut = handle(w, r, addr);
    let con_fut = con.run_buf::<256>();
    tokio::join!(fut, con_fut);
}

async fn init_manager() -> (ConnectionManager<String>, ChannelManager<String>) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (con, chan) = 
        udppm::managers::<_, String>(addr, 32).await.unwrap();
    return (con, chan)
}

#[tokio::main]
async fn main() {
    let (con1, chan1) = init_manager().await;
    let (con2, chan2) = init_manager().await;
    tokio::join!(
        manager_test(con1, chan1),
        manager_test(con2, chan2),
    );
}
