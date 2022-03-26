use tokio::sync::{mpsc, watch};


pub struct ShutdownReceiver {
    // Channel for shutdown notification
    shutdown_receiver: mpsc::Receiver<()>,

    shutdown_received: bool,
    
    // Channel for notifying about successfull shutdown
    notify_sender: watch::Sender<()>,
}

impl ShutdownReceiver {
    fn new(shutdown_receiver: mpsc::Receiver<()>, notify_sender: watch::Sender<()>) -> Self {
        ShutdownReceiver { shutdown_receiver, shutdown_received: false, notify_sender }
    }

    /// Future that resolves when shutdown signal is arrived
    /// 
    /// If the channel for receiving the signal is closed, it will never resolve
    pub async fn recv_shutdown(&mut self) {
        log::trace!("Waiting for shutdown signal...");
        if self.shutdown_received {
            log::trace!("Shutdown have been already received, done waiting");
            return
        }
        match self.shutdown_receiver.recv().await {
            Some(_) => {
                log::trace!("Received shutdown!");
                self.shutdown_received = true;
                return
            },
            None => {
                log::warn!("ShutdownReceiver's shutdown channel is closed, therefore no \
                signal will come");
                futures::future::pending().await
            },
        }
    }

    /// Notifies watchers that this job has completed
    pub async fn finish_shutdown(&mut self) {
        log::trace!("Notifying other side that shutdown is complete");
        // Don't care if anyone is listening
        self.notify_sender.send(()).unwrap_or(());
        log::trace!("Done with ShutdownReceiver");
    }
}

pub struct ShutdownSender {
    shutdown_sender: mpsc::Sender<()>,

    is_shutdown: bool,
    
    // Channel for notifying about successfull shutdown
    shutdown_notification: watch::Receiver<()>,
}

impl ShutdownSender {
    fn new(shutdown_sender: mpsc::Sender<()>, shutdown_notification: watch::Receiver<()>) -> Self {
        ShutdownSender { shutdown_sender, is_shutdown: false, shutdown_notification }
    }

    pub async fn send_shutdown(&mut self) {
        log::trace!("Sending shutdown...");
        match self.shutdown_sender.send(()).await {
            Ok(a) => log::trace!("Shutdown has been sent!"),
            // If the receiver is closed, it has already shut down (should be)
            Err(b) => {
                log::trace!("Couldn't send shutdown, assuming already finished");
                self.is_shutdown = true
            },
        }
    }

    /// Resolves when the shutdown is completed
    pub async fn wait_finish(&mut self) {
        log::trace!("Waiting for shutdown to finish...");
        if self.is_shutdown {
            log::trace!("Shutdown have been already completed, done waiting");
            return
        }
        log::trace!("Waiting for shutdown to complete...");
        // Ok() tells that shutdown notification is received
        // Err(_) means that sender was dropped so the other side has shut down
        self.shutdown_notification.changed().await;
        self.is_shutdown = true;
        log::trace!("Shutdown has completed, done with ShutdownSender");
    }
}

pub fn new_shutdown() -> (ShutdownSender, ShutdownReceiver) {
    let mpsc = mpsc::channel(1);
    let watch = watch::channel(());
    let s = ShutdownSender::new(mpsc.0, watch.1);
    let r = ShutdownReceiver::new(mpsc.1, watch.0);
    (s, r)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use tokio::time::{sleep, Duration, Instant};

    // Returns how much time passed
    async fn shutdown_target(mut shut: ShutdownReceiver, sleep_millis: u64) -> Duration {
        let start = Instant::now();
        let wait = sleep(Duration::from_millis(sleep_millis));
        tokio::select! {
            a = wait => {

            }
            b = shut.recv_shutdown() => {
                shut.finish_shutdown().await;
            }
        }
        Instant::now() - start
    }

    // Returns how long did it take for target to finish shutdown
    async fn shutdown_source(mut shut: ShutdownSender, sleep_millis: u64) -> Duration {
        sleep(Duration::from_millis(sleep_millis)).await;
        shut.send_shutdown().await;
        let start = Instant::now();
        shut.wait_finish().await;
        Instant::now() - start
    }

    #[tokio::test]
    async fn one_to_one() {
        simple_logger::SimpleLogger::new().init().unwrap();

        let (s, r) = new_shutdown();
        let target = shutdown_target(r, 1000);
        let source = shutdown_source(s, 0);
        let (target_ran, shutdown_happened) = tokio::join!(target, source);
        // Make sure the signal worked and target didn't just wait for completion
        assert!(target_ran.as_millis() < 900);
        // Make sure the target didn't just wait for completion, pretty redundant tbh
        assert!(shutdown_happened.as_millis() < 900);
    }

    // add more cases
}