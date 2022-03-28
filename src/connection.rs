use futures::{sink::SinkExt, stream::StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{BytesCodec, Decoder, Framed};

#[derive(Debug)]
pub enum Error {
    SerializationError(bincode::Error),
    IOError(std::io::Error),
    StreamEnded,
}

#[derive(Debug)]
pub struct Connection<T>
where
    T: AsyncRead + AsyncWrite + Sized + std::marker::Unpin + std::fmt::Debug,
{
    framed_stream: Framed<T, BytesCodec>,
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite + Sized + std::marker::Unpin + std::fmt::Debug,
{
    pub fn from_stream(stream: T) -> Self {
        let framed_stream = BytesCodec::new().framed(stream);
        Connection { framed_stream }
    }

    #[tracing::instrument(level = "debug")]
    pub async fn send_message<M>(&mut self, m: M) -> Result<(), Error>
    where
        M: Serialize + std::fmt::Debug,
    {
        let bytes = bincode::serialize(&m).map_err(Error::SerializationError)?;
        let bytes = bytes::Bytes::from(bytes);
        self.framed_stream
            .send(bytes)
            .await
            .map_err(Error::IOError)?;
        Ok(())
    }

    #[tracing::instrument(level = "debug")]
    pub async fn recv_message<M>(&mut self) -> Result<M, Error>
    where
        M: DeserializeOwned + std::fmt::Debug,
    {
        let result = match self.framed_stream.next().await {
            Some(v) => v,
            None => return Err(Error::StreamEnded),
        };
        let bytes = result.map_err(Error::IOError)?;
        let bytes = &bytes[..];
        let m = bincode::deserialize(bytes).map_err(Error::SerializationError)?;
        Ok(m)
    }

    pub fn get_ref(&self) -> &T {
        self.framed_stream.get_ref()
    }

    pub fn into_inner(self) -> T {
        self.framed_stream.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use crate::{auth::Identity, peer::Message, utils::init_debugging};
    use tokio::time;

    use super::*;
    use tracing::{debug, error, info, Level};

    #[derive(Debug, Clone, PartialEq)]
    enum Error {
        TimedOut,
        UnexpectedMessage(String),
        ConnectionError(String),
    }

    /// Sends `to_send` and expects `expect_recv` back `repeat` times.
    #[tracing::instrument(skip(conn))]
    async fn send_recv<M, T>(
        mut conn: Connection<T>,
        to_send: M,
        expect_recv: M,
        repeat: u64,
    ) -> Result<(), Error>
    where
        M: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + Clone,
        T: AsyncRead + AsyncWrite + Sized + std::marker::Unpin + std::fmt::Debug,
    {
        for _i in 0..repeat {
            debug!("Sending message {}", _i);
            conn.send_message(to_send.clone())
                .await
                .map_err(|e| Error::ConnectionError(format!("{:?}", e)))?;
            debug!("Receiving message {}", _i);
            let received: M = conn
                .recv_message()
                .await
                .map_err(|e| Error::ConnectionError(format!("{:?}", e)))?;
            if received != expect_recv {
                return Err(Error::UnexpectedMessage(format!(
                    "Unexpected message received: {:?}, expected {:?}",
                    received, expect_recv
                )));
            }
        }
        debug!("Done!");
        Ok(())
    }

    /// Sets up stream with 2 `Connection`s on each side. After that sends `msg1` and expects `msg2`
    /// back `repeat` times for first side and vice versa for the other part.
    #[tracing::instrument]
    async fn test_case<M>(msg1: M, msg2: M, repeat: u64) -> Result<(), Error>
    where
        M: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + Clone,
    {
        debug!("Starting test");
        debug!("Creating connections");
        let (client, server) = tokio::io::duplex(128);
        let client_conn = Connection::from_stream(client);
        let server_conn = Connection::from_stream(server);
        debug!("Connections created!");
        let client_fut = send_recv(client_conn, msg1.clone(), msg2.clone(), repeat);
        let server_fut = send_recv(server_conn, msg2, msg1, repeat);
        let client_fut = time::timeout(time::Duration::from_millis(300 * repeat), client_fut);
        let server_fut = time::timeout(time::Duration::from_millis(300 * repeat), server_fut);

        debug!("Starting transfers");
        let (c_res, s_res) = tokio::join!(client_fut, server_fut);

        // Report timeouts
        let c_res = c_res.map_err(|_| Error::TimedOut);
        let s_res = s_res.map_err(|_| Error::TimedOut);
        let c_res = c_res.clone().and_then(|x| x);
        let s_res = s_res.clone().and_then(|x| x);
        c_res.and(s_res).map(|a| {
            debug!("Transfers completed!");
            debug!("Test completed!");
            a
        })
    }

    // Run async test case in sync function to allow working with different types
    fn run_one<M>(msg1: M, msg2: M) -> Result<(), Error>
    where
        M: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + Clone,
    {
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime.block_on(test_case(msg1, msg2, 1))
    }

    // Simple test
    #[test]
    fn transfer_different_types() {
        init_debugging(Level::INFO);

        info!("Running tests");
        let results = vec![
            run_one(1, 2),
            run_one("aboba".to_owned(), "abeba".to_owned()),
            run_one((1, "a".to_owned()), (2, "b".to_owned())),
            run_one(Message::Ping, Message::Heartbeat),
            run_one(
                Message::ListPeersRequest,
                Message::ListPeersResponse(vec![
                    (*Identity::new(b"1"), "127.0.0.1:8080".parse().unwrap()),
                    (*Identity::new(b"2"), "127.0.0.2:5050".parse().unwrap()),
                ]),
            ),
        ];
        // Not elegant but works
        let mut failed = false;
        for (i, res) in results.iter().enumerate() {
            if let Err(e) = res {
                error!("Error running case {}: {:?}", i, e);
                failed = true;
            }
        }
        if failed {
            assert!(false);
        }
    }

    // Thanks https://stackoverflow.com/a/58175659
    fn do_vecs_match<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }

    // Test buffering
    #[tokio::test]
    async fn transfer_multiple_messages() {
        init_debugging(Level::INFO);

        info!("Running tests");
        let tests = [
            (test_case("a".to_owned(), "b".to_owned(), 1)),
            // (test_case("a".to_owned(), 10, "b".to_owned(), 3)),
        ];
        let results_expected: Vec<Result<(), Error>> = vec![Ok(()); tests.len()];
        let results = &futures::future::join_all(tests).await;
        for (i, res) in results.iter().enumerate() {
            if let Err(e) = res {
                error!("Error running case {}: {:?}", i, e);
            }
        }
        assert!(do_vecs_match(
            &results.iter().cloned().collect::<Vec<Result<(), Error>>>(),
            &results_expected
        ));
    }
}
