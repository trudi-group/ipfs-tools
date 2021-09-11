use bytes::Bytes;
use failure::ResultExt;
use futures::StreamExt;
use ipfs_resolver_common::wantlist::JSONMessage;
use ipfs_resolver_common::Result;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A connection implements receiving JSON messages from a bitswap logging monitor.
/// The messages are read from the underlying TCP socket within length-delimited frames and then
/// JSON-decoded.
pub struct Connection {
    pub remote: SocketAddr,
    pub messages_in: Receiver<JSONMessage>,
}

impl Connection {
    pub fn new(conn: TcpStream) -> Result<Connection> {
        let remote = conn.peer_addr()?;
        // Set SO_NODELAY, i.e. disable Nagle's algorithm.
        // What we really want is setting TCP_QUICKACK, i.e. disable delayed ACKs.
        // But we can't do that cross-platform...
        conn.set_nodelay(true)
            .context("unable to disable Nagle's algorithm")?;
        // We also set this giant buffer size, hopefully that helps.
        // This is enough space for four maximum-sized frames, including their length frame.
        conn.set_recv_buffer_size((32 * 1024 * 1024 + 4) * 4)
            .context("unable to set large receive buffer")?;

        // Set up length-delimited frames
        let framed = Framed::new(
            conn,
            LengthDelimitedCodec::builder()
                .length_field_length(4)
                .max_frame_length(32 * 1024 * 1024) // 32 MiB maximum frame size, which is _gigantic_ but necessary in some cases.
                .new_codec(),
        );

        // Set up some plumbing...
        let (tx_decode, mut rx_decode) = channel::<Bytes>(100);
        let (mut tx_message_in, rx_message_in) = channel::<JSONMessage>(100);

        // Decode incoming messages
        task::spawn(async move {
            while let Some(buf) = rx_decode.recv().await {
                debug!("decoder {}: got buffer {:?}", remote, buf);

                let res = serde_json::from_slice(&buf);
                if let Err(e) = res {
                    error!("decoder {}: unable to decode: {:?}", remote, e);
                    break;
                }
                let msg: Vec<JSONMessage> = res.unwrap();
                debug!("decoder {}: decoded {:?}", remote, msg);

                for wl in msg.into_iter() {
                    let res = tx_message_in.send(wl).await;
                    if let Err(e) = res {
                        debug!("decoder {}: unable to send: {:?}", remote, e);
                        break;
                    }
                }
            }

            debug!("decoder {}: shutting down", remote);
        });

        // Handle socket I/O
        task::spawn(Self::handle_socket_io(remote, framed, tx_decode));

        Ok(Connection {
            remote,
            messages_in: rx_message_in,
        })
    }

    async fn handle_socket_io(
        remote: SocketAddr,
        mut framed: Framed<TcpStream, LengthDelimitedCodec>,
        mut bytes_in: Sender<Bytes>,
    ) {
        while let Some(in_bytes) = framed.next().await {
            if let Err(e) = in_bytes {
                error!("I/O {}: socket read error: {:?}, shutting down", remote, e);
                return;
            }

            let res = bytes_in.send(in_bytes.unwrap().freeze()).await;
            if let Err(e) = res {
                // This can only happen if the decoder shut down, i.e. we're dropping the
                // client.
                debug!(
                    "I/O {}: unable to send to decoder: {:?}, shutting down",
                    remote, e
                );
                return;
            }
        }
        debug!("I/O {}: incoming connection closed, shutting down", remote);
    }
}
