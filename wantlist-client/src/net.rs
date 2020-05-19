use bytes::Bytes;
use futures::select;
use futures::StreamExt;
use futures_util::future::FutureExt;
use ipfs_resolver_common::wantlist::JSONMessage;
use ipfs_resolver_common::Result;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub(crate) struct Connection {
    pub remote: SocketAddr,
    pub messages_in: Receiver<JSONMessage>,
}

impl Connection {
    pub(crate) async fn new(conn: TcpStream) -> Result<Connection> {
        let remote = conn.peer_addr()?;
        // Set up length-delimited frames
        let framed = Framed::new(
            conn,
            LengthDelimitedCodec::builder()
                .length_field_length(4)
                .new_codec(),
        );

        // Set up some plumbing...
        //let (mut tx_encode, rx_encode) = channel::<Bytes>(100);
        let (tx_decode, mut rx_decode) = channel::<Bytes>(100);
        //let (tx_message_out, mut rx_message_out) = channel::<Message>(100);
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
        task::spawn(Self::handle_socket_io(
            remote, framed, tx_decode, /*, rx_encode*/
        ));

        Ok(Connection {
            remote,
            messages_in: rx_message_in,
        })
    }

    async fn handle_socket_io(
        remote: SocketAddr,
        mut framed: Framed<TcpStream, LengthDelimitedCodec>,
        mut bytes_in: Sender<Bytes>,
        //mut bytes_out: Receiver<Bytes>,
    ) {
        loop {
            select! {
                in_bytes = framed.next().fuse() => {
                    if let None = in_bytes {
                        debug!("I/O {}: incoming connection closed",remote);
                        break;
                    }
                    let in_bytes = in_bytes.unwrap();
                    if let Err(e) = in_bytes {
                        error!("I/O {}: socket read error: {:?}",remote,e);
                        break;
                    }

                    let res = bytes_in.send(in_bytes.unwrap().freeze()).await;
                    if let Err(e) = res {
                        // This can only happen if the decoder shut down, i.e. we're dropping the
                        // client.
                        debug!("I/O {}: unable to send to decoder: {:?}",remote,e);
                        break;
                    }
                },
                /*out_bytes = bytes_out.recv().fuse() => {
                    if let None = out_bytes {
                        debug!("I/O {}: outgoing byte stream closed",remote);
                        break;
                    }
                    let res = framed.send(out_bytes.unwrap()).await;
                    if let Err(e) = res {
                        // TODO handle backpressure? Maybe not because single producer? Maybe not
                        // because tokio channels behave differently on send?
                        error!("I/O {}: unable to send to socket: {:?}",remote,e);
                        break;
                    }
                }*/
            }
        }

        debug!("I/O {}: shutting down", remote);
    }
}
