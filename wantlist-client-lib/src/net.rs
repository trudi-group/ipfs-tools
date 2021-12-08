use bytes::Bytes;
use failure::ResultExt;
use failure::{err_msg, Fail};
use futures::prelude::*;
use futures::select;
use ipfs_resolver_common::wantlist::{JSONWantlistEntry, JsonCID};
use ipfs_resolver_common::Result;
use multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;

pub struct APIClient {
    requests: Sender<RequestTuple>,
}

type RequestTuple = (
    tokio::sync::oneshot::Sender<Result<ResponseType>>,
    RequestType,
);

impl APIClient {
    pub async fn new(conn: TcpStream) -> Result<(APIClient, Receiver<PushedEvent>)> {
        let Connection {
            remote,
            mut messages_in,
            messages_out,
        } = Connection::new(conn)
            .await
            .context("unable to create connection")?;

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<RequestTuple>(1);

        tokio::spawn(async move {
            let mut next_request_id = 1_u16;
            let mut open_reqs = HashMap::new();

            loop {
                select! {
                    req = req_rx.recv().fuse() => {
                        if let None = req {
                            // We're shutting down for some reason
                            debug!("{}: requests channel closed",remote);
                            break;
                        }
                        let (req_result,req) = req.unwrap();
                        debug!("{}: got request {:?}",remote,req);

                        let req_id = next_request_id;
                        next_request_id += 1;

                        if open_reqs.contains_key(&req_id) {
                            // We have 2^16 outstanding requests? Let's hope this does not happen.
                            panic!("more than 2^16 outstanding requests")
                        }
                        open_reqs.insert(req_id,req_result);

                        if let Err(e) = messages_out.send(OutgoingTCPMessage::Request(Request{
                            id: req_id,
                            req_type: req
                        })).await {
                            error!("{}: unable to send outgoing message: {:?}",remote,e);
                            break;
                        }
                    },
                    msg = messages_in.recv().fuse() => {
                        if let None = msg {
                            // We're shutting down for some reason
                            debug!("{}: messages in channel closed",remote);
                            break;
                        }
                        let msg = msg.unwrap();
                        debug!("{}: received message {:?}",remote,msg);

                        match msg {
                            IncomingTCPMessage::Event(e) => {
                                if let Err(e) = event_tx.send(e).await {
                                    error!("{}: unable to send on incoming events channel: {}",remote,e)
                                    // Maybe not break?
                                }
                            }
                            IncomingTCPMessage::Response (resp) => {
                                let Response{id,resp_type} = resp;

                               match open_reqs.remove(&id) {
                                    None => {
                                        error!("{}: got response for untracked request: {:?} ",remote, Response{id,resp_type});
                                        break;
                                    }
                                    Some(resp_sender) => {
                                        if let Err(e) = resp_sender.send(Ok(resp_type)) {
                                            panic!("{}: unable to send on response channel: {:?}",remote,e)
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }

            // We're exiting for some reason. Clear all outstanding requests.
            for (_, resp_chan) in open_reqs.into_iter() {
                if let Err(e) = resp_chan.send(Err(err_msg("Client closing"))) {
                    error!(
                        "{}: unable to send shutdown error to request channel: {:?}",
                        remote, e
                    )
                }
            }
        });

        Ok((APIClient { requests: req_tx }, event_rx))
    }

    async fn request(&self, req: RequestType) -> Result<ResponseType> {
        let (req_tx, req_rx) = tokio::sync::oneshot::channel::<Result<ResponseType>>();

        self.requests
            .send((req_tx, req))
            .await
            .context("unable to enqueue request")?;

        // Can this fail?
        let res = req_rx
            .await
            .expect("unable to read from request response channel")
            .context("unable to perform request")?;

        Ok(res)
    }

    pub async fn ping(&self) -> Result<()> {
        let res = self.request(RequestType::Ping {}).await?;
        match res {
            ResponseType::Ping {} => Ok(()),
            _ => {
                // TODO Now what?
                Err(err_msg("remote sent unexpected response type"))
            }
        }
    }

    pub async fn subscribe(&self) -> Result<()> {
        let res = self.request(RequestType::Subscribe {}).await?;
        match res {
            ResponseType::Subscribe { error } => match error {
                None => Ok(()),
                Some(err) => Err(err_msg(err).context("remote returned error").into()),
            },
            _ => {
                // TODO Now what?
                Err(err_msg("remote sent unexpected response type"))
            }
        }
    }

    pub async fn unsubscribe(&self) -> Result<()> {
        let res = self.request(RequestType::Unsubscribe {}).await?;
        match res {
            ResponseType::Unsubscribe {} => Ok(()),
            _ => {
                // TODO Now what?
                Err(err_msg("remote sent unexpected response type"))
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct VersionMessage {
    version: i32,
}

const PROTOCOL_VERSION: i32 = 2;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum OutgoingTCPMessage {
    #[serde(rename = "request")]
    Request(Request),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Request {
    id: u16,

    #[serde(flatten)]
    req_type: RequestType,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum RequestType {
    #[serde(rename = "ping")]
    Ping {},
    #[serde(rename = "subscribe")]
    Subscribe {},
    #[serde(rename = "unsubscribe")]
    Unsubscribe {},
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Response {
    id: u16,

    #[serde(flatten)]
    resp_type: ResponseType,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ResponseType {
    #[serde(rename = "subscribe")]
    Subscribe { error: Option<String> },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {},
    #[serde(rename = "ping")]
    Ping {},
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IncomingTCPMessage {
    #[serde(rename = "event")]
    Event(PushedEvent),
    #[serde(rename = "response")]
    Response(Response),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PushedEvent {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub peer: String,

    #[serde(flatten)]
    pub inner: EventType,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum EventType {
    #[serde(rename = "bitswap_message")]
    BitswapMessage(BitswapMessage),
    #[serde(rename = "connection_event")]
    ConnectionEvent(ConnectionEvent),
}

/// A Bitswap message received by the monitor and subsequently pushed to us via TCP.
/// This contains both "requests" (updates to the wantlist) as well as "responses" (blocks
/// and block presences).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BitswapMessage {
    pub wantlist_entries: Vec<JSONWantlistEntry>,
    pub full_wantlist: bool,
    pub blocks: Vec<JsonCID>,
    pub block_presences: Vec<BlockPresence>,
}

/// Block presence indication, as contained in a Bitswap message.
/// See the `TCP_BLOCK_PRESENCE_TYPE_` constants for the possible types.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BlockPresence {
    pub cid: JsonCID,
    pub block_presence_type: i32,
}

/// Block presence type constants for TCP messages.
pub const TCP_BLOCK_PRESENCE_TYPE_HAVE: i32 = 0;
pub const TCP_BLOCK_PRESENCE_TYPE_DONT_HAVE: i32 = 1;

/// A connection event, as reported by IPFS.
/// See the `TCP_CONN_EVENT_` constants for the types of connection events.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ConnectionEvent {
    pub remote: Multiaddr,
    pub connection_event_type: i32,
}

/// Connection event type constants for TCP messages.
pub const TCP_CONN_EVENT_CONNECTED: i32 = 0;
pub const TCP_CONN_EVENT_DISCONNECTED: i32 = 1;

/// A connection implements receiving JSON messages from a bitswap logging monitor.
/// The messages are read from the underlying TCP socket within length-delimited frames and then
/// JSON-decoded.
/// This is relatively low-level, you might want to use the `APIClient` instead.
#[derive(Debug)]
pub struct Connection {
    pub remote: SocketAddr,
    pub messages_in: Receiver<IncomingTCPMessage>,
    pub messages_out: Sender<OutgoingTCPMessage>,
}

/// Errors during the handshake procedure.
#[derive(Debug, Fail)]
pub enum HandshakeError {
    #[fail(display = "did not receive a version message")]
    NoVersionReceived,
    #[fail(display = "protocol version mismatch")]
    VersionMismatch,
    #[fail(display = "I/O error :)")]
    IO,
}

impl Connection {
    pub async fn new(conn: TcpStream) -> Result<Connection> {
        let remote = conn.peer_addr()?;
        // Set SO_NODELAY, i.e. disable Nagle's algorithm.
        // What we really want is setting TCP_QUICKACK, i.e. disable delayed ACKs.
        // But we can't do that cross-platform...
        //conn.set_nodelay(true)
        //    .context("unable to disable Nagle's algorithm")?;
        // We also set this giant buffer size, hopefully that helps.
        // This is enough space for four maximum-sized frames, including their length frame.
        //conn.set_recv_buffer_size((32 * 1024 * 1024 + 4) * 4)
        //    .context("unable to set large receive buffer")?;

        // Set up length-delimited frames
        let mut framed = tokio_util::codec::Framed::new(
            conn,
            tokio_util::codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .big_endian()
                .max_frame_length(32 * 1024 * 1024) // 32 MiB maximum frame size, which is _gigantic_ but necessary in some cases.
                .new_codec(),
        );

        Self::ensure_version(&mut framed)
            .await
            .context("unable to perform handshake")?;

        // Set up some plumbing...
        let mut json_framed = tokio_serde::Framed::new(
            framed,
            tokio_serde::formats::Json::<IncomingTCPMessage, OutgoingTCPMessage>::default(),
        );

        let (tx_message_in, rx_message_in) = channel::<IncomingTCPMessage>(1);
        let (tx_messages_out, mut rx_messages_out) = channel::<OutgoingTCPMessage>(1);

        task::spawn(async move {
            loop {
                select! {
                    msg = json_framed.try_next().fuse() => {
                        if let Err(e) = msg {
                            error!("<- {}: unable to read/decode: {:?}", remote, e);
                            break;
                        }
                        let msg = msg.unwrap();
                        if let None = msg {
                            debug!("<- {}: channel closed", remote);
                            break;
                        }
                        let msg = msg.unwrap();
                        debug!("<- {}: got message {:?}", remote, msg);

                        let res = tx_message_in.send(msg).await;
                        if let Err(e) = res {
                            // This is somewhat weird -- maybe the Connection was dropped?
                            debug!("<- {}: unable to send: {:?}", remote, e);
                            break;
                        }
                    },
                    out_msg = rx_messages_out.recv().fuse() => {
                        if let None = out_msg {
                            debug!("-> {}: channel closed", remote);
                            break;
                        }
                        let msg = out_msg.unwrap();
                        debug!("-> {}: got message {:?}", remote, msg);

                        let res = json_framed.send(msg).await;
                        if let Err(e) = res {
                            error!("-> {}: unable to send: {:?}", remote, e);
                            break;
                        }

                        debug!("-> {}: message sent",remote);
                    }
                }
            }
        });

        Ok(Connection {
            remote,
            messages_in: rx_message_in,
            messages_out: tx_messages_out,
        })
    }

    async fn ensure_version(
        framed: &mut tokio_util::codec::Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
    ) -> std::result::Result<(), HandshakeError> {
        let version_msg = VersionMessage {
            version: PROTOCOL_VERSION,
        };
        let version_msg_bytes = serde_json::to_vec(&version_msg).expect("unable to serialize");
        framed
            .send(Bytes::from(version_msg_bytes))
            .await
            .map_err(|_| HandshakeError::IO)?;

        let rec = framed.next().await;
        if rec.is_none() {
            return Err(HandshakeError::NoVersionReceived);
        }
        let rec = rec.unwrap();
        if let Err(_e) = rec {
            return Err(HandshakeError::IO);
        }
        let rec = rec.unwrap();
        let remote_version_msg: VersionMessage =
            serde_json::from_slice(rec.as_ref()).map_err(|_| HandshakeError::IO)?;
        debug!("got remote version message {:?}", remote_version_msg);

        if remote_version_msg.version != PROTOCOL_VERSION {
            return Err(HandshakeError::VersionMismatch);
        }

        Ok(())
    }
}
