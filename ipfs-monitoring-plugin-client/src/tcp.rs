use bytes::{Buf, Bytes, BytesMut};
use failure::Fail;
use failure::ResultExt;
use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::prelude::*;
use ipfs_resolver_common::wantlist::{JSONWantlistEntry, JsonCID};
use ipfs_resolver_common::Result;
use multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio_serde::{Deserializer, Serializer};

#[derive(Debug)]
pub struct MonitoringClient {
    pub remote: SocketAddr,
    conn: Connection,
}

impl Stream for MonitoringClient {
    type Item = Result<PushedEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.conn.poll_next_unpin(cx) {
            Poll::Ready(msg) => match msg {
                None => Poll::Ready(None),
                Some(msg) => Poll::Ready(Some(msg.map(|m| {
                    debug!("{}: received message {:?}", self.remote, m);
                    let IncomingTCPMessage::Event(e) = m;
                    e
                }))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl MonitoringClient {
    pub async fn new(conn: TcpStream) -> Result<MonitoringClient> {
        let conn = Connection::new(conn)
            .await
            .context("unable to create connection")?;

        Ok(MonitoringClient {
            remote: conn.remote.clone(),
            conn,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct VersionMessage {
    version: i32,
}

const PROTOCOL_VERSION: i32 = 3;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IncomingTCPMessage {
    #[serde(rename = "event")]
    Event(PushedEvent),
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
    pub connected_addresses: Vec<Multiaddr>,
}

/// Block presence indication, as contained in a Bitswap message.
/// See the `TCP_BLOCK_PRESENCE_TYPE_` constants for the possible types.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BlockPresence {
    pub cid: JsonCID,
    pub block_presence_type: i32,
}

/// Block presence type constants for TCP messages.
pub const BLOCK_PRESENCE_TYPE_HAVE: i32 = 0;
pub const BLOCK_PRESENCE_TYPE_DONT_HAVE: i32 = 1;

/// A connection event, as reported by IPFS.
/// See the `TCP_CONN_EVENT_` constants for the types of connection events.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ConnectionEvent {
    pub remote: Multiaddr,
    pub connection_event_type: i32,
}

/// Connection event type constants for TCP messages.
pub const CONN_EVENT_CONNECTED: i32 = 0;
pub const CONN_EVENT_DISCONNECTED: i32 = 1;

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

/// A codec for gzipped JSON.
#[derive(Debug)]
pub struct GzippedJson<Item, SinkItem> {
    ghost: PhantomData<(Item, SinkItem)>,
}

impl<Item, SinkItem> Default for GzippedJson<Item, SinkItem> {
    fn default() -> Self {
        GzippedJson { ghost: PhantomData }
    }
}

impl<Item, SinkItem> Deserializer<Item> for GzippedJson<Item, SinkItem>
where
    for<'a> Item: Deserialize<'a>,
{
    type Error = serde_json::Error;

    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> std::result::Result<Item, Self::Error> {
        debug!("read {} bytes: {:x?}", src.len(), src.as_ref());
        serde_json::from_reader(GzDecoder::new(std::io::Cursor::new(src).reader()))
    }
}

impl<Item, SinkItem> Serializer<SinkItem> for GzippedJson<Item, SinkItem>
where
    SinkItem: Serialize,
{
    type Error = std::io::Error;

    fn serialize(self: Pin<&mut Self>, item: &SinkItem) -> std::result::Result<Bytes, Self::Error> {
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        serde_json::to_writer(&mut e, item)?;
        let b: Bytes = e.finish().map(Into::into)?;
        debug!("encoded {} bytes: {:x?}", b.len(), b.as_ref());
        Ok(b)
    }
}

/// A connection implements receiving JSON messages from a bitswap logging monitor.
/// The messages are read from the underlying TCP socket within length-delimited frames and then
/// JSON-decoded.
/// This is relatively low-level, you might want to use the `MonitoringClient` instead.
#[derive(Debug)]
pub struct Connection {
    pub remote: SocketAddr,
    conn: tokio_serde::Framed<
        tokio_util::codec::Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
        IncomingTCPMessage,
        (),
        GzippedJson<IncomingTCPMessage, ()>,
    >,
}

impl Stream for Connection {
    type Item = Result<IncomingTCPMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.conn.poll_next_unpin(cx) {
            Poll::Ready(msg) => {
                //debug!("<- {}: got message {:?}", remote, msg);
                Poll::Ready(msg.map(|m| m.map_err(|e| e.context("unable to receive").into())))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Connection {
    pub async fn new(conn: TcpStream) -> Result<Connection> {
        let remote = conn.peer_addr()?;

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
        let json_framed =
            tokio_serde::Framed::new(framed, GzippedJson::<IncomingTCPMessage, ()>::default());

        Ok(Connection {
            remote,
            conn: json_framed,
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
