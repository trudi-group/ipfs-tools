use bytes::Buf;
use failure::err_msg;
use failure::ResultExt;
use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::prelude::*;
use ipfs_resolver_common::wantlist::{JSONWantlistEntry, JsonCID};
use ipfs_resolver_common::Result;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, Consumer, ExchangeKind};
use multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use serde_repr::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, Sender};

pub const ROUTING_KEY_PREFIX_MONITOR: &str = "monitor";
pub const ROUTING_KEY_SUFFIX_BITSWAP_MESSAGES: &str = "bitswap_messages";
pub const ROUTING_KEY_SUFFIX_CONNECTION_EVENTS: &str = "conn_events";
pub const EXCHANGE_NAME_PASSIVE_MONITORING: &str = "ipfs.passive_monitoring";

async fn connect(addr: &str) -> Result<Connection> {
    let conn = Connection::connect(addr, ConnectionProperties::default()).await?;
    Ok(conn)
}

/*
async fn set_prefetch(c: &Channel, prefetch: u16) -> Result<()> {
    c.basic_qos(prefetch, BasicQosOptions::default()).await?;
    Ok(())
}
*/

async fn set_up_exchange(c: &Channel) -> Result<()> {
    c.exchange_declare(
        EXCHANGE_NAME_PASSIVE_MONITORING,
        ExchangeKind::Topic,
        ExchangeDeclareOptions {
            passive: false,
            durable: false,
            auto_delete: false,
            internal: false,
            nowait: false,
        },
        FieldTable::default(),
    )
    .await?;
    Ok(())
}

/*
async fn publish_message(c: &Channel, payload: &[u8]) -> anyhow::Result<()> {
    c.basic_publish(
        "ipfs.bitswap",
        "",
        BasicPublishOptions {
            // Does not need to be routed anywhere (i.e., no subscribers?)
            mandatory: false,
            // Does not need to be routed immediately (i.e., backpressure? no subscribers?)
            immediate: false,
        },
        payload,
        BasicProperties::default().with_expiration(ShortString::from("60000")),
    )
        .await?;
    Ok(())
}

pub async fn post_event(c: &Channel, msg: &PushedEvent) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(&msg)?;
    publish_message(c, &payload).await?;
    Ok(())
}
 */

pub fn decode_event(payload: &[u8]) -> Result<PushedEvent> {
    let res = serde_json::from_slice(payload)?;
    Ok(res)
}

async fn set_up_queue_and_subscribe(c: &Channel, routing_keys: &[String]) -> Result<Consumer> {
    let queue = c
        .queue_declare(
            "",
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("unable to declare queue")?;

    let queue_name = queue.name();
    for routing_key in routing_keys {
        c.queue_bind(
            queue_name.as_str(),
            EXCHANGE_NAME_PASSIVE_MONITORING,
            routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .context(format!("unable to bind with routing key {}", routing_key))?;
    }

    let consumer = c
        .basic_consume(
            queue_name.as_str(),
            "",
            BasicConsumeOptions {
                no_local: false,
                no_ack: false,
                exclusive: true,
                nowait: false,
            },
            FieldTable::default(),
        )
        .await
        .context("unable to basic.consume")?;

    Ok(consumer)
}

fn decode_messages(payload: Vec<u8>) -> Result<Vec<PushedEvent>> {
    debug!("decoding {} bytes: {:x?}", payload.len(), payload);
    serde_json::from_reader(GzDecoder::new(std::io::Cursor::new(payload).reader()))
        .map_err(|err| err.into())
}

#[derive(Clone, Debug)]
pub enum RoutingKeyInformation {
    ConnectionEvents { monitor_name: String },
    BitswapMessages { monitor_name: String },
}

impl RoutingKeyInformation {
    fn to_routing_key(&self) -> String {
        match self {
            RoutingKeyInformation::ConnectionEvents { monitor_name } => {
                format!(
                    "{}.{}.{}",
                    ROUTING_KEY_PREFIX_MONITOR, monitor_name, ROUTING_KEY_SUFFIX_CONNECTION_EVENTS
                )
            }
            RoutingKeyInformation::BitswapMessages { monitor_name } => {
                format!(
                    "{}.{}.{}",
                    ROUTING_KEY_PREFIX_MONITOR, monitor_name, ROUTING_KEY_SUFFIX_BITSWAP_MESSAGES
                )
            }
        }
    }
}

fn decode_routing_key(routing_key: &str) -> Result<RoutingKeyInformation> {
    let split: Vec<_> = routing_key.split('.').collect();
    if split.len() != 3 {
        return Err(err_msg(format!(
            "expected three parts, found {}",
            split.len()
        )));
    }
    if *split.get(0).unwrap() != ROUTING_KEY_PREFIX_MONITOR {
        return Err(err_msg(format!(
            "expected prefix {}, found {}",
            ROUTING_KEY_PREFIX_MONITOR,
            split.get(0).unwrap()
        )));
    }
    match *split.get(2).unwrap() {
        ROUTING_KEY_SUFFIX_BITSWAP_MESSAGES => Ok(RoutingKeyInformation::BitswapMessages {
            monitor_name: split.get(1).unwrap().to_string(),
        }),
        ROUTING_KEY_SUFFIX_CONNECTION_EVENTS => Ok(RoutingKeyInformation::ConnectionEvents {
            monitor_name: split.get(1).unwrap().to_string(),
        }),
        _ => Err(err_msg(format!(
            "expected routing key suffix, found {}",
            split.get(2).unwrap()
        ))),
    }
}

fn encode_messages(msgs: &Vec<PushedEvent>) -> Result<Vec<u8>> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut e, &msgs)?;
    let b: Vec<u8> = e.finish()?;
    debug!("encoded {} bytes: {:x?}", b.len(), b);
    Ok(b)
}

#[derive(Debug)]
pub struct MonitoringClient {
    pub remote: String,
    chan: Channel,
    msg_in: Receiver<Result<(RoutingKeyInformation, Vec<PushedEvent>)>>,
}

impl Stream for MonitoringClient {
    type Item = Result<(RoutingKeyInformation, Vec<PushedEvent>)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.msg_in.poll_recv(cx)
    }
}

impl MonitoringClient {
    pub async fn new(
        addr: &str,
        routing_keys: &[RoutingKeyInformation],
    ) -> Result<MonitoringClient> {
        let conn = connect(addr)
            .await
            .context("unable to connect to RabbitMQ")?;

        let chan = conn
            .create_channel()
            .await
            .context("unable to set up AMQP channel")?;

        set_up_exchange(&chan)
            .await
            .context("unable to set up exchange")?;

        let consumer = set_up_queue_and_subscribe(
            &chan,
            &routing_keys
                .iter()
                .map(|k| k.to_routing_key())
                .collect::<Vec<_>>(),
        )
        .await
        .context("unable to set up queue and subscribe")?;

        let (msg_sender, msg_receiver) = tokio::sync::mpsc::channel(1);

        tokio::spawn(Self::process_incoming_messages(consumer, msg_sender));

        Ok(MonitoringClient {
            remote: addr.to_string(),
            chan,
            msg_in: msg_receiver,
        })
    }

    async fn process_incoming_messages(
        mut consumer: Consumer,
        msg_out: Sender<Result<(RoutingKeyInformation, Vec<PushedEvent>)>>,
    ) {
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Err(err) => {
                    // We ignore this error because we return immediately.
                    let _ = msg_out.send(Err(err.into())).await;
                    return;
                }
                Ok(delivery) => {
                    let Delivery {
                        data,
                        acker,
                        routing_key,
                        ..
                    } = delivery;
                    match decode_routing_key(routing_key.as_str())
                        .and_then(|key| decode_messages(data).map(|msg| (key, msg)))
                    {
                        Ok((key, msg)) => {
                            if let Err(e) = acker.ack(BasicAckOptions::default()).await {
                                // This probably means something is wrong, so let's abort.
                                error!("unable to ACK incoming delivery: {:?}", e);
                                if let Err(e) = msg_out.send(Err(e.into())).await {
                                    error!("unable to notify subscriber of error: {:?}", e);
                                }
                                return;
                            }
                            if let Err(_) = msg_out.send(Ok((key, msg))).await {
                                debug!("unable to pass on decoded message, quitting");
                                return;
                            }
                        }
                        Err(err) => {
                            error!("unable to decode incoming delivery: {:?}", err);
                            if let Err(e) = acker.nack(BasicNackOptions::default()).await {
                                error!("unable to NACK incoming delivery: {:?}", e);
                            }
                            if let Err(_) = msg_out.send(Err(err)).await {
                                error!("unable to notify subscriber of incoming error, quitting");
                            }
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// A monitoring-related event.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PushedEvent {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub peer: String,

    #[serde(flatten)]
    pub inner: EventType,
}

impl PushedEvent {
    /// Creates a constant-width identifier for this event.
    /// This is potentially expensive.
    pub fn constant_width_identifier(&self) -> String {
        // TODO it would be nice if this didn't return a string.
        // TODO I want something that implements Debug, and then formats this on the fly.
        match &self.inner {
            EventType::BitswapMessage(msg) => {
                let mut addrs = msg
                    .connected_addresses
                    .iter()
                    .map(|ma| format!("{}", ma))
                    .collect::<Vec<_>>()
                    .join(", ");
                addrs.truncate(30);
                format!("{:52} [{:30}]", self.peer, addrs)
            }
            EventType::ConnectionEvent(conn_event) => {
                format!("{:52} {:32}", self.peer, format!("{}", conn_event.remote))
            }
        }
    }
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
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BlockPresence {
    pub cid: JsonCID,
    pub block_presence_type: BlockPresenceType,
}

/// Block presence type constants for monitoring events.
#[derive(Serialize_repr, Deserialize_repr, Debug, Copy, Clone)]
#[repr(u8)]
pub enum BlockPresenceType {
    Have = 0,
    DontHave = 1,
}

/// A connection event, as reported by IPFS.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ConnectionEvent {
    pub remote: Multiaddr,
    pub connection_event_type: ConnectionEventType,
}

/// Connection event type constants for monitoring events.
#[derive(Serialize_repr, Deserialize_repr, Debug, Copy, Clone)]
#[repr(u8)]
pub enum ConnectionEventType {
    Connected = 0,
    Disconnected = 1,
}
