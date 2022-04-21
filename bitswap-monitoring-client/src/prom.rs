use ipfs_resolver_common::Result;
use prometheus::IntCounterVec;
use prometheus_exporter::PrometheusExporter;
use std::net::SocketAddr;
use std::thread;

lazy_static! {
    pub static ref WANTLIST_ENTRIES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by monitor, entry type, and send_dont_have",
        &["monitor", "entry_type", "send_dont_have"]
    )
    .unwrap();
    pub static ref WANTLIST_MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_messages_received",
        "number of wantlist messages received by monitor and whether the message was a full wantlist",
        &["monitor","full"]
    )
    .unwrap();

    pub static ref CONNECTION_EVENTS_CONNECTED: IntCounterVec = register_int_counter_vec!(
        "connection_events_connected",
        "number of connect events by monitor",
        &["monitor"]
    )
    .unwrap();

    pub static ref CONNECTION_EVENTS_DISCONNECTED: IntCounterVec = register_int_counter_vec!(
        "connection_events_disconnected",
        "number of disconnect events by monitor",
        &["monitor"]
    )
    .unwrap();

    pub static ref BITSWAP_MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_messages_received",
        "number of bitswap messages (both requests and responses) received by monitor",
        &["monitor"]
    )
    .unwrap();

    pub static ref BITSWAP_BLOCKS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_blocks_received",
        "number of blocks received via bitswap, by monitor",
        &["monitor"]
    )
    .unwrap();

    pub static ref BITSWAP_BLOCK_PRESENCES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_block_presences_received",
        "number of block presences received via bitswap, by monitor and presence type",
        &["monitor","presence_type"]
    )
    .unwrap();
}

pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    thread::Builder::new()
        .name("prom-exporter".to_string())
        .spawn(move || {
            PrometheusExporter::run(&addr).expect("unable to start server for prometheus");
        })?;

    Ok(())
}
