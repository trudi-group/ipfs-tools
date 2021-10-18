use ipfs_resolver_common::Result;
use prometheus::IntCounterVec;
use prometheus_exporter::PrometheusExporter;
use std::net::SocketAddr;
use std::thread;

lazy_static! {
    pub static ref ENTRIES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by monitor, message type, and send_dont_have",
        &["monitor", "message_type", "send_dont_have"]
    )
    .unwrap();
    pub static ref MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_messages_received",
        "number of wantlist messages received by monitor",
        &["monitor"]
    )
    .unwrap();
    pub static ref CONNECTION_EVENTS: IntCounterVec = register_int_counter_vec!(
        "connection_events",
        "number of connects and disconnects by monitor, event type, and whether there was a ledger present for the peer",
        &["monitor", "connected", "peer_found"]
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
