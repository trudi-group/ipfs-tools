use ipfs_resolver_common::Result;
use prometheus::IntCounterVec;
use prometheus_exporter::PrometheusExporter;
use std::net::SocketAddr;
use std::thread;

lazy_static! {
    pub static ref ENTRIES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by monitor, entry type, and send_dont_have",
        &["monitor", "entry_type", "send_dont_have"]
    )
    .unwrap();
    pub static ref MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_messages_received",
        "number of wantlist messages received by monitor and whether the message was a full wantlist",
        &["monitor","full"]
    )
    .unwrap();
    pub static ref CONNECTION_EVENTS: IntCounterVec = register_int_counter_vec!(
        "connection_events",
        "number of connects and disconnects by monitor, event type",
        &["monitor", "connected"]
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
