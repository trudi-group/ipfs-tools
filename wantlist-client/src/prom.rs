use ipfs_resolver_common::Result;
use prometheus::IntGauge;
use prometheus::IntGaugeVec;
use prometheus_exporter::PrometheusExporter;
use std::net::SocketAddr;
use std::thread;

lazy_static! {
    pub static ref ENTRIES_RECEIVED: IntGaugeVec = register_int_gauge_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by type and send_dont_have",
        &["message_type", "send_dont_have"]
    )
    .unwrap();
    pub static ref MESSAGES_RECEIVED: IntGauge = register_int_gauge!(
        "wantlist_messages_received",
        "number of wantlist messages received"
    )
    .unwrap();
    pub static ref CONNECTION_EVENTS: IntGaugeVec = register_int_gauge_vec!(
        "connection_events",
        "number of connects and disconnects by type and whether the peer was already found",
        &["connected", "peer_found"]
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
