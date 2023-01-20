use failure::ResultExt;
use ipfs_resolver_common::Result;
use prometheus::IntGaugeVec;
use std::net::SocketAddr;

lazy_static! {
    pub static ref HYPERGEOM_SIZE_ESTIMATE: IntGaugeVec = register_int_gauge_vec!(
        "monitoring_size_estimator_hypergeom_size_estimate",
        "hypergeometric size estimate by protocol and monitor pair",
        &["protocol", "monitor_pair"]
    )
    .unwrap();
    pub static ref COUPON_SIZE_ESTIMATE: IntGaugeVec = register_int_gauge_vec!(
        "monitoring_size_estimator_coupon_size_estimate",
        "size estimate based on the coupon collectors problem by protocol",
        &["protocol"]
    )
    .unwrap();
}

/// Starts a thread to serve prometheus metrics.
pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    prometheus_exporter::start(addr).context("can not start exporter")?;

    Ok(())
}
