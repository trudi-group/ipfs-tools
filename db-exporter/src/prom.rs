use crate::Result;
use prometheus::{IntGauge, IntGaugeVec};
use prometheus_exporter::PrometheusExporter;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use ipfs_resolver_db::db;

lazy_static! {
    pub static ref RESOLVES: IntGaugeVec =
        register_int_gauge_vec!("resolves", "number of resolves by result", &["result"]).unwrap();
    pub static ref BLOCKS: IntGauge = register_int_gauge!("blocks", "number of blocks").unwrap();
    pub static ref UNIXFS_BLOCKS: IntGauge =
        register_int_gauge!("unixfs_blocks", "number of UnixFS blocks").unwrap();
}

pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    thread::Builder::new()
        .name("prom-db-stats".to_string())
        .spawn(track_db_stats)?;

    PrometheusExporter::run(&addr)?;

    Ok(())
}

fn track_db_stats() {
    let successful_resolves = RESOLVES
        .get_metric_with_label_values(&["successful"])
        .unwrap();
    let failed_resolves = RESOLVES.get_metric_with_label_values(&["failed"]).unwrap();
    let blocks = &*BLOCKS;
    let unixfs_blocks = &*UNIXFS_BLOCKS;

    loop {
        thread::sleep(Duration::from_secs(5));

        debug!("connecting to DB...");
        let conn = ipfs_resolver_db::establish_connection();
        let conn = match conn {
            Ok(conn) => conn,
            Err(err) => {
                warn!("unable to connect to DB: {:?}", err);
                continue;
            }
        };

        debug!("counting blocks..");
        match db::count_blocks(&conn) {
            Ok(count) => {
                debug!("we have {} blocks", count);
                blocks.set(count as i64);
            }
            Err(err) => warn!("unable to get block count: {:?}", err),
        }

        debug!("counting UnixFS blocks...");
        match db::count_unixfs_blocks(&conn) {
            Ok(count) => {
                debug!("we have {} UnixFS blocks", count);
                unixfs_blocks.set(count as i64);
            }
            Err(err) => warn!("unable to get UnixFS block count: {:?}", err),
        }

        debug!("counting successful resolves...");
        match db::count_successful_resolves(&conn) {
            Ok(count) => {
                debug!("we have {} successful resolves", count);
                successful_resolves.set(count as i64);
            }
            Err(err) => warn!("unable to get successful resolves count: {:?}", err),
        }

        debug!("counting failed resolves...");
        match db::count_failed_resolves(&conn) {
            Ok(count) => {
                debug!("we have {} failed resolves", count);
                failed_resolves.set(count as i64);
            }
            Err(err) => warn!("unable to get failed resolves count: {:?}", err),
        }

        debug!("done collecting DB metrics");
    }
}
