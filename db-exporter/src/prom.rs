use crate::Result;
use failure::ResultExt;
use prometheus::{IntGauge, IntGaugeVec};
use prometheus_exporter::PrometheusExporter;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use ipfs_resolver_db::db;
use prometheus::core::{AtomicI64, GenericGauge};

lazy_static! {
    pub static ref RESOLVES: IntGaugeVec =
        register_int_gauge_vec!("resolves", "number of resolves by result", &["result"]).unwrap();
    pub static ref BLOCKS: IntGauge = register_int_gauge!("blocks", "number of blocks").unwrap();
    pub static ref UNIXFS_BLOCKS: IntGauge =
        register_int_gauge!("unixfs_blocks", "number of UnixFS blocks").unwrap();
}

pub(crate) fn run_prometheus(addr: SocketAddr, scrape_interval_seconds: u32) -> Result<()> {
    let successful_resolves = RESOLVES
        .get_metric_with_label_values(&["successful"])
        .unwrap();
    let failed_resolves = RESOLVES.get_metric_with_label_values(&["failed"]).unwrap();
    let blocks = &*BLOCKS;
    let unixfs_blocks = &*UNIXFS_BLOCKS;

    debug!("getting initial DB stats to avoid polluting Prometheus with 0-data");
    loop {
        let stats = get_db_stats();
        match stats {
            Err(err) => {
                warn!(
                    "unable to collect stats: {:?}, trying again in 3 seconds..",
                    err
                );
                thread::sleep(Duration::from_secs(3));
            }
            Ok(stats) => {
                publish_stats(
                    &successful_resolves,
                    &failed_resolves,
                    blocks,
                    unixfs_blocks,
                    stats,
                );
                debug!(
                    "done, now transitioning to normal mode (starting prometheus endpoint etc.)"
                );
                break;
            }
        }
    }

    thread::Builder::new()
        .name("prom-db-stats".to_string())
        .spawn(move || track_db_stats(scrape_interval_seconds))?;

    PrometheusExporter::run(&addr)?;

    Ok(())
}

#[derive(Debug, Clone, Default)]
struct DbStats {
    num_blocks: Option<i64>,
    num_unixfs_blocks: Option<i64>,
    num_successful_resolves: Option<i64>,
    num_failed_resolves: Option<i64>,
}

fn get_db_stats() -> Result<DbStats> {
    let mut res = DbStats::default();

    debug!("connecting to DB...");
    let conn = ipfs_resolver_db::establish_connection().context("unable to connecto to DB")?;

    debug!("counting blocks..");
    match db::count_blocks(&conn) {
        Ok(count) => {
            debug!("we have {} blocks", count);
            res.num_blocks = Some(count as i64);
        }
        Err(err) => warn!("unable to get block count: {:?}", err),
    }

    debug!("counting UnixFS blocks...");
    match db::count_unixfs_blocks(&conn) {
        Ok(count) => {
            debug!("we have {} UnixFS blocks", count);
            res.num_unixfs_blocks = Some(count as i64);
        }
        Err(err) => warn!("unable to get UnixFS block count: {:?}", err),
    }

    debug!("counting successful resolves...");
    match db::count_successful_resolves(&conn) {
        Ok(count) => {
            debug!("we have {} successful resolves", count);
            res.num_successful_resolves = Some(count as i64);
        }
        Err(err) => warn!("unable to get successful resolves count: {:?}", err),
    }

    debug!("counting failed resolves...");
    match db::count_failed_resolves(&conn) {
        Ok(count) => {
            debug!("we have {} failed resolves", count);
            res.num_failed_resolves = Some(count as i64);
        }
        Err(err) => warn!("unable to get failed resolves count: {:?}", err),
    }

    Ok(res)
}

fn track_db_stats(scrape_interval_seconds: u32) {
    let successful_resolves = RESOLVES
        .get_metric_with_label_values(&["successful"])
        .unwrap();
    let failed_resolves = RESOLVES.get_metric_with_label_values(&["failed"]).unwrap();
    let blocks = &*BLOCKS;
    let unixfs_blocks = &*UNIXFS_BLOCKS;

    loop {
        debug!("sleeping {} seconds...", scrape_interval_seconds);
        thread::sleep(Duration::from_secs(scrape_interval_seconds as u64));

        debug!("collecting stats...");
        let stats = get_db_stats();
        match stats {
            Err(err) => {
                warn!("unable to get stats: {:?}", err);
                continue;
            }
            Ok(stats) => {
                publish_stats(
                    &successful_resolves,
                    &failed_resolves,
                    blocks,
                    unixfs_blocks,
                    stats,
                );
            }
        }
        debug!("done collecting DB metrics");
    }
}

fn publish_stats(
    successful_resolves: &GenericGauge<AtomicI64>,
    failed_resolves: &GenericGauge<AtomicI64>,
    blocks: &GenericGauge<AtomicI64>,
    unixfs_blocks: &GenericGauge<AtomicI64>,
    stats: DbStats,
) {
    match stats.num_blocks {
        Some(num) => blocks.set(num),
        None => {}
    }

    match stats.num_unixfs_blocks {
        Some(num) => unixfs_blocks.set(num),
        None => {}
    }

    match stats.num_successful_resolves {
        Some(num) => successful_resolves.set(num),
        None => {}
    }

    match stats.num_failed_resolves {
        Some(num) => failed_resolves.set(num),
        None => {}
    }
}
