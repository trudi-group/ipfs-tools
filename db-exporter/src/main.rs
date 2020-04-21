#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

use failure::{Error, ResultExt};
use std::env;

mod logging;
mod prom;

pub(crate) type Result<T> = std::result::Result<T, Error>;

fn main() -> Result<()> {
    logging::set_up_logging(false)?;

    debug!("reading .env...");
    dotenv::dotenv().ok();

    debug!("connecting to DB...");
    ipfs_resolver_db::establish_connection()?;

    let listen_addr = env::var("DB_EXPORTER_PROMETHEUS_LISTEN_ADDR")
        .context("DB_EXPORTER_PROMETHEUS_LISTEN_ADDR must be set")?
        .parse()
        .expect("invalid DB_EXPORTER_PROMETHEUS_LISTEN_ADDR");

    info!("starting prometheus stuff..");
    prom::run_prometheus(listen_addr)?;

    Ok(())
}
