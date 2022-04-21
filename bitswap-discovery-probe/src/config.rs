use failure::ResultExt;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

use crate::Result;

/// Configuration file for bitswap monitoring client.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    /// Configures what monitors to connect to.
    pub(crate) monitors: Vec<MonitorConfig>,

    /// Specifies a list of CIDs to probe for.
    pub(crate) cids: Vec<String>,

    /// Specifies the duration between WANT and CANCEL in seconds.
    pub(crate) cancel_after_seconds: u32,

    /// Specifies how long to wait after the CANCEL broadcast for late responses.
    pub(crate) wait_after_cancel_seconds: u32,
}

/// Configuration for a single monitor to connect to.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MonitorConfig {
    /// The name of the monitor.
    pub(crate) name: String,

    /// The address of the TCP endpoint of this monitor, for bitswap monitoring.
    pub(crate) monitoring_address: String,

    /// The base URL for the HTTP API of this monitor.
    pub(crate) api_base_url: String,
}

impl Config {
    /// Reads a Config from a given path.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Config> {
        let f = File::open(path).context("unable to open file")?;

        let config = serde_yaml::from_reader(f).context("unable to deserialize config")?;

        Ok(config)
    }
}
