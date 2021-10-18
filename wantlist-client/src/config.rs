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

    /// Specifies on what address a prometheus endpoint will be created.
    pub(crate) prometheus_address: String,
}

/// Configuration for a single monitor to connect to.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MonitorConfig {
    /// The name of the monitor.
    pub(crate) name: String,

    /// The address of the TCP endpoint of this monitor.
    pub(crate) address: String,
}

impl Config {
    /// Reads a Config from a given path.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Config> {
        let f = File::open(path).context("unable to open file")?;

        let config = serde_yaml::from_reader(f).context("unable to deserialize config")?;

        Ok(config)
    }
}
