use failure::ResultExt;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

use crate::Result;

/// Configuration file for bitswap monitoring client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    /// Specifies on what address a prometheus endpoint will be created.
    pub(crate) prometheus_address: String,

    /// Specifies the monitors to connect to
    pub(crate) monitors: Vec<MonitorConfig>,

    /// The time to sleep between size estimations, in seconds.
    pub(crate) sample_interval_seconds: u64,
}

/// Configuration for a single data source.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MonitorConfig {
    /// The name of the monitor.
    pub(crate) name: String,

    /// The address of the plugin API.
    pub(crate) plugin_api_address: String,
}

impl Config {
    /// Reads a Config from a given path.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Config> {
        let f = File::open(path).context("unable to open file")?;

        let config = serde_yaml::from_reader(f).context("unable to deserialize config")?;

        Ok(config)
    }
}
