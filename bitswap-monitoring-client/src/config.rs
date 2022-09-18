use failure::ResultExt;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

use crate::Result;

/// Configuration file for bitswap monitoring client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    /// Configures what monitors to connect to.
    pub(crate) monitors: Vec<MonitorConfig>,

    /// Specifies on what address a prometheus endpoint will be created.
    pub(crate) prometheus_address: String,

    /// Specifies where MaxMind GeoLite databases are located.
    /// Defaults to /usr/local/share/GeoIP if unspecified.
    #[serde(default = "default_geoip_database_path")]
    pub(crate) geoip_database_path: String,

    /// Specifies the location of the public gateway ID file.
    /// Each line in the file should contain one peer ID.
    /// If not provided, all traffic will be logged as non-gateway traffic.
    pub(crate) gateway_file_path: Option<String>,
}

fn default_geoip_database_path() -> String {
    "/usr/local/share/GeoIP".to_string()
}

/// Configuration for a single monitor to connect to.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
