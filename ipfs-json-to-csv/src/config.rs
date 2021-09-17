use failure::ResultExt;
use ipfs_resolver_common::{wantlist, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) input_globs: Vec<String>,
    pub(crate) wantlist_output_file_pattern: String,
    pub(crate) connection_events_output_file: String,
    pub(crate) connection_duration_output_file: String,
    pub(crate) ledger_count_output_file: String,
    pub(crate) simulation_config: wantlist::EngineSimulationConfig,
}

impl Config {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Config> {
        let f = File::open(path).context("unable to open file")?;

        let config = serde_yaml::from_reader(f).context("unable to deserialize config")?;

        Ok(config)
    }

    pub(crate) fn glob_results(&self) -> Result<Vec<PathBuf>> {
        ipfs_resolver_common::expand_globs(&self.input_globs)
    }
}
