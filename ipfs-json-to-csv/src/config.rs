use failure::ResultExt;
use ipfs_resolver_common::{wantlist, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    input_globs: Vec<String>,
    pub(crate) wantlist_output_file_pattern: String,
    pub(crate) connection_events_output_file: String,
    pub(crate) connection_duration_output_file: String,
    pub(crate) simulation_config: wantlist::EngineSimulationConfig,
}

impl Config {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Config> {
        let f = File::open(path).context("unable to open file")?;

        let config = serde_yaml::from_reader(f).context("unable to deserialize config")?;

        Ok(config)
    }

    pub(crate) fn glob_results(&self) -> Result<Vec<PathBuf>> {
        let input_globs = self.input_globs.clone();
        let mut entries = Vec::new();

        for pattern in input_globs {
            let ent: std::result::Result<Vec<PathBuf>, _> =
                glob::glob(&pattern).context("invalid glob")?.collect();
            entries.extend(ent.context("unable to traverse path")?.into_iter());
        }

        Ok(entries)
    }
}
