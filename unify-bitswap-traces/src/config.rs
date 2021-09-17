use crate::Result;
use failure::ResultExt;
use ipfs_resolver_common::wantlist;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

/// Configuration file for the bitswap trace unification tool.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    /// Configures which sources to unify.
    ///
    /// The monitors configured here will be given numeric IDs in the order they are provided in
    /// the config file.
    pub(crate) monitors: Vec<MonitorSourceConfig>,

    /// The size of the sliding window (in number of JSON messages) to use for sorting the streams
    /// of per-monitor messages.
    pub(crate) message_sorting_window_size: usize,

    /// A pattern for output file paths.
    /// The pattern must contain "$id$", which will be replaced by the ID (number) of the first
    /// message in this file, formatted in such a way that the paths are lexicographically ordered.
    /// This is used in order not to produce one gigantic output file, but rather a bunch of smaller
    /// ones.
    ///
    /// The output file will be gzipped CSV.
    ///
    /// Example: output/part-$id$.csv.gz
    pub(crate) wantlist_output_file_pattern: String,

    /// Currently unused.
    ///
    /// The path for a file to record something about ledgers in.
    ///
    /// The file will be gzipped CSV.
    pub(crate) ledger_count_output_file: String,

    /// Configuration for the global matching and deduplication algorithm.
    pub(crate) matching_config: MatchingConfig,

    /// Configuration for the single-monitor bitswap simulations.
    pub(crate) simulation_config: wantlist::EngineSimulationConfig,
}

impl Config {
    /// Reads a Config from a given path.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Config> {
        let f = File::open(path).context("unable to open file")?;

        let config = serde_yaml::from_reader(f).context("unable to deserialize config")?;

        Ok(config)
    }
}

/// Configuration for a single monitor.
/// This mostly defines which trace files to read (and in what order).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MonitorSourceConfig {
    /// The name of the monitor.
    /// This is only printed once, at program start.
    /// In the CSV files, monitors are assigned ascending numeric IDs, probably beginning at zero.
    /// The order is kept the same as in the config.
    pub(crate) monitor_name: String,

    /// The globs defining the input files.
    /// These are expanded in order and then processed in that order.
    /// In our setup, files are named in such a way that they order lexicographically over time,
    /// i.e., ordered by date and time of day.
    pub(crate) input_globs: Vec<String>,
}

/// Configuration for the global matching algorithms.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct MatchingConfig {
    /// The window size, in milliseconds, in which entries can be matched between monitors.
    pub(crate) inter_monitor_matching_window_milliseconds: u64,

    /// The window size, in seconds, within which entries will be marked as inter-monitor
    /// duplicates.
    pub(crate) global_duplicate_window_seconds: u64,

    /// Whether to match an incoming entry with the newest (= most recent) entry in the window.
    pub(crate) match_newest_first: bool,

    /// Whether to allow matching an entry in the window multiple times.
    pub(crate) allow_multiple_match: bool,

    /// Whether to match based on the exact entry type, or just the general request status.
    ///
    /// If this is set, entries are only matched between monitors if their entry type is exactly
    /// the same.
    /// This includes the `SEND_DONT_WANT` flag and whether a request was `WANT_HAVE` or `WANT_BLOCK`.
    ///
    /// If this is unset, entries are matched base on request status, i.e., all request-type entries
    /// are matched to each other.
    /// Request-type entries exclude `CANCEL`s, which are only matched to other `CANCEL`s.
    ///
    /// Synthetic entries are never matched.
    ///
    /// Matching does not differentiate between full and incremental wantlists.
    /// TODO maybe we should? Unclear...
    pub(crate) match_exact_entry_type: bool,
}
