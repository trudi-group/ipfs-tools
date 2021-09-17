use crate::config::MatchingConfig;
use crate::Result;
use failure::ResultExt;
use ipfs_resolver_common::wantlist;
use ipfs_resolver_common::wantlist::{CSVWantlistEntry, IngestResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// An entry in a per-peer queue.
/// These queues hold entries from multiple monitors.
/// We need to keep track of where they came from and whether they've been matched with.
#[derive(Debug, Clone)]
struct SourcedCSVWantlistEntry {
    entry: CSVWantlistEntry,
    monitor_id: usize,
    matched: bool,
}

/// Functions to calculate time differences.
/// Note that they always assume the receiver (the `SourcedCSVWantlistEntry`) to be further in the
/// past than the thing compared to.
impl SourcedCSVWantlistEntry {
    fn millis_to_timestamp(&self, ts: chrono::DateTime<chrono::Utc>) -> i64 {
        (ts.timestamp() - self.entry.timestamp_seconds) * 1000
            + (ts.timestamp_subsec_millis() as i64
                - self.entry.timestamp_subsec_milliseconds as i64)
    }

    fn millis_to_csv_entry(&self, other: &CSVWantlistEntry) -> i64 {
        debug!(
            "self.ts: {}, other.ts: {}, self.millis: {}, other.millis: {}",
            self.entry.timestamp_seconds,
            other.timestamp_seconds,
            self.entry.timestamp_subsec_milliseconds,
            other.timestamp_subsec_milliseconds
        );
        (other.timestamp_seconds - self.entry.timestamp_seconds) * 1000
            + (other.timestamp_subsec_milliseconds as i64
                - self.entry.timestamp_subsec_milliseconds as i64)
    }
}

/// A CSV entry augmented with information about the source and whether it has been matched to
/// another entry.
#[derive(Debug, Clone)]
struct MatchedCSVWantlistEntry {
    entry: CSVWantlistEntry,
    monitor_id: u64,

    inter_source_match: Option<InterSourceMatching>,
}

#[derive(Debug, Clone, Copy)]
struct InterSourceMatching {
    matched_to_monitor_id: u64,
    match_time_diff_ms: u64,
}

/// A CSV entry augmented with everything `MatchedCSVWantlistEntry` has, plus global duplicate
/// status.
#[derive(Debug, Clone)]
struct GloballyDupedMatchedCSVWantlistEntry {
    entry: MatchedCSVWantlistEntry,

    global_dup: Option<GlobalDuplicateStatus>,
}

#[derive(Debug, Clone, Copy)]
struct GlobalDuplicateStatus {
    time_since_dup_ms: u64,
}

/// The struct emitted by the global duplication and matching algorithm.
/// To be serialized to CSV.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutputCSVWantlistEntry {
    /// The ID of the monitor this entry originates from.
    pub monitor_id: u64,

    /// Whether this entry was previously seen at another monitor, and if yes: which one.
    pub matched_to_monitor_id: Option<u64>,
    /// The time difference between the matches, in milliseconds.
    pub match_time_diff_ms: Option<u64>,

    /// Whether this entry was seen at another monitor, within the global duplicate window.
    /// If yes: What's the time difference between the duplicates, in milliseconds?
    pub global_duplicate_time_diff_ms: Option<u64>,

    // ================================================
    // TODO
    // These are the exact fields of `common::wantlist::CSVWantlistEntry`.
    // It'd be nice to just [serde(flatten)] that in here, but the `csv` crate does not support
    // that (yet?).
    /// A synthetic counter which is incremented for every object in the stream of logged
    /// `JSONMessage`s.
    /// This is used to group `want_list` entries from the same message etc.
    pub message_id: i64,

    /// Message type, see `CSV_MESSAGE_TYPE_`constants.
    pub message_type: i32,

    /// Timestamp as seconds since the Unix epoch.
    pub timestamp_seconds: i64,
    /// Sub-second milliseconds of the timestamp.
    pub timestamp_subsec_milliseconds: u32,

    /// The ID of the sending peer.
    pub peer_id: String,
    /// The underlay multiaddress of the sending peer, if available.
    pub address: String,

    /// The `priority` field as was sent in the original JSON message.
    pub priority: i32,
    /// Entry type, see `CSV_ENTRY_TYPE_` constants.
    pub entry_type: i32,
    /// The human-readable CID as was sent in the original JSON message, not normalized.
    pub cid: String,

    /// Marks _request_ messages as duplicates, see `CSV_DUPLICATE_STATUS` constants.
    /// This functions as a bitfield, i.e. an entry that is both a `full_wantlist` and a `reconnect`
    /// duplicate will have `duplicate_status` set to
    /// `CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST | CSV_DUPLICATE_STATUS_DUP_RECONNECT`.
    /// This does not apply to CANCEL entries.
    pub duplicate_status: u32,
    /// Indicates the smallest sliding window that matched for a sliding window duplicate request
    /// entry. This does not apply to CANCEL entries
    pub sliding_window_smallest_match: u32,
    /// Lists the time difference in seconds between duplicate requests or request-CANCEL pairs.
    /// If the entry type is CANCEL, the latter applies.
    pub secs_since_earlier_message: u32,
    /// Indicates whether this request upgrades an earlier request.
    /// This is only valid for requests of type `WANT_BLOCK`, as they can upgrade earlier
    /// `WANT_HAVE` requests.
    pub upgrades_earlier_request: bool,
}

impl From<GloballyDupedMatchedCSVWantlistEntry> for OutputCSVWantlistEntry {
    fn from(e: GloballyDupedMatchedCSVWantlistEntry) -> Self {
        OutputCSVWantlistEntry {
            message_id: e.entry.entry.message_id,
            message_type: e.entry.entry.message_type,
            timestamp_seconds: e.entry.entry.timestamp_seconds,
            timestamp_subsec_milliseconds: e.entry.entry.timestamp_subsec_milliseconds,
            peer_id: e.entry.entry.peer_id,
            address: e.entry.entry.address,
            priority: e.entry.entry.priority,
            entry_type: e.entry.entry.entry_type,
            cid: e.entry.entry.cid,
            duplicate_status: e.entry.entry.duplicate_status,
            sliding_window_smallest_match: e.entry.entry.sliding_window_smallest_match,
            secs_since_earlier_message: e.entry.entry.secs_since_earlier_message,
            monitor_id: e.entry.monitor_id,
            matched_to_monitor_id: e.entry.inter_source_match.map(|m| m.matched_to_monitor_id),
            match_time_diff_ms: e.entry.inter_source_match.map(|m| m.match_time_diff_ms),
            global_duplicate_time_diff_ms: e.global_dup.map(|m| m.time_since_dup_ms),
            upgrades_earlier_request: false,
        }
    }
}

/// The algorithm keeping track of global duplicates and matches between monitors.
#[derive(Debug, Clone)]
pub(crate) struct InterMonitorMatcher {
    /// Configuration
    cfg: MatchingConfig,

    /// Statistics
    stats: MatcherStatistics,

    /// Maps Peer IDs to Queues of CSV entries.
    /// The queues contain past entries for that peer ID, for both monitors, ordered by timestamp.
    /// The queues do not contain synthetic entries.
    /// The queue is truncated to the appropriate window size whenever a new entry for that peer ID
    /// is processed.
    peer_queues: HashMap<String, VecDeque<SourcedCSVWantlistEntry>>,
}

/// Some statistics about entry matches between monitors.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MatcherStatistics {
    pub(crate) total_entries: usize,
    pub(crate) matched_entries: usize,
    pub(crate) min_match_diff: i64,
    pub(crate) max_match_diff: i64,
    pub(crate) match_diff_sum: f64,
}

impl Default for MatcherStatistics {
    fn default() -> Self {
        MatcherStatistics {
            total_entries: 0,
            matched_entries: 0,
            min_match_diff: std::i64::MAX,
            max_match_diff: std::i64::MIN,
            match_diff_sum: 0_f64,
        }
    }
}

impl InterMonitorMatcher {
    pub(crate) fn new_from_config(cfg: &MatchingConfig) -> Result<InterMonitorMatcher> {
        Ok(InterMonitorMatcher {
            cfg: cfg.clone(),
            stats: Default::default(),
            peer_queues: Default::default(),
        })
    }

    /// Returns stats about how entries were matched.
    pub(crate) fn stats(&self) -> MatcherStatistics {
        self.stats.clone()
    }

    /// Drives the matching and duplicate detection algorithm with the given entries.
    /// Returns the entries (minus connection events) augmented with matching and duplicate
    /// detection information.
    pub(crate) fn handle_ingest_result(
        &mut self,
        monitor_id: usize,
        ts: chrono::DateTime<chrono::Utc>,
        peer_id: String,
        ingest_result: IngestResult,
    ) -> Result<Vec<OutputCSVWantlistEntry>> {
        if let Some(conn_event) = ingest_result.connection_event {
            debug!(
                "ingest result contained connection event {:?}, ignoring...",
                conn_event
            );
        }

        let output_entries = if let Some(entries) = ingest_result.wantlist_entries {
            self.stats.total_entries += entries.len();
            let queue = self.peer_queues.entry(peer_id).or_default();
            Self::handle_entries(queue, monitor_id, entries, ts, &self.cfg, &mut self.stats)
                .context("unable to handle entries")?
                .into_iter()
                .map(|entry| OutputCSVWantlistEntry::from(entry))
                .collect()
        } else {
            Vec::default()
        };

        Ok(output_entries)
    }

    fn handle_entries(
        queue: &mut VecDeque<SourcedCSVWantlistEntry>,
        monitor_id: usize,
        entries: Vec<CSVWantlistEntry>,
        ts: chrono::DateTime<chrono::Utc>,
        cfg: &MatchingConfig,
        stats: &mut MatcherStatistics,
    ) -> Result<Vec<GloballyDupedMatchedCSVWantlistEntry>> {
        // Clear entries in peer queue older than max window size
        let max_window_size = cfg
            .inter_monitor_matching_window_milliseconds
            .max(cfg.global_duplicate_window_seconds * 1000) as i64;
        queue.retain(|e| {
            let diff_ms = e.millis_to_timestamp(ts);
            diff_ms <= max_window_size
        });
        debug!("peer message queue is {:?}", queue);

        // Try to find and mark matches between monitors.
        // This augments every entry with a possible global match.
        // The number of entries does not change.
        let matched_entries: Vec<_> = entries
            .into_iter()
            .map(|entry| Self::match_single_entry(queue, monitor_id, cfg, stats, entry))
            .collect();
        debug!("generated matched entries {:?}", matched_entries);

        // Find and mark global duplicates, i.e., duplicates in some window calculated over all
        // monitors.
        // The number of entries does not change.
        let duped_entries: Vec<_> = matched_entries
            .into_iter()
            .map(|entry| Self::search_dup_single_entry(queue, cfg, entry))
            .collect();
        debug!("generated duped entries {:?}", duped_entries);

        // Append entries to peer queue
        // We use the output entries for this so we can mark which ones were matched.
        // We filter synthetic entries, which are not to be included in the per-peer windows.
        queue.extend(
            duped_entries
                .iter()
                .filter(|&e| e.entry.entry.entry_type != wantlist::CSV_MESSAGE_TYPE_SYNTHETIC)
                .map(|e| SourcedCSVWantlistEntry {
                    entry: e.entry.entry.clone(),
                    monitor_id,
                    matched: e.entry.inter_source_match.is_some(),
                }),
        );
        debug!("peer message queue after update is {:?}", queue);

        Ok(duped_entries)
    }

    fn search_dup_single_entry(
        queue: &mut VecDeque<SourcedCSVWantlistEntry>,
        cfg: &MatchingConfig,
        entry: MatchedCSVWantlistEntry,
    ) -> GloballyDupedMatchedCSVWantlistEntry {
        debug!("searching for global duplicates for entry {:?}", entry);
        // Oldest entries are first in the queue.
        let dup = queue
            .iter()
            // Newest entries are first in the queue.
            .rev()
            // Find entries that reference the same CID.
            .filter(|&dup| dup.entry.cid == entry.entry.cid)
            // Find entries with the correct entry type.
            .filter(|&dup| {
                if cfg.match_exact_entry_type {
                    // We do not check whether the _message_type_ is the same, as there are no
                    // synthetic messages in the queue.
                    // We do not differentiate full wantlists from incremental ones.
                    dup.entry.entry_type == entry.entry.entry_type
                } else {
                    // If we don't match the exact entry type, just check whether both of them were
                    // requests or both of them were CANCELs...
                    wantlist::csv_entry_type_is_request(dup.entry.entry_type)
                        == wantlist::csv_entry_type_is_request(entry.entry.entry_type)
                }
            })
            // Calculate the time difference between entries in the queue and the new entry.
            .map(|dup| {
                let diff_ms = dup.millis_to_csv_entry(&entry.entry);
                assert!(diff_ms >= 0);
                (dup, diff_ms)
            })
            // Find the first one that fits into our window.
            // This will be the most recent matching entry because we reversed the iterator at some
            // point.
            .find(|&(_, diff_ms)| diff_ms <= cfg.global_duplicate_window_seconds as i64 * 1000);

        // Create output based on whether we found a duplicate
        GloballyDupedMatchedCSVWantlistEntry {
            entry,
            global_dup: dup.map_or_else(
                || {
                    debug!("found no matching entry");
                    None
                },
                |(dup, diff_ms)| {
                    debug!("found matching entry {:?}", dup);
                    Some(GlobalDuplicateStatus {
                        time_since_dup_ms: diff_ms as u64,
                    })
                },
            ),
        }
    }

    fn match_single_entry(
        queue: &mut VecDeque<SourcedCSVWantlistEntry>,
        monitor_id: usize,
        cfg: &MatchingConfig,
        stats: &mut MatcherStatistics,
        entry: CSVWantlistEntry,
    ) -> MatchedCSVWantlistEntry {
        debug!("searching for matches for entry {:?}", entry);

        // Search peer queue for matches from another monitor
        // (oldest are first in queue, queue only contains non-synthetic messages)
        // We will match with the first candidate while iterating.
        // If we want to match newest-first, we need to reverse the iterator.
        let it: Box<dyn Iterator<Item = &mut SourcedCSVWantlistEntry>> = if cfg.match_newest_first {
            Box::new(queue.iter_mut().rev())
        } else {
            Box::new(queue.iter_mut())
        };

        let mut matched_entry = it
            .filter(|ref dup| {
                // Do we allow matching the same entry multiple times?
                cfg.allow_multiple_match || !dup.matched
            })
            .filter(|ref dup| {
                dup.monitor_id != monitor_id
                    && dup.entry.cid == entry.cid
                    && dup.entry.message_type == entry.message_type
                    && dup.entry.entry_type == entry.entry_type
            })
            .map(|dup| {
                let diff_ms = dup.millis_to_csv_entry(&entry);
                assert!(diff_ms >= 0);

                (dup, diff_ms)
            })
            .find(|&(_, diff_ms)| diff_ms <= cfg.inter_monitor_matching_window_milliseconds as i64);

        // Mark the entry as matched
        if let Some((entry, _)) = &mut matched_entry {
            entry.matched = true
        }

        // Construct output based on our findings...
        MatchedCSVWantlistEntry {
            entry,
            monitor_id: monitor_id as u64,
            inter_source_match: matched_entry.map_or_else(
                || {
                    debug!("found no matching entry");
                    None
                },
                |(matched_entry, diff_ms)| {
                    debug!("found matching entry {:?}", matched_entry);
                    stats.matched_entries += 1;
                    Self::record_match_time_diff(diff_ms, stats);

                    Some(InterSourceMatching {
                        matched_to_monitor_id: matched_entry.monitor_id as u64,
                        match_time_diff_ms: diff_ms as u64,
                    })
                },
            ),
        }
    }

    fn record_match_time_diff(diff_ms: i64, stats: &mut MatcherStatistics) {
        if diff_ms < stats.min_match_diff {
            stats.min_match_diff = diff_ms
        }
        if diff_ms > stats.max_match_diff {
            stats.max_match_diff = diff_ms
        }
        stats.match_diff_sum += (diff_ms as f64) / 1000.0;
    }
}
