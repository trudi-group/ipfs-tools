use crate::Result;
use failure::{err_msg, ResultExt};
use parity_multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use serde_repr::*;
use std::collections::HashMap;
use std::mem;

/// Constants for the `want_type` field of a `JSONWantlistEntry`.
#[derive(Serialize_repr, Deserialize_repr, Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum JSONWantType {
    Block = 0,
    Have = 1,
}

/// The JSON encoding of a CID.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct JsonCID {
    #[serde(rename = "/")]
    pub path: String,
}

/// A single entry of a wantlist message.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct JSONWantlistEntry {
    #[serde(alias = "Priority")]
    pub priority: i32,
    #[serde(alias = "Cancel")]
    pub cancel: bool,
    #[serde(alias = "SendDontHave")]
    pub send_dont_have: bool,
    #[serde(alias = "Cid")]
    pub cid: JsonCID,
    #[serde(alias = "WantType")]
    pub want_type: JSONWantType,
}

/// A wantlist message.
/// These are produced by the modified Go implementation of IPFS.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JSONMessage {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub peer: String,
    pub address: Option<Multiaddr>,
    pub received_entries: Option<Vec<JSONWantlistEntry>>,
    pub full_want_list: Option<bool>,
    pub peer_connected: Option<bool>,
    pub peer_disconnected: Option<bool>,
    pub connect_event_peer_found: Option<bool>,
}

/// Message type constants for CSV files.
pub const CSV_MESSAGE_TYPE_INCREMENTAL: i32 = 1;
pub const CSV_MESSAGE_TYPE_FULL: i32 = 2;
pub const CSV_MESSAGE_TYPE_SYNTHETIC: i32 = 3;

/// Entry type constants for CSV files.
pub const CSV_ENTRY_TYPE_CANCEL: i32 = 1;
pub const CSV_ENTRY_TYPE_WANT_BLOCK: i32 = 2;
pub const CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE: i32 = 3;
pub const CSV_ENTRY_TYPE_WANT_HAVE: i32 = 4;
pub const CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE: i32 = 5;
pub const CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_FULL_WANTLIST: i32 = 6;
pub const CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_DISCONNECT: i32 = 7;
pub const CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_END_OF_SIMULATION: i32 = 8;

pub fn csv_entry_type_is_request(entry_type: i32) -> bool {
    match entry_type {
        CSV_ENTRY_TYPE_CANCEL
        | CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_FULL_WANTLIST
        | CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_DISCONNECT
        | CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_END_OF_SIMULATION => false,
        CSV_ENTRY_TYPE_WANT_BLOCK
        | CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE
        | CSV_ENTRY_TYPE_WANT_HAVE
        | CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE => true,
        _ => panic!("invalid entry type {}", entry_type),
    }
}

/// Connection event type constants for CSV files.
pub const CSV_CONNECTION_EVENT_CONNECTED_FOUND: i32 = 1;
pub const CSV_CONNECTION_EVENT_CONNECTED_NOT_FOUND: i32 = 2;
pub const CSV_CONNECTION_EVENT_DISCONNECTED_FOUND: i32 = 3;
pub const CSV_CONNECTION_EVENT_DISCONNECTED_NOT_FOUND: i32 = 4;

/// Reasons for duplicate messages.
/// These function as a bitfield.
pub const CSV_DUPLICATE_STATUS_NO_DUP: u32 = 0;
pub const CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST: u32 = 1;
pub const CSV_DUPLICATE_STATUS_DUP_RECONNECT: u32 = 2;
pub const CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW: u32 = 4;

// TODO keep this in sync with `unification::matcher::OutputCSVWantlistEntry`.
/// A wantlist entry, to be serialized as CSV.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CSVWantlistEntry {
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

impl CSVWantlistEntry {
    pub fn from_wantlist_entries(
        entries: Vec<WantlistEntry>,
        message: &JSONMessage,
        id: i64,
        message_type: i32,
        entry_type: i32,
        duplicate_status: u32,
        sliding_window_smallest_match: u32,
    ) -> Vec<CSVWantlistEntry> {
        let message_timestamp_seconds = message.timestamp.timestamp();
        let message_timestamp_subsec_millis = message.timestamp.timestamp_subsec_millis();

        entries
            .into_iter()
            .map(|e| CSVWantlistEntry {
                message_id: id,
                message_type,
                timestamp_seconds: message_timestamp_seconds,
                timestamp_subsec_milliseconds: message_timestamp_subsec_millis,
                peer_id: message.peer.clone(),
                address: match &message.address {
                    Some(address) => address.to_string(),
                    None => "".to_string(),
                },
                priority: 0,
                entry_type,
                cid: e.cid,
                duplicate_status,
                sliding_window_smallest_match,
                secs_since_earlier_message: 0,
                upgrades_earlier_request: false,
            })
            .collect()
    }

    pub fn from_json_message(message: JSONMessage, id: i64) -> Result<Vec<CSVWantlistEntry>> {
        let entries = message
            .received_entries
            .ok_or_else(|| err_msg("no entries when converting from JSON message"))?;
        let peer = message.peer.clone();
        let timestamp = message.timestamp;
        let timestamp_seconds = timestamp.timestamp();
        let timestamp_subsec_millis = timestamp.timestamp_subsec_millis();
        let full_want_list = message.full_want_list;
        let address = match &message.address {
            Some(address) => address.to_string(),
            None => "".to_string(),
        };

        let csv_entries = entries
            .into_iter()
            .map(|entry| CSVWantlistEntry {
                message_id: id,
                message_type: match full_want_list {
                    Some(full) => {
                        if full {
                            CSV_MESSAGE_TYPE_FULL
                        } else {
                            CSV_MESSAGE_TYPE_INCREMENTAL
                        }
                    }
                    None => {
                        // TODO forbid this
                        CSV_MESSAGE_TYPE_INCREMENTAL
                    }
                },
                timestamp_seconds,
                timestamp_subsec_milliseconds: timestamp_subsec_millis,
                peer_id: peer.clone(),
                address: address.clone(),
                priority: entry.priority,
                entry_type: if entry.cancel {
                    CSV_ENTRY_TYPE_CANCEL
                } else {
                    match entry.want_type {
                        JSONWantType::Block => {
                            if entry.send_dont_have {
                                CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE
                            } else {
                                CSV_ENTRY_TYPE_WANT_BLOCK
                            }
                        }
                        JSONWantType::Have => {
                            if entry.send_dont_have {
                                CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE
                            } else {
                                CSV_ENTRY_TYPE_WANT_HAVE
                            }
                        }
                    }
                },
                cid: entry.cid.path,
                duplicate_status: CSV_DUPLICATE_STATUS_NO_DUP,
                sliding_window_smallest_match: 0,
                secs_since_earlier_message: 0,
                upgrades_earlier_request: false,
            })
            .collect();

        Ok(csv_entries)
    }
}

/// A connection event, to be serialized as CSV.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CSVConnectionEvent {
    /// A synthetic counter which is incremented for every object in the stream of logged
    /// `JSONMessage`s.
    /// This is used to group want_list entries from the same message etc.
    pub message_id: i64,

    /// Timestamp as seconds since the Unix epoch.
    pub timestamp_seconds: i64,
    /// Sub-second milliseconds of the timestamp.
    pub timestamp_subsec_millis: u32,

    /// The human-readable ID of the sending peer.
    pub peer_id: String,
    /// The underlay multiaddress of the sending peer, if available.
    pub address: String,
    /// The type of the connection event, see the `CSV_CONNECTION_TYPE_` constants.
    pub event_type: i32,
}

impl CSVConnectionEvent {
    pub fn from_json_message(message: JSONMessage, id: i64) -> Result<CSVConnectionEvent> {
        let found = message
            .connect_event_peer_found
            .ok_or_else(|| err_msg("connection event is missing found"))?;
        let disconnected = message
            .peer_disconnected
            .ok_or_else(|| err_msg("connection event is missing peer_disconnected"))?;
        let connected = message
            .peer_connected
            .ok_or_else(|| err_msg("connection event is missing peer_connected"))?;
        ensure!(
            disconnected || connected,
            "connection event needs either connected or disconnected to be set..."
        );
        ensure!(
            !(disconnected && connected),
            "connection event needs exactly one of connected or disconnected to be set"
        );

        let event_type = if disconnected {
            if found {
                CSV_CONNECTION_EVENT_DISCONNECTED_FOUND
            } else {
                CSV_CONNECTION_EVENT_DISCONNECTED_NOT_FOUND
            }
        } else {
            if found {
                CSV_CONNECTION_EVENT_CONNECTED_FOUND
            } else {
                CSV_CONNECTION_EVENT_CONNECTED_NOT_FOUND
            }
        };

        Ok(CSVConnectionEvent {
            message_id: id,
            timestamp_seconds: message.timestamp.timestamp(),
            timestamp_subsec_millis: message.timestamp.timestamp_subsec_millis(),
            peer_id: message.peer,
            address: match &message.address {
                Some(address) => address.to_string(),
                None => "".to_string(),
            },
            event_type,
        })
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
enum WantType {
    Block,
    Have,
}

impl WantType {
    fn from_json_entry(e: &JSONWantlistEntry) -> WantType {
        match e.want_type {
            JSONWantType::Block => WantType::Block,
            JSONWantType::Have => WantType::Have,
        }
    }

    fn from_csv_entry(e: &CSVWantlistEntry) -> WantType {
        match e.entry_type {
            CSV_ENTRY_TYPE_WANT_BLOCK | CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE => WantType::Block,
            CSV_ENTRY_TYPE_WANT_HAVE | CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE => WantType::Have,
            _ => panic!("attempted to create WantType from weird CSV entry: {:?}", e),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WantlistEntry {
    cid: String,
    want_type: WantType,
    ts: chrono::DateTime<chrono::Utc>,
}

/// A ledger keeps track of the entries WANTed by a peer, and some metadata about connection status
/// and timestamps.
#[derive(Clone, Debug)]
pub struct Ledger {
    /// A counter for parallel connections.
    /// In the optimal case this is always zero or one, but IPFS misreports events sometimes.
    /// We assert that this is never less than zero.
    connection_count: i32,

    /// The entries currently WANTed by this peer, sorted by CID.
    /// This is checked to not contain duplicates.
    wanted_entries: Vec<WantlistEntry>,

    /// The entries WANTed by this peer immediately before we got disconnected.
    wanted_entries_before_disconnect: Option<Vec<WantlistEntry>>,

    /// The beginning of the current overlay session, if we are connected.
    connected_ts: Option<chrono::DateTime<chrono::Utc>>,
}

/// The configuration of our engine simulation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EngineSimulationConfig {
    /// Whether to allow the `full_want_list` field to be unspecified in JSON messages.
    /// This is for historic reasons, as we did not track this field in the very beginning of our
    /// recordings.
    /// For anything remotely new, this should be set to `false`.
    pub allow_empty_full_wantlist: bool,

    /// Whether to allow the `peer_connected` and `peer_disconnected` fields to be unspecified in
    /// JSON messages.
    /// This should usually be set to `false` when dealing with modern recordings.
    pub allow_empty_connection_event: bool,

    /// Whether to emit `SYNTHETIC_CANCEL` records for entries "canceled" through a `full_wantlist`.
    pub insert_full_wantlist_synth_cancels: bool,
    /// Whether to emit `SYNTHETIC_CANCEL` records for entries "canceled" through the peer
    /// disconnecting.
    pub insert_disconnect_synth_cancels: bool,

    /// The number of seconds after a peer reconnects to us to consider for marking messages as
    /// duplicates.
    /// This is relevant in order to judge whether a peer has longstanding WANTed entries which are
    /// resent on a reconnect.
    pub reconnect_duplicate_duration_secs: u32,

    /// The window sizes in seconds to sort duplicate entries into.
    /// These will eventually be sorted.
    pub sliding_window_lengths: Vec<u32>,
}

/// A simulation of the BitSwap engine as was present in v0.5 of the Go IPFS client.
#[derive(Clone, Debug, Default)]
pub struct EngineSimulation {
    peers: HashMap<String, Ledger>,
    cfg: EngineSimulationConfig,
}

impl EngineSimulation {
    /// Returns the number of ledgers simulated in total up to now.
    pub fn num_ledgers(&self) -> usize {
        self.peers.len()
    }
}

#[derive(Clone, Debug, Default)]
pub struct IngestResult {
    pub missing_ledger: bool,
    pub wantlist_entries: Option<Vec<CSVWantlistEntry>>,
    pub connection_event: Option<CSVConnectionEvent>,
}

impl EngineSimulation {
    pub fn new(mut cfg: EngineSimulationConfig) -> Result<EngineSimulation> {
        cfg.sliding_window_lengths.sort();
        if cfg.sliding_window_lengths.len() > 0 {
            ensure!(
                cfg.sliding_window_lengths[0] > 0,
                "sliding windows must be >0"
            )
        }
        Ok(EngineSimulation {
            peers: HashMap::new(),
            cfg,
        })
    }

    /// Generates end-of-simulation synthetic CANCEL entries.
    pub fn generate_end_of_simulation_entries(
        self,
        ts: chrono::DateTime<chrono::Utc>,
        msg_id: i64,
    ) -> Vec<CSVWantlistEntry> {
        let ts_secs = ts.timestamp();
        let ts_subsec_milliseconds = ts.timestamp_subsec_millis();
        self.peers
            .into_iter()
            .map(|(peer_id, ledger)| {
                ledger
                    .wanted_entries
                    .into_iter()
                    .map(move |e| CSVWantlistEntry {
                        message_id: msg_id,
                        message_type: CSV_MESSAGE_TYPE_SYNTHETIC,
                        timestamp_seconds: ts_secs,
                        timestamp_subsec_milliseconds: ts_subsec_milliseconds,
                        peer_id: peer_id.clone(),
                        address: "".to_string(),
                        priority: 0,
                        entry_type: CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_END_OF_SIMULATION,
                        cid: e.cid,
                        duplicate_status: CSV_DUPLICATE_STATUS_NO_DUP,
                        sliding_window_smallest_match: 0,
                        secs_since_earlier_message: 0,
                        upgrades_earlier_request: false,
                    })
            })
            .flatten()
            .collect()
    }

    fn ingest_wantlist_message(
        &mut self,
        msg: &JSONMessage,
        entries: &Vec<JSONWantlistEntry>,
        msg_id: i64,
    ) -> Result<IngestResult> {
        let mut missing_ledger = false;
        let ledger = self.peers.entry(msg.peer.clone()).or_insert_with(|| {
            debug!("received wantlist from {} ({:?}), but don't have a ledger for that peer. Starting empty one with one connection.", msg.peer, msg.address);
            missing_ledger = true;
            Ledger { connection_count: 1, wanted_entries: Default::default(), wanted_entries_before_disconnect: Default::default(), connected_ts: Some(msg.timestamp.clone()) }
        });
        if ledger.connection_count == 0 {
            warn!("got wantlist entries from peer {}, but we are still disconnected from that peer (was previously connected). This is either an error in how IPFS notifies about connection events, or in how we ingest them.", msg.peer);
            ledger.connection_count = 1;
            ledger.connected_ts = Some(msg.timestamp.clone());
        }
        assert!(ledger.connection_count > 0);

        let (mut full_wl_dups, mut full_wl_synth_cancels) = (None, None);
        let (new_wants, new_cancels) = Self::split_wants_cancels(&entries);

        // Compute entry time differences and upgrade statuses between the new message and the
        // existing ledger.
        let offsets_since_earlier_messages =
            Self::get_secs_until_earlier_message_with_same_cid_and_want_type(
                ledger,
                msg.timestamp.clone(),
                &new_wants,
                &new_cancels,
            );
        let upgrade_statuses =
            Self::get_upgraded_status_for_same_cid(ledger, &new_wants, &new_cancels);
        let offsets_since_request_for_cancels = Self::calculate_secs_since_request_for_cancels(
            ledger,
            &new_cancels,
            msg.timestamp.clone(),
        );

        // Now update the ledger.
        match &msg.full_want_list {
            Some(full) => match full {
                true => {
                    let new_wants = new_wants
                        .iter()
                        .map(|c| WantlistEntry {
                            cid: c.cid.path.clone(),
                            ts: msg.timestamp.clone(),
                            want_type: WantType::from_json_entry(c),
                        })
                        .collect();
                    let old_wants = mem::replace(&mut ledger.wanted_entries, new_wants);
                    ledger
                        .wanted_entries
                        .sort_unstable_by(|e1, e2| e1.cid.cmp(&e2.cid));
                    ensure!(
                        !Self::check_ledger_for_duplicates(&ledger.wanted_entries),
                        "ledger contains duplicates"
                    );

                    let (full_wl_dups_t, full_wl_synth_cancels_t) =
                        Self::get_duplicate_entries_and_synth_cancels_from_full_wantlist(
                            old_wants,
                            &ledger.wanted_entries,
                        );
                    full_wl_dups = full_wl_dups_t;
                    if self.cfg.insert_full_wantlist_synth_cancels {
                        full_wl_synth_cancels = full_wl_synth_cancels_t;
                    }
                }
                false => {
                    Self::apply_new_entries(
                        &mut ledger.wanted_entries,
                        new_wants,
                        new_cancels,
                        &msg.peer,
                        msg.timestamp.clone(),
                    )?;
                }
            },
            None => {
                if self.cfg.allow_empty_full_wantlist {
                    debug!("got empty full_want_list, assuming incremental.");
                    Self::apply_new_entries(
                        &mut ledger.wanted_entries,
                        new_wants,
                        new_cancels,
                        &msg.peer,
                        msg.timestamp.clone(),
                    )?;
                } else {
                    error!("got empty full_want_list: {:?}", msg);
                    return Err(err_msg("got empty full_want_list, should be set"));
                }
            }
        }

        let reconnect_dups = Self::get_duplicate_entries_from_reconnect(
            ledger,
            msg.timestamp.clone(),
            self.cfg.reconnect_duplicate_duration_secs,
        );

        let synth_cancels = full_wl_synth_cancels.map(|cs| {
            CSVWantlistEntry::from_wantlist_entries(
                cs,
                &msg,
                msg_id,
                CSV_MESSAGE_TYPE_SYNTHETIC,
                CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_FULL_WANTLIST,
                CSV_DUPLICATE_STATUS_NO_DUP,
                0,
            )
        });
        let mut entries = CSVWantlistEntry::from_json_message(msg.clone(), msg_id)
            .context("unable to convert entries to JSON")?;

        // Mark duplicates in the generated entries.
        for entry in entries.iter_mut() {
            if entry.entry_type == CSV_ENTRY_TYPE_CANCEL {
                // Cancels are never dups (I hope).
                continue;
            }
            if let Some(full_wl_dups) = full_wl_dups.as_ref() {
                if full_wl_dups
                    .iter()
                    .find(|e| e.cid == entry.cid && e.want_type == WantType::from_csv_entry(entry))
                    .is_some()
                {
                    // This is a dup
                    entry.duplicate_status += CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST
                }
            }
            if let Some(reconnect_dups) = reconnect_dups.as_ref() {
                if reconnect_dups
                    .iter()
                    .find(|e| e.cid == entry.cid && e.want_type == WantType::from_csv_entry(entry))
                    .is_some()
                {
                    // This is a dup too
                    entry.duplicate_status += CSV_DUPLICATE_STATUS_DUP_RECONNECT
                }
            }
        }

        // Now figure out sliding window stuff.
        // We first make sure the number of non-CANCEL entries is the same as the number of offsets
        // we calculated earlier (for non-CANCEL entries).
        // This costs performance but better be safe than sorry...
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.entry_type != CSV_ENTRY_TYPE_CANCEL)
                .count(),
            offsets_since_earlier_messages.len()
        );

        // We now know they are the same length and same ordering, so we can zip 'em up and be
        // gucci.
        entries
            .iter_mut()
            .filter(|e| e.entry_type != CSV_ENTRY_TYPE_CANCEL)
            .zip(offsets_since_earlier_messages.into_iter())
            .for_each(|(e, (ee, offset))| {
                assert_eq!(e.cid, ee.cid.path);
                if let Some(offset) = offset {
                    e.secs_since_earlier_message = offset;

                    // Figure out if it matches any sliding window.
                    // cfg.sliding_window_lengths is sorted, so we take the first match that is
                    // big enough.
                    if let Some(smallest_match) = self
                        .cfg
                        .sliding_window_lengths
                        .iter()
                        .find(|window_len| **window_len > offset)
                    {
                        e.duplicate_status += CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW;
                        e.sliding_window_smallest_match = *smallest_match
                    }
                }
            });

        // And figure out whether requests are upgraded.
        // Again, figure out if the length of these match.
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.entry_type != CSV_ENTRY_TYPE_CANCEL)
                .count(),
            upgrade_statuses.len()
        );
        entries
            .iter_mut()
            .filter(|e| e.entry_type != CSV_ENTRY_TYPE_CANCEL)
            .zip(upgrade_statuses.into_iter())
            .for_each(|(e, (ee, upgraded))| {
                assert_eq!(e.cid, ee.cid.path);
                e.upgrades_earlier_request = upgraded;
            });

        // Add time-since-request for CANCELs.
        // Again, verify the length of these match.
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.entry_type == CSV_ENTRY_TYPE_CANCEL)
                .count(),
            offsets_since_request_for_cancels.len()
        );
        entries
            .iter_mut()
            .filter(|e| e.entry_type == CSV_ENTRY_TYPE_CANCEL)
            .zip(offsets_since_request_for_cancels.into_iter())
            .for_each(|(e, (ee, secs))| {
                assert_eq!(e.cid, ee.cid.path);
                if let Some(secs) = secs {
                    e.secs_since_earlier_message = secs;
                }
            });

        // Add synthetic cancels
        if let Some(cancels) = synth_cancels {
            entries.extend(cancels.into_iter())
        }

        Ok(IngestResult {
            missing_ledger,
            wantlist_entries: Some(entries),
            connection_event: None,
        })
    }

    fn calculate_secs_since_request_for_cancels(
        ledger: &Ledger,
        new_cancels: &Vec<&JSONWantlistEntry>,
        msg_ts: chrono::DateTime<chrono::Utc>,
    ) -> Vec<(JSONWantlistEntry, Option<u32>)> {
        new_cancels
            .iter()
            .cloned()
            .map(|cancel| {
                if let Ok(i) = ledger
                    .wanted_entries
                    .binary_search_by(|e| e.cid.cmp(&cancel.cid.path))
                {
                    let time_diff = msg_ts - ledger.wanted_entries.get(i).unwrap().ts;
                    if time_diff.num_seconds() == 0 {
                        // Pathological case of less than one second since last message.
                        (cancel.clone(), Some(1))
                    } else {
                        (cancel.clone(), Some(time_diff.num_seconds() as u32))
                    }
                } else {
                    // Not found.
                    (cancel.clone(), None)
                }
            })
            .collect()
    }

    fn get_upgraded_status_for_same_cid(
        ledger: &Ledger,
        new_entries: &Vec<&JSONWantlistEntry>,
        new_cancels: &Vec<&JSONWantlistEntry>,
    ) -> Vec<(JSONWantlistEntry, bool)> {
        new_entries
            .iter()
            .cloned()
            .map(|e| {
                if new_cancels
                    .iter()
                    .find(|ee| ee.cid.path == e.cid.path)
                    .is_some()
                {
                    // We found a CANCEL for the CID.
                    return (e.clone(), false);
                }
                let existing_entry = ledger.wanted_entries.iter().find(|ee| ee.cid == e.cid.path);
                if let Some(existing) = existing_entry {
                    if existing.want_type == WantType::Have
                        && WantType::from_json_entry(e) == WantType::Block
                    {
                        (e.clone(), true)
                    } else {
                        (e.clone(), false)
                    }
                } else {
                    (e.clone(), false)
                }
            })
            .collect()
    }

    fn get_secs_until_earlier_message_with_same_cid_and_want_type(
        ledger: &Ledger,
        msg_ts: chrono::DateTime<chrono::Utc>,
        new_entries: &Vec<&JSONWantlistEntry>,
        new_cancels: &Vec<&JSONWantlistEntry>,
    ) -> Vec<(JSONWantlistEntry, Option<u32>)> {
        new_entries
            .iter()
            .cloned()
            .map(|e| {
                if new_cancels
                    .iter()
                    .find(|ee| ee.cid.path == e.cid.path)
                    .is_some()
                {
                    // We found a CANCEL for the CID.
                    return (e.clone(), None);
                }
                let existing_entry = ledger.wanted_entries.iter().find(|ee| {
                    ee.cid == e.cid.path && ee.want_type == WantType::from_json_entry(e)
                });
                if let Some(existing) = existing_entry {
                    let diff = msg_ts - existing.ts;
                    if diff.num_seconds() == 0 {
                        // Pathological case of less than one second since last message.
                        (e.clone(), Some(1))
                    } else {
                        (e.clone(), Some(diff.num_seconds() as u32))
                    }
                } else {
                    (e.clone(), None)
                }
            })
            .collect()
    }

    fn get_duplicate_entries_and_synth_cancels_from_full_wantlist(
        old_entries: Vec<WantlistEntry>,
        new_entries: &Vec<WantlistEntry>,
    ) -> (Option<Vec<WantlistEntry>>, Option<Vec<WantlistEntry>>) {
        if old_entries.is_empty() {
            return (None, None);
        }
        let (dups, cancels): (Vec<WantlistEntry>, Vec<WantlistEntry>) =
            old_entries.into_iter().partition(|e| {
                new_entries
                    .iter()
                    .find(|n| n.cid == e.cid && n.want_type == e.want_type)
                    .is_some()
            });

        if dups.is_empty() {
            // Cancels must be something
            assert!(!cancels.is_empty());
            (None, Some(cancels))
        } else if cancels.is_empty() {
            // Vice versa
            assert!(!dups.is_empty());
            (Some(dups), None)
        } else {
            (Some(dups), Some(cancels))
        }
    }

    fn get_duplicate_entries_from_reconnect(
        ledger: &mut Ledger,
        msg_ts: chrono::DateTime<chrono::Utc>,
        reconnect_duplicate_duration_secs: u32,
    ) -> Option<Vec<WantlistEntry>> {
        let old_wants = ledger.wanted_entries_before_disconnect.take();
        let reconnect_ts = ledger.connected_ts.clone().unwrap();
        match old_wants {
            Some(old_wants) => {
                // Split into duplicates and non-duplicates.
                let (dups, no_dups): (Vec<WantlistEntry>, Vec<WantlistEntry>) =
                    old_wants.into_iter().partition(|e| {
                        ledger
                            .wanted_entries
                            .iter()
                            .find(|n| n.cid == e.cid && n.want_type == e.want_type)
                            .is_some()
                    });

                // If we're still within the time limit, write back non-dups and emit duplicates.
                let limit_ts = reconnect_ts
                    + chrono::Duration::seconds(reconnect_duplicate_duration_secs as i64);
                if limit_ts > msg_ts {
                    // we're within limits.
                    ledger.wanted_entries_before_disconnect = Some(no_dups);
                    Some(dups)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn ingest_connection_event(&mut self, msg: &JSONMessage, msg_id: i64) -> Result<IngestResult> {
        debug!("ingesting connection event {:?}", msg);
        let mut missing_ledger = false;
        match &msg.peer_disconnected {
            Some(disconnected) => {
                let found = msg.connect_event_peer_found.ok_or_else(|| {
                    err_msg(
                        "message had peer_disconnected set but connect_event_peer_found missing",
                    )
                })?;

                if *disconnected {
                    // Disconnected, decrement connection counter.
                    let ledger = self.peers.entry(msg.peer.clone()).or_insert_with(|| {
                        if found {
                            debug!("disconnect event had connect_event_peer_found=true, but we don't have a ledger for peer {}", msg.peer);
                        }
                        // If not present, we return a fresh entry with one connection, because we
                        // decrement the counter right away.
                        debug!("creating new ledger with one connection for peer {} since we got a disconnection event",msg.peer);
                        missing_ledger = true;
                        Ledger { connection_count: 1, wanted_entries: Default::default(), wanted_entries_before_disconnect: Default::default(), connected_ts: Some(msg.timestamp.clone()) }
                    });
                    debug!("working on ledger {:?}", ledger);

                    ledger.connection_count -= 1;
                    if ledger.connection_count < 0 {
                        warn!(
                            "got disconnected from disconnected peer. Setting connection count to zero. Ledger: {:?}, message: {:?}",
                            ledger,
                            msg
                        );
                        ledger.connection_count = 0;
                    }

                    if ledger.connection_count == 0 {
                        if !ledger.wanted_entries.is_empty() {
                            debug!("found wanted entries, generating synthetic CANCELs");
                            ledger.wanted_entries_before_disconnect =
                                Some(mem::take(&mut ledger.wanted_entries));

                            return Ok(IngestResult {
                                missing_ledger,
                                wantlist_entries: if self.cfg.insert_disconnect_synth_cancels {
                                    // emit synthetic cancels for the disconnect
                                    Some(CSVWantlistEntry::from_wantlist_entries(
                                        ledger.wanted_entries_before_disconnect.clone().unwrap(),
                                        msg,
                                        msg_id,
                                        CSV_MESSAGE_TYPE_SYNTHETIC,
                                        CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_DISCONNECT,
                                        CSV_DUPLICATE_STATUS_NO_DUP,
                                        0,
                                    ))
                                } else {
                                    None
                                },
                                connection_event: Some(
                                    CSVConnectionEvent::from_json_message(msg.clone(), msg_id)
                                        .unwrap(),
                                ),
                            });
                        }
                    }
                }
            }
            None => {
                if self.cfg.allow_empty_connection_event {
                    warn!(
                        "got empty message and no connection event from message {:?}",
                        msg
                    );
                } else {
                    error!(
                        "got empty message and no connection event from message {:?}",
                        msg
                    );
                    return Err(err_msg("got no wantlist entries and no connection event"));
                }
            }
        }

        match &msg.peer_connected {
            Some(connected) => {
                let found = msg.connect_event_peer_found.ok_or_else(|| {
                    err_msg("message had peer_connected set but connect_event_peer_found missing")
                })?;

                if *connected {
                    // Connected, increment connection counter :)
                    let ledger = self.peers.get_mut(&msg.peer);
                    match ledger {
                        Some(ledger) => {
                            if found && ledger.connection_count == 0 {
                                warn!("connect event had connect_event_peer_found=true, but our ledger has zero connections for peer {}", msg.peer);
                            } else if !found && ledger.connection_count > 0 {
                                warn!("connect event had connect_event_peer_found=false, but we have a ledger with at least one connection for peer {}", msg.peer);
                                // To be consistent with IPFS behaviour, we assume IPFS is right
                                // about found==false and didn't report an earlier disconnect.
                                // That means we need to clear out our ledger.
                                let entries = mem::take(&mut ledger.wanted_entries);
                                ledger.wanted_entries_before_disconnect = Some(entries);
                            }
                        }
                        None => {
                            if found {
                                warn!("connect event had connect_event_peer_found=true, but we don't have a ledger for peer {}", msg.peer);
                            }
                        }
                    }

                    let ledger = self.peers.entry(msg.peer.clone()).or_insert_with(|| {
                        debug!("creating new ledger for connection event");
                        Ledger {
                            connection_count: 0,
                            wanted_entries: Default::default(),
                            wanted_entries_before_disconnect: Default::default(),
                            connected_ts: Some(msg.timestamp.clone()),
                        }
                    });
                    debug!("working with ledger {:?}", ledger);

                    ledger.connection_count += 1;
                }
            }
            None => {
                if self.cfg.allow_empty_connection_event {
                    warn!(
                        "got empty message and no connection event from message {:?}",
                        msg
                    );
                } else {
                    error!(
                        "got empty message and no connection event from message {:?}",
                        msg
                    );
                    return Err(err_msg("got no wantlist entries and no connection event"));
                }
            }
        }
        Ok(IngestResult {
            missing_ledger,
            wantlist_entries: None,
            connection_event: Some(
                CSVConnectionEvent::from_json_message(msg.clone(), msg_id).unwrap(),
            ),
        })
    }

    /// Ingests a new message, advancing the simulation and emitting entries.
    pub fn ingest(&mut self, msg: &JSONMessage, msg_id: i64) -> Result<IngestResult> {
        match &msg.received_entries {
            Some(entries) => {
                // This is a wantlist message.
                debug!(
                    "ledger before ingestion of wantlist message is {:?}",
                    self.peers.get(&msg.peer)
                );
                let res = self.ingest_wantlist_message(msg, entries, msg_id);
                debug!(
                    "ledger after ingestion of wantlist message is {:?}",
                    self.peers.get(&msg.peer)
                );
                res
            }
            None => {
                // This is a connection event.
                debug!(
                    "ledger before ingestion of connection event is {:?}",
                    self.peers.get(&msg.peer)
                );
                let res = self.ingest_connection_event(msg, msg_id);
                debug!(
                    "ledger after ingestion of connection event is {:?}",
                    self.peers.get(&msg.peer)
                );
                res
            }
        }
    }

    fn split_wants_cancels(
        new_entries: &[JSONWantlistEntry],
    ) -> (Vec<&JSONWantlistEntry>, Vec<&JSONWantlistEntry>) {
        let (cancels, wants): (Vec<&JSONWantlistEntry>, Vec<&JSONWantlistEntry>) =
            new_entries.iter().partition(|e| e.cancel);
        (wants, cancels)
    }

    fn check_ledger_for_duplicates(l: &[WantlistEntry]) -> bool {
        let mut previous = None;
        for e in l {
            if let Some(previous) = previous {
                if &e.cid == previous {
                    return true;
                }
            }

            previous = Some(&e.cid)
        }

        false
    }

    fn apply_new_entries(
        current_entries: &mut Vec<WantlistEntry>,
        wants: Vec<&JSONWantlistEntry>,
        cancels: Vec<&JSONWantlistEntry>,
        peer: &str,
        ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        for cancel in cancels {
            if let Ok(i) = current_entries.binary_search_by(|e| e.cid.cmp(&cancel.cid.path)) {
                current_entries.remove(i);
            } else {
                // Not found.
                warn!(
                    "got CANCEL for CID {} from peer {}, but don't have an entry for that",
                    cancel.cid.path, peer
                )
            }
        }

        for want in wants {
            match current_entries.binary_search_by(|e| e.cid.cmp(&want.cid.path)) {
                Ok(i) => {
                    // we already have the entry, we need to update its timestamp and want type.
                    current_entries[i].ts = ts.clone();
                    current_entries[i].want_type = WantType::from_json_entry(want);
                }
                Err(i) => current_entries.insert(
                    i,
                    WantlistEntry {
                        cid: want.cid.path.clone(),
                        ts: ts.clone(),
                        want_type: WantType::from_json_entry(want),
                    },
                ),
            }
        }

        ensure!(
            !Self::check_ledger_for_duplicates(current_entries),
            "ledger contains duplicates"
        );

        Ok(())
    }
}
