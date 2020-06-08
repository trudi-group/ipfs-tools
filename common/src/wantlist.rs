use crate::Result;
use failure::err_msg;
use parity_multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem;

/// Constants for the `want_type` field of an `JSONWantlistEntry`.
pub const JSON_WANT_TYPE_BLOCK: i32 = 0;
pub const JSON_WANT_TYPE_HAVE: i32 = 1;

/// The JSON encoding of a CID.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JsonCID {
    #[serde(rename = "/")]
    pub path: String,
}

/// A single entry of a wantlist message.
#[derive(Clone, Serialize, Deserialize, Debug)]
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
    pub want_type: i32,
}

/// A wantlist message.
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
pub const CSV_ENTRY_TYPE_SYNTHETIC_CANCEL: i32 = 6;

/// Connection event type constants for CSV files.
pub const CSV_CONNECTION_EVENT_CONNECTED_FOUND: i32 = 1;
pub const CSV_CONNECTION_EVENT_CONNECTED_NOT_FOUND: i32 = 2;
pub const CSV_CONNECTION_EVENT_DISCONNECTED_FOUND: i32 = 3;
pub const CSV_CONNECTION_EVENT_DISCONNECTED_NOT_FOUND: i32 = 4;

/// Returns the lookup table for entry types as CSV.
pub fn entry_types_csv() -> &'static str {
    include_str!("entry_types.csv")
}

/// Returns the lookup table for message types as CSV.
pub fn message_types_csv() -> &'static str {
    include_str!("message_types.csv")
}

/// Returns the lookup table for connection events as CSV.
pub fn connection_event_types_csv() -> &'static str {
    include_str!("connection_event_types.csv")
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CSVWantlistEntry {
    pub message_id: i64,
    pub message_type: i32,
    pub timestamp_seconds: i64,
    pub timestamp_subsec_milliseconds: u32,
    pub peer_id: String,
    pub address: String,
    pub priority: i32,
    pub entry_type: i32,
    pub cid: String,
}

impl CSVWantlistEntry {
    pub fn from_synthetic_cancels(
        cancels: Vec<WantlistEntry>,
        message: &JSONMessage,
        id: i64,
    ) -> Result<Vec<CSVWantlistEntry>> {
        let message_timestamp_seconds = message.timestamp.timestamp();
        let message_timestamp_subsec_millis = message.timestamp.timestamp_subsec_millis();

        Ok(cancels
            .into_iter()
            .map(|e| CSVWantlistEntry {
                message_id: id,
                message_type: CSV_MESSAGE_TYPE_SYNTHETIC,
                timestamp_seconds: message_timestamp_seconds,
                timestamp_subsec_milliseconds: message_timestamp_subsec_millis,
                peer_id: message.peer.clone(),
                address: match &message.address {
                    Some(address) => address.to_string(),
                    None => "".to_string(),
                },
                priority: 0,
                entry_type: CSV_ENTRY_TYPE_SYNTHETIC_CANCEL,
                cid: e.cid,
            })
            .collect())
    }

    pub fn from_json_message(message: JSONMessage, id: i64) -> Result<Vec<CSVWantlistEntry>> {
        match message.received_entries {
            Some(entries) => {
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
                                JSON_WANT_TYPE_BLOCK => {
                                    if entry.send_dont_have {
                                        CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE
                                    } else {
                                        CSV_ENTRY_TYPE_WANT_BLOCK
                                    }
                                }
                                JSON_WANT_TYPE_HAVE => {
                                    if entry.send_dont_have {
                                        CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE
                                    } else {
                                        CSV_ENTRY_TYPE_WANT_HAVE
                                    }
                                }
                                _ => panic!(format!("unknown JSON want type {}", entry.want_type)),
                            }
                        },
                        cid: entry.cid.path,
                    })
                    .collect();

                Ok(csv_entries)
            }
            None => Err(err_msg("no entries")),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CSVConnectionEvent {
    pub message_id: i64,
    pub timestamp_seconds: i64,
    pub timestamp_subsec_millis: u32,
    pub peer_id: String,
    pub address: String,
    pub event_type: i32,
}

impl CSVConnectionEvent {
    pub fn from_json_message(message: JSONMessage, id: i64) -> Result<CSVConnectionEvent> {
        if let Some(disconnected) = message.peer_disconnected {
            if disconnected {
                return match message.connect_event_peer_found {
                    Some(found) => Ok(CSVConnectionEvent {
                        message_id: id,
                        timestamp_seconds: message.timestamp.timestamp(),
                        timestamp_subsec_millis: message.timestamp.timestamp_subsec_millis(),
                        peer_id: message.peer,
                        address: match &message.address {
                            Some(address) => address.to_string(),
                            None => "".to_string(),
                        },
                        event_type: if found {
                            CSV_CONNECTION_EVENT_DISCONNECTED_FOUND
                        } else {
                            CSV_CONNECTION_EVENT_DISCONNECTED_NOT_FOUND
                        },
                    }),
                    None => Err(err_msg("got disconnected but found is None")),
                };
            }
        }
        if let Some(connected) = message.peer_connected {
            if connected {
                return match message.connect_event_peer_found {
                    Some(found) => Ok(CSVConnectionEvent {
                        message_id: id,
                        timestamp_seconds: message.timestamp.timestamp(),
                        timestamp_subsec_millis: message.timestamp.timestamp_subsec_millis(),
                        peer_id: message.peer,
                        address: match &message.address {
                            Some(address) => address.to_string(),
                            None => "".to_string(),
                        },
                        event_type: if found {
                            CSV_CONNECTION_EVENT_CONNECTED_FOUND
                        } else {
                            CSV_CONNECTION_EVENT_CONNECTED_NOT_FOUND
                        },
                    }),
                    None => Err(err_msg("got connected but found is None")),
                };
            }
        }

        Err(err_msg("not a connection event"))
    }
}

/*
enum WantType {
    Block,
    Have,
}*/

#[derive(Clone, Debug)]
pub struct WantlistEntry {
    cid: String,
    //want_type: WantType,
    //send_dont_have: bool,
}

#[derive(Clone, Debug)]
pub struct Ledger {
    connection_count: i32,
    wanted_entries: Vec<WantlistEntry>,
    wanted_entries_before_disconnect: Option<Vec<WantlistEntry>>,
}

#[derive(Clone, Debug, Default)]
pub struct Wantlist {
    peers: HashMap<String, Ledger>,
}

#[derive(Clone, Debug, Default)]
pub struct IngestResult {
    pub missing_ledger: bool, // This means we are missing a ledger entry for the peer, not generally a mismatch between found and our state.
    pub entries_canceled_because_full_want_list: Option<Vec<WantlistEntry>>,
    pub entries_canceled_because_new_connection: Option<Vec<WantlistEntry>>,
}

impl Wantlist {
    pub fn new() -> Wantlist {
        Wantlist {
            peers: HashMap::new(),
        }
    }

    pub fn ingest(
        &mut self,
        msg: &JSONMessage,
        allow_empty_full_want_list: bool,
        allow_empty_connection_event: bool,
    ) -> Result<IngestResult> {
        let mut missing_ledger = false;
        match &msg.received_entries {
            Some(entries) => {
                // This is a wantlist message.
                let ledger = self.peers.entry(msg.peer.clone()).or_insert_with(|| {
                    debug!("received wantlist from {} ({:?}), but don't have a ledger for that peer. Starting empty one with one connection.",msg.peer,msg.address);
                    missing_ledger = true;
                    Ledger{connection_count:1,wanted_entries:Default::default(),wanted_entries_before_disconnect:Default::default()}
                });
                if ledger.connection_count == 0{
                    error!("got wantlist entries from peer {}, but we are still disconnected from that peer (was previously connected). This is either an error in how IPFS notifies about connection events, or in how we ingest them.",msg.peer);
                    ledger.connection_count = 1;
                }
                assert!(ledger.connection_count > 0);

                match &msg.full_want_list {
                    Some(full) => match full {
                        true => {
                            let (new_wants, _) = Self::split_wants_cancels(&entries);
                            let old_wants = mem::replace(
                                &mut ledger.wanted_entries,
                                new_wants
                                    .iter()
                                    .map(|c| WantlistEntry {
                                        cid: c.cid.path.clone(),
                                    })
                                    .collect(),
                            );
                            ledger
                                .wanted_entries
                                .sort_unstable_by(|e1, e2| e1.cid.cmp(&e2.cid));

                            assert!(
                                !(!old_wants.is_empty()
                                    && ledger.wanted_entries_before_disconnect.is_some())
                            );
                            let old_wants = {
                                match ledger.wanted_entries_before_disconnect.take() {
                                    Some(entries) => entries,
                                    None => old_wants,
                                }
                            };
                            let dropped_wants: Vec<WantlistEntry> = old_wants
                                .into_iter()
                                .filter(|e| {
                                    new_wants.iter().find(|n| n.cid.path == e.cid).is_none()
                                })
                                .collect();
                            return Ok(IngestResult {
                                missing_ledger,
                                entries_canceled_because_full_want_list: Some(dropped_wants),
                                entries_canceled_because_new_connection: None,
                            });
                        }
                        false => {
                            assert!(
                                !(ledger.wanted_entries_before_disconnect.is_some()
                                    && !ledger.wanted_entries.is_empty())
                            );
                            Self::apply_new_entries(
                                &mut ledger.wanted_entries,
                                entries,
                                &msg.peer,
                            )?;
                            let old_wants = ledger.wanted_entries_before_disconnect.take();
                            match old_wants {
                                Some(old_wants) => {
                                    let dropped_wants: Vec<WantlistEntry> = old_wants
                                        .into_iter()
                                        .filter(|e| {
                                            ledger
                                                .wanted_entries
                                                .iter()
                                                .find(|n| n.cid == e.cid)
                                                .is_none()
                                        })
                                        .collect();
                                    return Ok(IngestResult {
                                        missing_ledger,
                                        entries_canceled_because_full_want_list: None,
                                        entries_canceled_because_new_connection: Some(
                                            dropped_wants,
                                        ),
                                    });
                                }
                                None => {}
                            }
                        }
                    },
                    None => {
                        if allow_empty_full_want_list {
                            debug!("got empty full_want_list, assuming incremental.");
                            assert!(
                                !(ledger.wanted_entries_before_disconnect.is_some()
                                    && !ledger.wanted_entries.is_empty())
                            );
                            Self::apply_new_entries(
                                &mut ledger.wanted_entries,
                                entries,
                                &msg.peer,
                            )?;
                            let old_wants = ledger.wanted_entries_before_disconnect.take();
                            match old_wants {
                                Some(old_wants) => {
                                    let dropped_wants: Vec<WantlistEntry> = old_wants
                                        .into_iter()
                                        .filter(|e| {
                                            ledger
                                                .wanted_entries
                                                .iter()
                                                .find(|n| n.cid == e.cid)
                                                .is_none()
                                        })
                                        .collect();
                                    return Ok(IngestResult {
                                        missing_ledger,
                                        entries_canceled_because_full_want_list: None,
                                        entries_canceled_because_new_connection: Some(
                                            dropped_wants,
                                        ),
                                    });
                                }
                                None => {}
                            }
                        } else {
                            error!("got empty full_want_list, but is not allowed.");
                            return Err(err_msg("got empty full_want_list, should be set"));
                        }
                    }
                }
            }
            None => {
                // This is a connection event.
                match &msg.peer_disconnected {
                    Some(disconnected) => {
                        let found = msg.connect_event_peer_found.ok_or_else(|| {err_msg("message had peer_disconnected set but connect_event_peer_found missing")})?;

                        if *disconnected {
                            // Disconnected, decrement connection counter.
                            let ledger = self.peers.entry(msg.peer.clone()).or_insert_with(|| {
                                if found {
                                    debug!("disconnect event had connect_event_peer_found=true, but we don't have a ledger for peer {}",msg.peer);
                                }
                                // If not present, we return a fresh entry with one connection, because we decrement the counter right away.
                                missing_ledger = true;
                                Ledger{connection_count:1,wanted_entries:Default::default(),wanted_entries_before_disconnect:Default::default()}
                            });

                            ledger.connection_count -= 1;
                            assert!(ledger.connection_count >= 0);

                            if ledger.connection_count == 0 {
                                assert!(
                                    ledger.wanted_entries_before_disconnect.is_none()
                                        || ledger.wanted_entries.is_empty()
                                );
                                if !ledger.wanted_entries_before_disconnect.is_none() {
                                    ledger.wanted_entries_before_disconnect =
                                        Some(mem::take(&mut ledger.wanted_entries));
                                }
                            }
                        }
                    }
                    None => {
                        if allow_empty_connection_event {
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
                        let found = msg.connect_event_peer_found.ok_or_else(|| {err_msg("message had peer_connected set but connect_event_peer_found missing")})?;

                        if *connected {
                            // Connected, increment connection counter :)
                            let ledger = self.peers.get(&msg.peer);
                            match ledger {
                                Some(ledger) => {
                                    if found && ledger.connection_count == 0 {
                                        warn!("connect event had connect_event_peer_found=true, but our ledger has zero connections for peer {}",msg.peer);
                                    } else if !found && ledger.connection_count > 0 {
                                        warn!("connect event had connect_event_peer_found=false, but we have a ledger with at least one connection for peer {}",msg.peer);
                                    }
                                }
                                None => {
                                    if found {
                                        warn!("connect event had connect_event_peer_found=true, but we don't have a ledger for peer {}",msg.peer);
                                    }
                                }
                            }

                            let ledger =
                                self.peers
                                    .entry(msg.peer.clone())
                                    .or_insert_with(|| Ledger {
                                        connection_count: 0,
                                        wanted_entries: Default::default(),
                                        wanted_entries_before_disconnect: Default::default(),
                                    });

                            ledger.connection_count += 1;
                            assert!(ledger.connection_count > 0);
                            assert!(
                                !(ledger.wanted_entries_before_disconnect.is_some()
                                    && !ledger.wanted_entries.is_empty())
                            );
                        }
                    }
                    None => {
                        if allow_empty_connection_event {
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
            }
        }

        Ok(IngestResult {
            missing_ledger,
            entries_canceled_because_full_want_list: None,
            entries_canceled_because_new_connection: None,
        })
    }

    fn split_wants_cancels(
        new_entries: &[JSONWantlistEntry],
    ) -> (Vec<&JSONWantlistEntry>, Vec<&JSONWantlistEntry>) {
        let (cancels, wants): (Vec<&JSONWantlistEntry>, Vec<&JSONWantlistEntry>) =
            new_entries.iter().partition(|e| e.cancel);
        (wants, cancels)
    }

    fn apply_new_entries(
        current_entries: &mut Vec<WantlistEntry>,
        new_entries: &[JSONWantlistEntry],
        peer: &str,
    ) -> Result<()> {
        let (wants, cancels) = Self::split_wants_cancels(new_entries);

        for cancel in cancels {
            if let Ok(i) = current_entries.binary_search_by(|e| e.cid.cmp(&cancel.cid.path)) {
                current_entries.remove(i);
            } else {
                // Not found.
                debug!(
                    "got CANCEL for CID {} from peer {}, but don't have an entry for that",
                    cancel.cid.path, peer
                )
            }
        }

        for want in wants {
            match current_entries.binary_search_by(|e| e.cid.cmp(&want.cid.path)) {
                Ok(_) => {} // we already have the entry, no need to do anything
                Err(i) => current_entries.insert(
                    i,
                    WantlistEntry {
                        cid: want.cid.path.clone(),
                    },
                ),
            }
        }

        Ok(())
    }
}
