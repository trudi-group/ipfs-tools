use crate::Result;
use failure::err_msg;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Clone, Debug, Default)]
pub struct Wantlist {
    peers: HashMap<String, Vec<WantlistEntry>>,
}

impl Wantlist {
    pub fn new() -> Wantlist {
        Wantlist {
            peers: HashMap::new(),
        }
    }

    pub fn ingest(&mut self, msg: &JSONMessage) -> Result<Option<Vec<WantlistEntry>>> {
        match &msg.received_entries {
            Some(entries) => {
                let ledger = self.peers.entry(msg.peer.clone()).or_default();

                match &msg.full_want_list {
                    Some(full) => {
                        match full {
                            true => {
                                // TODO we can speed this up by splitting wants/cancels and replacing with wants.
                                ledger.truncate(0);
                            }
                            false => {}
                        }
                        Self::apply_new_entries(ledger, entries)?;
                    }
                    None => {
                        debug!("got empty full_want_list, assuming incremental.");
                        Self::apply_new_entries(ledger, entries)?;
                    }
                }
            }
            None => {
                match &msg.peer_disconnected {
                    Some(disconnected) => {
                        if *disconnected {
                            // Disconnected, clear our entries for this peer.
                            return Ok(self.peers.remove(&msg.peer));
                        }
                    }
                    None => warn!(
                        "got empty message and no connection event from message {:?}",
                        msg
                    ),
                }
            }
        }

        Ok(None)
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
    ) -> Result<()> {
        let (wants, cancels) = Self::split_wants_cancels(new_entries);

        for cancel in cancels {
            if let Ok(i) = current_entries.binary_search_by(|e| e.cid.cmp(&cancel.cid.path)) {
                current_entries.remove(i);
            }
            // Else not found, but doesn't matter.
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
