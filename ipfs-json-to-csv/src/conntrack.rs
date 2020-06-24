use failure::{err_msg, ResultExt};
use ipfs_resolver_common::{wantlist, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct ConnectionDurationTracker {
    beginning_ts: Option<chrono::DateTime<chrono::Utc>>,
    connections: HashMap<String, PeerConnectionBuffer>,
}

#[derive(Clone, Debug)]
struct PeerConnectionBuffer {
    connection_start: Option<chrono::DateTime<chrono::Utc>>,
    connected_address: Option<String>,
    past_connections: Vec<ConnectionMetadata>,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionMetadata {
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    address: Option<String>,
}

impl ConnectionMetadata {
    pub(crate) fn to_csv(&self, peer_id: String) -> CSVConnectionMetadata {
        CSVConnectionMetadata {
            peer_id,
            start_ts_seconds: self.start.timestamp(),
            start_ts_subsec_millis: self.start.timestamp_subsec_millis(),
            end_ts_seconds: self.end.timestamp(),
            end_ts_subsec_millis: self.end.timestamp_subsec_millis(),
            address: self.address.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CSVConnectionMetadata {
    peer_id: String,
    start_ts_seconds: i64,
    start_ts_subsec_millis: u32,
    end_ts_seconds: i64,
    end_ts_subsec_millis: u32,
    address: Option<String>,
}

impl ConnectionDurationTracker {
    pub fn new() -> ConnectionDurationTracker {
        ConnectionDurationTracker {
            beginning_ts: None,
            connections: HashMap::new(),
        }
    }

    pub fn finalize(
        mut self,
        final_ts: chrono::DateTime<chrono::Utc>,
    ) -> HashMap<String, Vec<ConnectionMetadata>> {
        for (k, v) in self.connections.iter_mut() {
            if v.connection_start.is_some() {
                debug!("finalizing currently connected peer {}", k);

                let start = v.connection_start.take().unwrap();
                let addr = v.connected_address.take();
                v.past_connections.push(ConnectionMetadata {
                    start,
                    end: final_ts.clone(),
                    address: addr,
                })
            }
        }

        self.connections
            .into_iter()
            .map(|(k, v)| (k, v.past_connections))
            .collect()
    }

    pub fn push(&mut self, msg: &wantlist::JSONMessage) -> Result<()> {
        if self.beginning_ts.is_none() {
            self.beginning_ts = Some(msg.timestamp)
        }

        if let Some(connected) = msg.peer_connected {
            let disconnected = msg
                .peer_disconnected
                .ok_or(err_msg("missing disconnected"))
                .context("message has Some(connected), but disconnected is missing")?;
            let found = msg
                .connect_event_peer_found
                .ok_or(err_msg("missing found"))
                .context("message had Some(connected), but found is missing")?;
            let addr = msg.address.clone().map(|a| a.to_string());

            if !connected && !disconnected {
                debug!(
                    "skipping because CONNECTED and DISCONNECTED are both false, msg: {:?}",
                    msg
                );
                return Ok(());
            }

            self.push_inner(
                msg.peer.clone(),
                connected,
                disconnected,
                found,
                addr,
                msg.timestamp,
            )
            .context("unable to track connection duration")?;
        }

        Ok(())
    }

    fn push_inner(
        &mut self,
        peer_id: String,
        connected: bool,
        disconnected: bool,
        _found: bool,
        addr: Option<String>,
        ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let mut fresh = false;
        let mut entry = self.connections.entry(peer_id.clone()).or_insert_with(|| {
            debug!(
                "peer connection buffer for peer {} not found, initializing new one",
                peer_id
            );
            fresh = true;
            PeerConnectionBuffer {
                connection_start: Some(ts.clone()),
                connected_address: addr.clone(),
                past_connections: Vec::new(),
            }
        });

        if entry.connected_address.is_some() {
            if addr != entry.connected_address {
                warn!(
                    "address mismatch, had {:?}, got {:?}",
                    entry.connected_address, addr
                )
            }
        }

        if connected {
            if entry.connection_start.is_some() {
                if !fresh {
                    warn!(
                        "got CONNECTED, but already have a connection for peer {}, ignoring",
                        peer_id
                    );
                }
                return Ok(());
            }

            entry.connection_start = Some(ts);
            entry.connected_address = addr;
        } else if disconnected {
            if entry.connection_start.is_none() {
                warn!(
                    "got DISCONNECTED, but don't have a connection for peer {}, ignoring",
                    peer_id
                );
                return Ok(());
            }

            let start = entry.connection_start.take().unwrap();
            let addr = entry.connected_address.take();
            entry.past_connections.push(ConnectionMetadata {
                start,
                end: ts,
                address: addr,
            });
        } else {
            return Err(err_msg("got neither CONNECTED nor DISCONNECTED"));
        }

        Ok(())
    }
}
