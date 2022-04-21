#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

use clap::{App, Arg};
use failure::{ensure, err_msg, ResultExt};
use futures_util::future::try_join_all;
use futures_util::{StreamExt, TryFutureExt};
use multiaddr::Multiaddr;
use std::collections::{HashMap, HashSet};
use std::io::stdout;
use std::str::FromStr;
use std::{env, io, time};
use tokio::net::TcpStream;
use tokio::select;

use crate::config::Config;
use ipfs_resolver_common::{logging, Result};
use wantlist_client_lib::http::{APIClient, BroadcastBitswapWantCancelEntry};
use wantlist_client_lib::net;
use wantlist_client_lib::net::{EventType, MonitoringClient, PushedEvent};

mod config;

#[tokio::main]
async fn main() -> Result<()> {
    logging::set_up_logging(false)?;

    // Set up CLI.
    let matches = App::new("IPFS Bitswap monitoring content discovery tool")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("connects to Bitswap monitoring nodes and discovers content placement on connected nodes via Bitswap broadcasts")
        .arg(
            Arg::with_name("cfg")
                .long("config")
                .value_name("PATH")
                .default_value("config.yaml")
                .help("the config file to load")
                .required(true),
        )
        .get_matches();

    // Read args.
    if !matches.is_present("cfg") {
        println!("{}", matches.usage());
        return Err(err_msg("missing config"));
    }
    let cfg = matches.value_of("cfg").unwrap();

    // Read config.
    info!("attempting to load config file '{}'", cfg);
    let cfg = Config::open(cfg).context("unable to load config")?;
    debug!("read config {:?}", cfg);

    // Parse CIDs.
    // We do this in order to properly compare CIDs we receive.
    let cids = cfg
        .cids
        .iter()
        .map(|c| {
            cid::Cid::from_str(c)
                //.map(|c| Cid::new_v1(c.codec(), c.hash().clone()))
                .map_err(|e| err_msg(format!("unable to parse CID {}: {:?}", c, e)))
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("unable to parse CID")?;
    debug!("parsed CIDs {:?}", cids);

    // Deduplicate CIDs.
    let mut cids = cids;
    cids.sort();
    cids.windows(2)
        .try_for_each(|cs| {
            let c1 = cs[0];
            let c2 = cs[1];
            if c1 == c2 {
                Err(err_msg(format!("duplicate CID {} in input", c1)))
            } else {
                Ok(())
            }
        })
        .context("input contains duplicates")?;
    cids.dedup();
    let cids = cids;

    // Connect to monitors.
    info!("connecting to monitors");
    let probes = try_join_all(cfg.monitors.iter().map(|c| {
        let name = c.name.clone();
        Probe::connect(&c.monitoring_address, &c.api_base_url, &cids, &c.name)
            .and_then(|p| async move { futures::future::ok((name, p)).await })
    }))
    .await
    .context("unable to set up probes")?;
    debug!("connected to probes: {:?}", probes);

    let monitor_names = probes
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    // This is a Map<(Monitor,PeerID),(IP,Map<CID,(WANT ts, HAVE ts, DONT_HAVE ts, BLOCK ts, CANCEL ts)>)>
    let mut results: HashMap<_, PeerEntry> = HashMap::new();

    // Send WANT+CANCEL broadcasts.
    info!(
        "sending broadcast request for {} CIDs with {} seconds in between WANT and CANCEL",
        cids.len(),
        cfg.cancel_after_seconds
    );
    debug!("sending broadcast request for CIDs {:?}", cids);
    let res = try_join_all(probes.iter().map(|(monitor_name, probe)| {
        debug!("probing on {}...", monitor_name);
        probe.broadcast(&cids, cfg.cancel_after_seconds)
    }))
    .await
    .context("unable to send broadcast")?;
    info!(
        "attempted to broadcast to {} peers, sent {} WANTs and {} CANCELs successfully",
        res.iter().flatten().count(),
        res.iter()
            .flatten()
            .filter(|r| r.want_status.error.is_none())
            .count(),
        res.iter()
            .flatten()
            .filter(|r| r.cancel_status.error.is_none())
            .count()
    );
    debug!("got broadcast results {:?}", res);

    // Add to results
    monitor_names
        .iter()
        .zip(res.into_iter())
        .try_for_each(|(name, res)| {
            res.into_iter().try_for_each(|res| {
                let entry = results.entry((name.clone(), res.peer)).or_default();
                for c in cids.clone() {
                    ensure!(
                        entry
                            .cids
                            .insert(
                                c.to_string(),
                                CIDEntry {
                                    want_broadcast_status: BroadcastSendStatus {
                                        before_ts: res.want_status.timestamp_before_send,
                                        send_duration_millis: res.want_status.send_duration_millis,
                                        error: res.want_status.error.clone()
                                    },
                                    have_received_ts: None,
                                    dont_have_received_ts: None,
                                    block_received_ts: None,
                                    cancel_broadcast_status: BroadcastSendStatus {
                                        before_ts: res.cancel_status.timestamp_before_send,
                                        send_duration_millis: res
                                            .cancel_status
                                            .send_duration_millis,
                                        error: res.cancel_status.error.clone()
                                    }
                                }
                            )
                            .is_none(),
                        "maybe duplicate CID {}?",
                        c
                    );
                }
                Ok(())
            })
        })
        .context("unable to construct results")?;

    // Sleep for a while.
    info!(
        "sleeping for {} seconds, waiting for slow responses...",
        cfg.wait_after_cancel_seconds
    );
    tokio::time::sleep(time::Duration::from_secs(
        cfg.wait_after_cancel_seconds as u64,
    ))
    .await;

    // Shut down monitoring.
    info!("done probing, now disconnecting from monitors and collecting results");
    let res = try_join_all(probes.into_iter().map(|(monitor_name, probe)| {
        debug!("shutting down {}...", monitor_name);
        probe.close()
    }))
    .await
    .context("unable to cleanly shut down probes")?;
    debug!("got bitswap responses {:?}", res);

    // Add monitoring results to overall results.
    monitor_names.iter().zip(res.into_iter()).for_each(|(name,res)| {
        res.into_iter().for_each(|res| {
            let entry = results.get_mut(&(name.clone(),res.peer.clone()));
            match entry {
                Some( entry) => {
                    let mut cid_entry = entry.cids.get_mut(&res.cid.to_string()).expect("missing CID in result set");

                    // Add addresses
                    entry.connected_addrs.extend(res.connected_addrs.clone().into_iter());
                    entry.connected_addrs.sort();
                    entry.connected_addrs.dedup();

                    // Set timestamps
                    match res.response {
                        BroadcastResponseType::Block => {
                            if cid_entry.block_received_ts.is_some() {
                                warn!("double BLOCK response by peer {} to WANT {} on monitor {}; ignoring; previous state: {:?}",res.peer,res.cid,name,cid_entry);
                                return
                            }
                            cid_entry.block_received_ts = Some(res.timestamp)
                        }
                        BroadcastResponseType::BlockPresence { presence_type } => {
                            match presence_type {
                                net::TCP_BLOCK_PRESENCE_TYPE_HAVE => {
                                    if cid_entry.have_received_ts.is_some() {
                                        warn!("double HAVE response by peer {} to WANT {} on monitor {}; ignoring; previous state: {:?}",res.peer,res.cid,name,cid_entry);
                                        return
                                    }
                                    cid_entry.have_received_ts = Some(res.timestamp)
                                },
                                net::TCP_BLOCK_PRESENCE_TYPE_DONT_HAVE => {
                                    if cid_entry.dont_have_received_ts.is_some() {
                                        warn!("double DONT_HAVE response by peer {} to WANT {} on monitor {}; ignoring; previous state: {:?}",res.peer,res.cid,name,cid_entry);
                                        return
                                    }
                                    cid_entry.dont_have_received_ts = Some(res.timestamp)
                                },
                                _ => {
                                    error!("invalid block presence type emitted by monitor {}: {:?}",name,res)
                                }
                            }
                        }
                    }

                },
                None => {
                    warn!("received Bitswap response from peer {} to which we didn't successfully sent a request",res.peer)
                }
            }
        })
    });
    debug!("constructed complete result set: {:?}", results);

    debug!("writing CSV output...");
    let mut output_writer = csv::Writer::from_writer(io::BufWriter::new(stdout()));
    results
        .into_iter()
        .flat_map(|((monitor_name, peer_id), peer_entry)| {
            let connected_addrs = format!(
                "[{}]",
                peer_entry
                    .connected_addrs
                    .clone()
                    .into_iter()
                    .map(|a| format!("{}", a))
                    .collect::<Vec<String>>()
                    .join(",")
            );
            peer_entry
                .cids
                .into_iter()
                .zip(std::iter::repeat((monitor_name, peer_id, connected_addrs)))
                .map(
                    |((c, entry), (monitor_name, peer_id, connected_addrs))| OutputCSVRow {
                        monitor: monitor_name,
                        peer_id,
                        connected_addrs,
                        cid: c,
                        want_before_send_ts_seconds: entry
                            .want_broadcast_status
                            .before_ts
                            .timestamp(),
                        want_before_send_ts_subsec_milliseconds: entry
                            .want_broadcast_status
                            .before_ts
                            .timestamp_subsec_millis(),
                        want_send_error: entry.want_broadcast_status.error,
                        want_send_duration_millis: entry.want_broadcast_status.send_duration_millis,
                        have_received_ts_seconds: entry.have_received_ts.map(|ts| ts.timestamp()),
                        have_received_ts_subsec_milliseconds: entry
                            .have_received_ts
                            .map(|ts| ts.timestamp_subsec_millis()),
                        dont_have_received_ts_seconds: entry
                            .dont_have_received_ts
                            .map(|ts| ts.timestamp()),
                        dont_have_received_ts_subsec_milliseconds: entry
                            .dont_have_received_ts
                            .map(|ts| ts.timestamp_subsec_millis()),
                        block_received_ts_seconds: entry.block_received_ts.map(|ts| ts.timestamp()),
                        block_received_ts_subsec_milliseconds: entry
                            .block_received_ts
                            .map(|ts| ts.timestamp_subsec_millis()),
                        cancel_before_send_ts_seconds: entry
                            .cancel_broadcast_status
                            .before_ts
                            .timestamp(),
                        cancel_before_send_ts_subsec_milliseconds: entry
                            .cancel_broadcast_status
                            .before_ts
                            .timestamp_subsec_millis(),
                        cancel_send_error: entry.cancel_broadcast_status.error,
                        cancel_send_duration_millis: entry
                            .cancel_broadcast_status
                            .send_duration_millis,
                    },
                )
        })
        .try_for_each(|row| output_writer.serialize(row))
        .context("unable to write CSV output")?;
    info!("done writing CSV output");

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputCSVRow {
    pub monitor: String,
    pub peer_id: String,
    pub connected_addrs: String,
    pub cid: String,

    pub want_before_send_ts_seconds: i64,
    pub want_before_send_ts_subsec_milliseconds: u32,
    pub want_send_error: Option<String>,
    pub want_send_duration_millis: i64,

    pub have_received_ts_seconds: Option<i64>,
    pub have_received_ts_subsec_milliseconds: Option<u32>,

    pub dont_have_received_ts_seconds: Option<i64>,
    pub dont_have_received_ts_subsec_milliseconds: Option<u32>,

    pub block_received_ts_seconds: Option<i64>,
    pub block_received_ts_subsec_milliseconds: Option<u32>,

    pub cancel_before_send_ts_seconds: i64,
    pub cancel_before_send_ts_subsec_milliseconds: u32,
    pub cancel_send_error: Option<String>,
    pub cancel_send_duration_millis: i64,
}

#[derive(Debug, Default, Clone, Serialize)]
struct PeerEntry {
    connected_addrs: Vec<Multiaddr>,
    cids: HashMap<String, CIDEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct CIDEntry {
    want_broadcast_status: BroadcastSendStatus,
    have_received_ts: Option<chrono::DateTime<chrono::Utc>>,
    dont_have_received_ts: Option<chrono::DateTime<chrono::Utc>>,
    block_received_ts: Option<chrono::DateTime<chrono::Utc>>,
    cancel_broadcast_status: BroadcastSendStatus,
}

#[derive(Debug, Clone, Serialize)]
struct BroadcastSendStatus {
    before_ts: chrono::DateTime<chrono::Utc>,
    send_duration_millis: i64,
    error: Option<String>,
}

#[derive(Clone, Debug)]
struct BroadcastResponse {
    peer: String,
    connected_addrs: Vec<Multiaddr>,
    timestamp: chrono::DateTime<chrono::Utc>,
    cid: cid::Cid,
    response: BroadcastResponseType,
}

#[derive(Clone, Debug)]
enum BroadcastResponseType {
    Block,
    BlockPresence { presence_type: i32 },
}

#[derive(Debug)]
struct Probe {
    api: APIClient,
    msg_chan: tokio::sync::oneshot::Receiver<Result<Vec<BroadcastResponse>>>,
    shutdown_chan: tokio::sync::oneshot::Sender<()>,
}

impl Probe {
    async fn connect(
        monitoring_address: &str,
        api_base_url: &str,
        cids_of_interest: &[cid::Cid],
        monitor_name: &str,
    ) -> Result<Probe> {
        // Connect to node's plugin API.
        debug!("connecting to node {} at {}...", monitor_name, api_base_url);
        let client = APIClient::new(api_base_url).context("unable to create API client")?;

        debug!("testing API for node {}...", monitor_name);
        client.ping().await.context("unable to ping API")?;
        info!("connected to node {} at {}...", monitor_name, api_base_url);

        // Connect to node's monitoring endpoint.
        debug!(
            "connecting to monitor {} at {}...",
            monitor_name, monitoring_address
        );
        let conn = TcpStream::connect(monitoring_address)
            .await
            .context("unable to connect")?;
        info!(
            "connected to monitor {} at {}",
            monitor_name, monitoring_address
        );

        let monitoring_client =
            tokio::time::timeout(time::Duration::from_secs(30), MonitoringClient::new(conn))
                .await
                .context("timeout creating monitoring client")?
                .context("unable to initiate monitoring client")?;
        debug!("{}: created monitoring client", monitor_name);

        // Set up some plumbing.
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Fire off a worker to handle the monitoring.
        let cids = cids_of_interest.to_vec();
        let monitor_name = monitor_name.to_string();
        tokio::spawn(Self::receive_messages(
            monitor_name,
            monitoring_client,
            shutdown_rx,
            cids.into_iter().collect(),
            res_tx,
            ready_tx,
        ));

        ready_rx
            .await
            .context("bitswap wiretap receiver failed to start")?;

        Ok(Probe {
            api: client,
            msg_chan: res_rx,
            shutdown_chan: shutdown_tx,
        })
    }

    async fn broadcast(
        &self,
        cids: &[cid::Cid],
        cancel_after_seconds: u32,
    ) -> Result<Vec<BroadcastBitswapWantCancelEntry>> {
        self.api
            .broadcast_bitswap_want_cancel(
                cids.iter().map(|c| c.to_string()).collect(),
                cancel_after_seconds,
            )
            .await
    }

    async fn close(self) -> Result<Vec<BroadcastResponse>> {
        let Probe {
            api: _,
            msg_chan,
            shutdown_chan,
        } = self;

        if let Err(_) = shutdown_chan.send(()) {
            return Err(err_msg(
                "unable to shut down worker cleanly, probably died in the meantime",
            ));
        }

        let messages = msg_chan
            .await
            .context("failed to collect messages, worker died for unknown reasons")?;

        messages
    }

    async fn receive_messages(
        monitor_name: String,
        mut monitoring_client: MonitoringClient,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        cids_of_interest: HashSet<cid::Cid>,
        res_chan: tokio::sync::oneshot::Sender<Result<Vec<BroadcastResponse>>>,
        ready_chan: tokio::sync::oneshot::Sender<()>,
    ) {
        let mut first = true;
        let mut responses = Vec::new();
        let mut ready_chan = Some(ready_chan);

        loop {
            select! {
                _ = &mut shutdown_rx => {
                    // Shut down.
                    debug!("{}: shutting down monitoring cleanly",monitor_name);
                    break
                },
                event = monitoring_client.next() => {
                    match event {
                        None => {break}
                        Some(event) => {
                            if let Err(e) = Self::handle_message(&monitor_name,
                                &cids_of_interest,
                                event,
                                &mut first,
                                &mut responses,
                                &mut ready_chan) {
                                error!("{}: unable to handle message: {}",monitor_name,e);
                                break
                            }
                        }
                    }
                }
            }
        }
        debug!("{}: disconnected", monitor_name);

        if let Err(_) = res_chan.send(Ok(responses)) {
            error!(
                "{}: unable to send result, receiver has hung up",
                monitor_name
            );
        }
        debug!("{}: exiting", monitor_name);
    }

    fn handle_message(
        monitor_name: &str,
        cids_of_interest: &HashSet<cid::Cid>,
        event: Result<PushedEvent>,
        first: &mut bool,
        responses: &mut Vec<BroadcastResponse>,
        ready_chan: &mut Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<()> {
        let event = event.context("unable to receive")?;

        if *first {
            *first = false;
            info!("receiving messages from monitor {}...", monitor_name);
            if let Err(_) = ready_chan.take().unwrap().send(()) {
                panic!("{}: unable to signal readiness", monitor_name)
            }
        }

        // Create a constant-width identifier for logging.
        // This makes logging output nicely aligned :)
        let ident = match &event.inner {
            EventType::BitswapMessage(msg) => {
                let mut addrs = msg
                    .connected_addresses
                    .iter()
                    .map(|ma| format!("{}", ma))
                    .collect::<Vec<_>>()
                    .join(", ");
                addrs.truncate(30);
                format!("{:52} [{:30}]", event.peer, addrs,)
            }
            EventType::ConnectionEvent(conn_event) => {
                format!("{:52} {:32}", event.peer, format!("{}", conn_event.remote))
            }
        };

        match event.inner {
            EventType::ConnectionEvent(_) => {
                // ignore
            }
            EventType::BitswapMessage(msg) => {
                // We only care for blocks and block presences.
                if !msg.blocks.is_empty() {
                    for entry in msg.blocks.iter() {
                        let c = cid::Cid::from_str(&entry.path);
                        match c {
                            Ok(c) => {
                                //let c1 = Cid::new_v1(c.codec(), c.hash().clone());
                                if cids_of_interest.contains(&c) {
                                    debug!("{} {:9} {}", ident, "BLOCK", entry.path);
                                    responses.push(BroadcastResponse {
                                        peer: event.peer.clone(),
                                        connected_addrs: msg.connected_addresses.clone(),
                                        timestamp: event.timestamp,
                                        cid: c,
                                        response: BroadcastResponseType::Block,
                                    });
                                }
                            }
                            Err(e) => {
                                error!(
                                    "{}: unable to decode incoming CID {}: {:?}",
                                    monitor_name, entry.path, e
                                )
                            }
                        }
                    }
                }

                if !msg.block_presences.is_empty() {
                    for entry in msg.block_presences.iter() {
                        let c = cid::Cid::from_str(&entry.cid.path);
                        match c {
                            Ok(c) => {
                                //let c = Cid::new_v1(c.codec(), c.hash().clone());
                                if cids_of_interest.contains(&c) {
                                    debug!(
                                        "{} {:9} {}",
                                        ident,
                                        match entry.block_presence_type {
                                            net::TCP_BLOCK_PRESENCE_TYPE_HAVE => "HAVE".to_string(),
                                            net::TCP_BLOCK_PRESENCE_TYPE_DONT_HAVE =>
                                                "DONT_HAVE".to_string(),
                                            _ => format!(
                                                "UNKNOWN_PRESENCE_{}",
                                                entry.block_presence_type
                                            ),
                                        },
                                        entry.cid.path
                                    );
                                    responses.push(BroadcastResponse {
                                        peer: event.peer.clone(),
                                        connected_addrs: msg.connected_addresses.clone(),
                                        timestamp: event.timestamp,
                                        cid: c,
                                        response: BroadcastResponseType::BlockPresence {
                                            presence_type: entry.block_presence_type,
                                        },
                                    });
                                }
                            }
                            Err(e) => {
                                error!(
                                    "{}: unable to decode incoming CID {}: {:?}",
                                    monitor_name, entry.cid.path, e
                                )
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
