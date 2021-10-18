#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

use clap::{App, Arg};
use failure::{err_msg, ResultExt};
use std::env;
use tokio::net::TcpStream;

use crate::config::Config;
use ipfs_resolver_common::{logging, wantlist, Result};
use prometheus::core::{AtomicI64, GenericCounter};
use wantlist_client_lib::net::Connection;

mod config;
mod prom;

#[tokio::main]
async fn main() -> Result<()> {
    logging::set_up_logging(false)?;

    // Set up CLI
    let matches = App::new("IPFS Bitswap monitoring real-time analysis tool")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("connects to Bitswap monitoring nodes and analyzes traffic in real-time")
        .arg(
            Arg::with_name("cfg")
                .long("config")
                .value_name("PATH")
                .default_value("config.yaml")
                .help("the config file to load")
                .required(true),
        )
        .get_matches();

    // Read args
    if !matches.is_present("cfg") {
        println!("{}", matches.usage());
        return Err(err_msg("missing config"));
    }
    let cfg = matches.value_of("cfg").unwrap();

    // Read config
    info!("attempting to load config file '{}'", cfg);
    let cfg = Config::open(cfg).context("unable to load config")?;
    debug!("read config {:?}", cfg);

    // Set up prometheus
    let prometheus_address = cfg
        .prometheus_address
        .parse()
        .expect("invalid prometheus_address");

    info!("starting prometheus server");
    prom::run_prometheus(prometheus_address)?;

    // Connect to monitors
    info!("starting infinite connection loop, try Ctrl+C to exit");
    let handles = cfg
        .monitors
        .into_iter()
        .map(|c| {
            tokio::spawn(async move {
                let name = c.name.clone();
                let addr = c.address;

                let num_cancels = prom::ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "cancel", "false"])
                    .unwrap();
                let num_want_block = prom::ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_block", "false"])
                    .unwrap();
                let num_want_block_send_dont_have = prom::ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_block", "true"])
                    .unwrap();
                let num_want_have = prom::ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_have", "false"])
                    .unwrap();
                let num_want_have_send_dont_have = prom::ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_have", "true"])
                    .unwrap();
                let num_unknown = prom::ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "unknown", "false"])
                    .unwrap();

                let num_connected_found = prom::CONNECTION_EVENTS
                    .get_metric_with_label_values(&[name.as_str(), "true", "true"])
                    .unwrap();
                let num_connected_not_found = prom::CONNECTION_EVENTS
                    .get_metric_with_label_values(&[name.as_str(), "true", "false"])
                    .unwrap();
                let num_disconnected_found = prom::CONNECTION_EVENTS
                    .get_metric_with_label_values(&[name.as_str(), "false", "true"])
                    .unwrap();
                let num_disconnected_not_found = prom::CONNECTION_EVENTS
                    .get_metric_with_label_values(&[name.as_str(), "false", "false"])
                    .unwrap();

                let num_messages = prom::MESSAGES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str()])
                    .unwrap();

                loop {
                    let res = connect_and_receive(
                        &num_cancels,
                        &num_want_block,
                        &num_want_block_send_dont_have,
                        &num_want_have,
                        &num_want_have_send_dont_have,
                        &num_unknown,
                        &num_connected_found,
                        &num_connected_not_found,
                        &num_disconnected_found,
                        &num_disconnected_not_found,
                        &num_messages,
                        &name,
                        &addr,
                    )
                    .await;

                    info!("monitor {}: result: {:?}", name, res);

                    info!("monitor {}: sleeping for one second", name);
                    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
                }
            })
        })
        .collect::<Vec<_>>();

    // Sleep forever (probably)
    for handle in handles {
        handle.await.context("connection loop failed")?;
    }

    Ok(())
}

async fn connect_and_receive(
    num_cancels: &GenericCounter<AtomicI64>,
    num_want_block: &GenericCounter<AtomicI64>,
    num_want_block_send_dont_have: &GenericCounter<AtomicI64>,
    num_want_have: &GenericCounter<AtomicI64>,
    num_want_have_send_dont_have: &GenericCounter<AtomicI64>,
    num_unknown: &GenericCounter<AtomicI64>,
    num_connected_found: &GenericCounter<AtomicI64>,
    num_connected_not_found: &GenericCounter<AtomicI64>,
    num_disconnected_found: &GenericCounter<AtomicI64>,
    num_disconnected_not_found: &GenericCounter<AtomicI64>,
    num_messages: &GenericCounter<AtomicI64>,
    monitor_name: &str,
    address: &str,
) -> Result<()> {
    debug!("connecting to monitor {} at {}...", monitor_name, address);
    let conn = TcpStream::connect(address).await?;
    info!("connected to monitor {} at {}", monitor_name, address);

    let client = Connection::new(conn)?;
    let _remote = client.remote;
    let mut messages_in = client.messages_in;
    let mut first = true;

    while let Some(wl) = messages_in.recv().await {
        if first {
            first = false;
            info!("receiving messages from monitor {}...", monitor_name)
        }
        if wl.peer_connected.is_some() && wl.peer_connected.unwrap() {
            // Unwrap this because I hope that works...
            if wl.connect_event_peer_found.unwrap() {
                num_connected_found.inc();
            } else {
                num_connected_not_found.inc();
            }
            debug!(
                "{} {:38} {:25}",
                wl.peer,
                match &wl.address {
                    Some(address) => address.to_string(),
                    None => "".to_string(),
                },
                if wl.connect_event_peer_found.unwrap() {
                    "CONNECTED; FOUND"
                } else {
                    "CONNECTED; NOT FOUND"
                }
            )
        } else if wl.peer_disconnected.is_some() && wl.peer_disconnected.unwrap() {
            if wl.connect_event_peer_found.unwrap() {
                num_disconnected_found.inc();
            } else {
                num_disconnected_not_found.inc();
            }
            debug!(
                "{} {:38} {:25}",
                wl.peer,
                match &wl.address {
                    Some(address) => address.to_string(),
                    None => "".to_string(),
                },
                if wl.connect_event_peer_found.unwrap() {
                    "DISCONNECTED; FOUND"
                } else {
                    "DISCONNECTED; NOT FOUND"
                }
            )
        } else if wl.received_entries.is_some() {
            num_messages.inc();

            match wl.received_entries {
                Some(entries) => {
                    for entry in entries.iter() {
                        if entry.cancel {
                            num_cancels.inc();
                        } else if entry.want_type == wantlist::JSON_WANT_TYPE_BLOCK {
                            if entry.send_dont_have {
                                num_want_block_send_dont_have.inc();
                            } else {
                                num_want_block.inc();
                            }
                        } else if entry.want_type == wantlist::JSON_WANT_TYPE_HAVE {
                            if entry.send_dont_have {
                                num_want_have_send_dont_have.inc();
                            } else {
                                num_want_have.inc();
                            }
                        } else {
                            num_unknown.inc();
                        }

                        debug!(
                            "{} {:38} {:4} {:25} ({:10}) {}",
                            wl.peer,
                            match &wl.address {
                                Some(address) => address.to_string(),
                                None => "".to_string(),
                            },
                            match wl.full_want_list {
                                Some(f) =>
                                    if f {
                                        "FULL"
                                    } else {
                                        "INC"
                                    },
                                None => {
                                    "???"
                                }
                            },
                            if entry.cancel {
                                "CANCEL".to_string()
                            } else if entry.want_type == wantlist::JSON_WANT_TYPE_BLOCK {
                                if entry.send_dont_have {
                                    "WANT_BLOCK|SEND_DONT_HAVE".to_string()
                                } else {
                                    "WANT_BLOCK".to_string()
                                }
                            } else if entry.want_type == wantlist::JSON_WANT_TYPE_HAVE {
                                if entry.send_dont_have {
                                    "WANT_HAVE|SEND_DONT_HAVE".to_string()
                                } else {
                                    "WANT_HAVE".to_string()
                                }
                            } else {
                                format!("WANT_UNKNOWN_TYPE_{}", entry.want_type)
                            },
                            entry.priority,
                            entry.cid.path
                        )
                    }
                }
                None => debug!("empty entries"),
            }
        } else {
            warn!(
                "monitor {}: message has no connect/disconnect event and no entries?",
                monitor_name
            )
        }
    }

    info!("monitor {}: disconnected?", monitor_name);

    Ok(()) // I guess?
}
