#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

use clap::{App, Arg};
use failure::{err_msg, ResultExt};
use futures_util::StreamExt;
use std::env;
use tokio::net::TcpStream;

use crate::config::Config;
use ipfs_monitoring_plugin_client::tcp::{
    EventType, MonitoringClient, BLOCK_PRESENCE_TYPE_DONT_HAVE, BLOCK_PRESENCE_TYPE_HAVE,
};
use ipfs_resolver_common::{logging, wantlist, Result};
use prometheus::core::{AtomicI64, GenericCounter};

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

                let num_messages = prom::BITSWAP_MESSAGES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str()])
                    .unwrap();

                let num_cancels = prom::WANTLIST_ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "cancel", "false"])
                    .unwrap();
                let num_want_block = prom::WANTLIST_ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_block", "false"])
                    .unwrap();
                let num_want_block_send_dont_have = prom::WANTLIST_ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_block", "true"])
                    .unwrap();
                let num_want_have = prom::WANTLIST_ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_have", "false"])
                    .unwrap();
                let num_want_have_send_dont_have = prom::WANTLIST_ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "want_have", "true"])
                    .unwrap();
                let num_unknown = prom::WANTLIST_ENTRIES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "unknown", "false"])
                    .unwrap();

                let num_connected = prom::CONNECTION_EVENTS_CONNECTED
                    .get_metric_with_label_values(&[name.as_str()])
                    .unwrap();
                let num_disconnected = prom::CONNECTION_EVENTS_DISCONNECTED
                    .get_metric_with_label_values(&[name.as_str()])
                    .unwrap();

                let num_wl_messages_incremental = prom::WANTLIST_MESSAGES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "false"])
                    .unwrap();
                let num_wl_messages_full = prom::WANTLIST_MESSAGES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "true"])
                    .unwrap();

                let num_blocks = prom::BITSWAP_BLOCKS_RECEIVED
                    .get_metric_with_label_values(&[name.as_str()])
                    .unwrap();

                let num_block_presence_have = prom::BITSWAP_BLOCK_PRESENCES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "HAVE"])
                    .unwrap();
                let num_block_presence_dont_have = prom::BITSWAP_BLOCK_PRESENCES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "DONT_HAVE"])
                    .unwrap();

                loop {
                    let res = connect_and_receive(
                        &num_messages,
                        &num_cancels,
                        &num_want_block,
                        &num_want_block_send_dont_have,
                        &num_want_have,
                        &num_want_have_send_dont_have,
                        &num_unknown,
                        &num_connected,
                        &num_disconnected,
                        &num_wl_messages_incremental,
                        &num_wl_messages_full,
                        &num_blocks,
                        &num_block_presence_have,
                        &num_block_presence_dont_have,
                        &name,
                        &addr,
                    )
                    .await;

                    info!("monitor {}: result: {:?}", name, res);

                    info!("monitor {}: sleeping for one second", name);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
    num_messages: &GenericCounter<AtomicI64>,
    num_entry_cancels: &GenericCounter<AtomicI64>,
    num_entry_want_block: &GenericCounter<AtomicI64>,
    num_entry_want_block_send_dont_have: &GenericCounter<AtomicI64>,
    num_entry_want_have: &GenericCounter<AtomicI64>,
    num_entry_want_have_send_dont_have: &GenericCounter<AtomicI64>,
    num_entry_unknown: &GenericCounter<AtomicI64>,
    num_connected: &GenericCounter<AtomicI64>,
    num_disconnected: &GenericCounter<AtomicI64>,
    num_wl_messages_incremental: &GenericCounter<AtomicI64>,
    num_wl_messages_full: &GenericCounter<AtomicI64>,
    num_blocks: &GenericCounter<AtomicI64>,
    num_block_presence_have: &GenericCounter<AtomicI64>,
    num_block_presence_dont_have: &GenericCounter<AtomicI64>,
    monitor_name: &str,
    address: &str,
) -> Result<()> {
    debug!("connecting to monitor {} at {}...", monitor_name, address);
    let conn = TcpStream::connect(address).await?;
    info!("connected to monitor {} at {}", monitor_name, address);

    let mut client = MonitoringClient::new(conn).await?;

    let mut first = true;

    while let Some(event) = client.next().await {
        match event {
            Err(err) => {
                error!("unable to receive events: {}", err);
                break;
            }
            Ok(event) => {
                if first {
                    first = false;
                    info!("receiving messages from monitor {}...", monitor_name)
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

                match &event.inner {
                    EventType::ConnectionEvent(conn_event) => {
                        match conn_event.connection_event_type {
                            ipfs_monitoring_plugin_client::tcp::CONN_EVENT_CONNECTED => {
                                num_connected.inc();
                                debug!("{} {:12}", ident, "CONNECTED")
                            }
                            ipfs_monitoring_plugin_client::tcp::CONN_EVENT_DISCONNECTED => {
                                num_disconnected.inc();
                                debug!("{} {:12}", ident, "DISCONNECTED")
                            }
                            _ => {}
                        }
                    }
                    EventType::BitswapMessage(msg) => {
                        num_messages.inc();

                        if !msg.wantlist_entries.is_empty() {
                            if msg.full_wantlist {
                                num_wl_messages_full.inc();
                            } else {
                                num_wl_messages_incremental.inc();
                            }

                            for entry in msg.wantlist_entries.iter() {
                                if entry.cancel {
                                    num_entry_cancels.inc();
                                } else if entry.want_type == wantlist::JSON_WANT_TYPE_BLOCK {
                                    if entry.send_dont_have {
                                        num_entry_want_block_send_dont_have.inc();
                                    } else {
                                        num_entry_want_block.inc();
                                    }
                                } else if entry.want_type == wantlist::JSON_WANT_TYPE_HAVE {
                                    if entry.send_dont_have {
                                        num_entry_want_have_send_dont_have.inc();
                                    } else {
                                        num_entry_want_have.inc();
                                    }
                                } else {
                                    num_entry_unknown.inc();
                                }

                                debug!(
                                    "{} {:4} {:18} ({:10}) {}",
                                    ident,
                                    if msg.full_wantlist { "FULL" } else { "INC" },
                                    if entry.cancel {
                                        "CANCEL".to_string()
                                    } else if entry.want_type == wantlist::JSON_WANT_TYPE_BLOCK {
                                        if entry.send_dont_have {
                                            "WANT_BLOCK|SEND_DH".to_string()
                                        } else {
                                            "WANT_BLOCK".to_string()
                                        }
                                    } else if entry.want_type == wantlist::JSON_WANT_TYPE_HAVE {
                                        if entry.send_dont_have {
                                            "WANT_HAVE|SEND_DH".to_string()
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

                        if !msg.blocks.is_empty() {
                            for entry in msg.blocks.iter() {
                                num_blocks.inc();
                                debug!("{} {:9} {}", ident, "BLOCK", entry.path)
                            }
                        }

                        if !msg.block_presences.is_empty() {
                            for entry in msg.block_presences.iter() {
                                match entry.block_presence_type {
                                    BLOCK_PRESENCE_TYPE_HAVE => num_block_presence_have.inc(),
                                    BLOCK_PRESENCE_TYPE_DONT_HAVE => {
                                        num_block_presence_dont_have.inc()
                                    }
                                    _ => {
                                        warn!(
                                            "monitor {} sent unknown block presence type {}: {:?}",
                                            monitor_name, entry.block_presence_type, entry
                                        )
                                    }
                                }
                                debug!(
                                    "{} {:9} {}",
                                    ident,
                                    match entry.block_presence_type {
                                        BLOCK_PRESENCE_TYPE_HAVE => "HAVE".to_string(),
                                        BLOCK_PRESENCE_TYPE_DONT_HAVE => "DONT_HAVE".to_string(),
                                        _ => format!(
                                            "UNKNOWN_PRESENCE_{}",
                                            entry.block_presence_type
                                        ),
                                    },
                                    entry.cid.path
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    info!("monitor {}: disconnected?", monitor_name);

    Ok(()) // I guess?
}
