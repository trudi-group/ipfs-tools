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
use wantlist_client_lib::net::{APIClient,  EventType};

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

                let num_connected = prom::CONNECTION_EVENTS
                    .get_metric_with_label_values(&[name.as_str(), "true"])
                    .unwrap();
                let num_disconnected = prom::CONNECTION_EVENTS
                    .get_metric_with_label_values(&[name.as_str(), "false"])
                    .unwrap();

                let num_messages_incremental = prom::MESSAGES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "false"])
                    .unwrap();
                let num_messages_full = prom::MESSAGES_RECEIVED
                    .get_metric_with_label_values(&[name.as_str(), "true"])
                    .unwrap();

                loop {
                    let res = connect_and_receive(
                        &num_cancels,
                        &num_want_block,
                        &num_want_block_send_dont_have,
                        &num_want_have,
                        &num_want_have_send_dont_have,
                        &num_unknown,
                        &num_connected,
                        &num_disconnected,
                        &num_messages_incremental,
                        &num_messages_full,
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
    num_cancels: &GenericCounter<AtomicI64>,
    num_want_block: &GenericCounter<AtomicI64>,
    num_want_block_send_dont_have: &GenericCounter<AtomicI64>,
    num_want_have: &GenericCounter<AtomicI64>,
    num_want_have_send_dont_have: &GenericCounter<AtomicI64>,
    num_unknown: &GenericCounter<AtomicI64>,
    num_connected: &GenericCounter<AtomicI64>,
    num_disconnected: &GenericCounter<AtomicI64>,
    num_messages_incremental: &GenericCounter<AtomicI64>,
    num_messages_full: &GenericCounter<AtomicI64>,
    monitor_name: &str,
    address: &str,
) -> Result<()> {
    debug!("connecting to monitor {} at {}...", monitor_name, address);
    let conn = TcpStream::connect(address).await?;
    info!("connected to monitor {} at {}", monitor_name, address);

    let (client, mut events_in) = APIClient::new(conn).await?;
    for _i in 0..10 {
        let before = std::time::Instant::now();
        client.ping().await.context("unable to ping")?;
        info!("ping took {:?}", before.elapsed())
    }

    client.subscribe().await.context("unable to subscribe to events")?;

    let mut first = true;

    while let Some(event) = events_in.recv().await {
        if first {
            first = false;
            info!("receiving messages from monitor {}...", monitor_name)
        }
        match &event.inner {
            EventType::ConnectionEvent(conn_event) => match conn_event.connection_event_type {
                wantlist_client_lib::net::TCP_CONN_EVENT_CONNECTED => {
                    num_connected.inc();
                    debug!("{:52} {:12} {}", event.peer, "CONNECTED", conn_event.remote)
                }
                wantlist_client_lib::net::TCP_CONN_EVENT_DISCONNECTED => {
                    num_disconnected.inc();
                    debug!(
                        "{:52} {:12} {}",
                        event.peer, "DISCONNECTED", conn_event.remote
                    )
                }
                _ => {}
            },
            EventType::BitswapMessage(msg) => {
                if msg.full_wantlist {
                    num_messages_full.inc();
                } else {
                    num_messages_incremental.inc();
                }

                for entry in msg.wantlist_entries.iter() {
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
                        "{:52} {:4} {:18} ({:10}) {}",
                        event.peer,
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
        }
    }

    info!("monitor {}: disconnected?", monitor_name);

    Ok(()) // I guess?
}
