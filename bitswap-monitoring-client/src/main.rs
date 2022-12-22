#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

use crate::config::Config;
use crate::prom::{MetricsKey, PublicGatewayStatus};
use clap::{App, Arg};
use failure::{err_msg, ResultExt};
use futures_util::StreamExt;
use ipfs_monitoring_plugin_client::monitoring::{
    BlockPresenceType, EventType, MonitoringClient, RoutingKeyInformation,
};
use ipfs_resolver_common::wantlist::JSONWantType;
use ipfs_resolver_common::{logging, Result};
use prom::{Geolocation, Metrics};
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

mod config;
mod gateways;
mod geolocation;
mod prom;

#[tokio::main]
async fn main() -> Result<()> {
    logging::set_up_logging()?;

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

    run_with_config(cfg).await
}

async fn run_with_config(cfg: Config) -> Result<()> {
    // Read GeoIP databases.
    info!("reading MaxMind GeoLite2 database...");
    let country_db =
        geolocation::read_geoip_database(cfg.clone()).context("unable to open GeoIP databases")?;
    let country_db = Arc::new(country_db);
    info!("successfully read MaxMind database");

    // Read list of public gateway IDs.
    let known_gateways = Arc::new(RwLock::new(HashSet::new()));
    match cfg.gateway_file_path {
        Some(path) => {
            debug!("loading gateway IDs from {}", path);
            gateways::update_known_gateways(&path, &known_gateways)
                .await
                .context("unable to load gateway IDs")?;
            info!("loaded {} gateway IDs", known_gateways.read().await.len());

            debug!("starting loop to handle SIGUSR1");
            let known_gateways = known_gateways.clone();
            gateways::set_up_signal_handling(path.clone(), known_gateways)
                .context("unable to set up signal handling to reload gateway IDs")?;
            info!("started signal handler. Send SIGUSR1 to reload list of gateways.");
        }
        None => {
            info!("no gateway file provided, all traffic will be logged as non-gateway")
        }
    }

    // Set up prometheus
    let prometheus_address = cfg
        .prometheus_address
        .parse()
        .expect("invalid prometheus_address");

    debug!("starting prometheus server");
    prom::run_prometheus(prometheus_address)?;
    info!("started prometheus server");

    // Connect to monitors
    info!("starting infinite connection loop, try Ctrl+C to exit");
    let handles = cfg
        .amqp_servers
        .into_iter()
        .map(|c| {
            c.monitor_names
                .into_iter()
                .map(|name| {
                    let name = name.clone();
                    let country_db = country_db.clone();
                    let known_gateways = known_gateways.clone();
                    let amqp_server_address = c.amqp_server_address.clone();

                    tokio::spawn(async move {
                        // Create metrics for a few popular countries ahead of time.
                        let mut metrics_by_country = Metrics::create_basic_set(&name);
                        let routing_keys = vec![
                            RoutingKeyInformation::BitswapMessages {
                                monitor_name: name.clone(),
                            },
                            RoutingKeyInformation::ConnectionEvents {
                                monitor_name: name.clone(),
                            },
                        ];

                        loop {
                            let country_db = country_db.clone();
                            let res = connect_and_receive(
                                &mut metrics_by_country,
                                &name,
                                &amqp_server_address,
                                &routing_keys,
                                country_db,
                                &known_gateways,
                            )
                            .await;

                            info!(
                                "server {}, monitor {}: result: {:?}",
                                amqp_server_address, name, res
                            );

                            info!(
                                "server {}, monitor {}: sleeping for one second",
                                amqp_server_address, name
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    })
                })
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect::<Vec<_>>();

    // Sleep forever (probably)
    for handle in handles {
        handle.await.context("connection loop failed")?;
    }

    Ok(())
}

async fn connect_and_receive(
    metrics_by_country: &mut prom::MetricsMap,
    monitor_name: &str,
    amqp_server_address: &str,
    routing_keys: &[RoutingKeyInformation],
    country_db: Arc<maxminddb::Reader<Vec<u8>>>,
    known_gateways: &Arc<RwLock<HashSet<String>>>,
) -> Result<()> {
    debug!(
        "connecting to AMQP server {} at {} and subscribing to events for monitor {}...",
        monitor_name, amqp_server_address, monitor_name
    );
    let mut client = MonitoringClient::new(amqp_server_address, &routing_keys).await?;
    info!(
        "connected for monitor {} at {}",
        monitor_name, amqp_server_address
    );

    let mut first = true;

    while let Some(events) = client.next().await {
        match events {
            Err(err) => {
                error!("unable to receive events: {}", err);
                break;
            }
            Ok((_, events)) => {
                if first {
                    first = false;
                    info!("receiving messages for monitor {}...", monitor_name)
                }

                for event in events {
                    let geolocation = geolocation::geolocate_event(&country_db, &event);
                    debug!(
                        "{}: determined origin of event {:?} to be {:?}",
                        monitor_name, event, geolocation
                    );

                    let origin_type = if known_gateways.read().await.contains(&event.peer) {
                        PublicGatewayStatus::Gateway
                    } else {
                        PublicGatewayStatus::NonGateway
                    };

                    let metrics_key = MetricsKey {
                        geo_origin: geolocation,
                        overlay_origin: origin_type,
                    };

                    let metrics = match metrics_by_country.get(&metrics_key) {
                        None => {
                            debug!(
                                "{}: metrics for {:?} missing, creating on the fly...",
                                monitor_name, metrics_key
                            );
                            match Metrics::new_for_key(monitor_name, &metrics_key) {
                                Ok(new_metrics) => {
                                    // We know that the metrics_key value is safe, since we were able to create metrics with it.
                                    metrics_by_country.insert(metrics_key.clone(), new_metrics);
                                    // We know this is safe since we just inserted it.
                                    metrics_by_country.get(&metrics_key).unwrap()
                                }
                                Err(e) => {
                                    error!(
                                    "unable to create metrics for country {:?} on the fly: {:?}",
                                    metrics_key, e
                                );
                                    // We use the Error country instead.
                                    // We know this is safe since that country is always present in the map.
                                    metrics_by_country
                                        .get(&MetricsKey {
                                            geo_origin: Geolocation::Error,
                                            overlay_origin: metrics_key.overlay_origin,
                                        })
                                        .unwrap()
                                }
                            }
                        }
                        Some(m) => m,
                    };

                    // Create a constant-width identifier for logging.
                    // This makes logging output nicely aligned :)
                    // We only use this in debug logging, so we only create it if debug logging is enabled.
                    let ident = if log_enabled!(log::Level::Debug) {
                        event.constant_width_identifier()
                    } else {
                        "".to_string()
                    };

                    match &event.inner {
                        EventType::ConnectionEvent(conn_event) => {
                            match conn_event.connection_event_type {
                                ipfs_monitoring_plugin_client::monitoring::ConnectionEventType::Connected => {
                                    metrics.num_connected.inc();
                                    debug!("{} {:12}", ident, "CONNECTED")
                                }
                                ipfs_monitoring_plugin_client::monitoring::ConnectionEventType::Disconnected => {
                                    metrics.num_disconnected.inc();
                                    debug!("{} {:12}", ident, "DISCONNECTED")
                                }
                            }
                        }
                        EventType::BitswapMessage(msg) => {
                            metrics.num_messages.inc();

                            if !msg.wantlist_entries.is_empty() {
                                if msg.full_wantlist {
                                    metrics.num_wantlists_full.inc();
                                } else {
                                    metrics.num_wantlists_incremental.inc();
                                }

                                for entry in msg.wantlist_entries.iter() {
                                    if entry.cancel {
                                        metrics.num_entries_cancel.inc();
                                    } else {
                                        match entry.want_type {
                                            JSONWantType::Block => {
                                                if entry.send_dont_have {
                                                    metrics
                                                        .num_entries_want_block_send_dont_have
                                                        .inc();
                                                } else {
                                                    metrics.num_entries_want_block.inc();
                                                }
                                            }
                                            JSONWantType::Have => {
                                                if entry.send_dont_have {
                                                    metrics
                                                        .num_entries_want_have_send_dont_have
                                                        .inc();
                                                } else {
                                                    metrics.num_entries_want_have.inc();
                                                }
                                            }
                                        }
                                    }

                                    debug!(
                                        "{} {:4} {:18} ({:10}) {}",
                                        ident,
                                        if msg.full_wantlist { "FULL" } else { "INC" },
                                        if entry.cancel {
                                            "CANCEL".to_string()
                                        } else {
                                            match entry.want_type {
                                                JSONWantType::Block => {
                                                    if entry.send_dont_have {
                                                        "WANT_BLOCK|SEND_DH".to_string()
                                                    } else {
                                                        "WANT_BLOCK".to_string()
                                                    }
                                                }
                                                JSONWantType::Have => {
                                                    if entry.send_dont_have {
                                                        "WANT_HAVE|SEND_DH".to_string()
                                                    } else {
                                                        "WANT_HAVE".to_string()
                                                    }
                                                }
                                            }
                                        },
                                        entry.priority,
                                        entry.cid.path
                                    )
                                }
                            }

                            if !msg.blocks.is_empty() {
                                for entry in msg.blocks.iter() {
                                    metrics.num_blocks.inc();
                                    debug!("{} {:9} {}", ident, "BLOCK", entry.path)
                                }
                            }

                            if !msg.block_presences.is_empty() {
                                for entry in msg.block_presences.iter() {
                                    match entry.block_presence_type {
                                        BlockPresenceType::Have => {
                                            metrics.num_block_presence_have.inc()
                                        }
                                        BlockPresenceType::DontHave => {
                                            metrics.num_block_presence_dont_have.inc()
                                        }
                                    }
                                    debug!(
                                        "{} {:9} {}",
                                        ident,
                                        match entry.block_presence_type {
                                            BlockPresenceType::Have => "HAVE".to_string(),
                                            BlockPresenceType::DontHave => "DONT_HAVE".to_string(),
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
    }

    info!("monitor {}: disconnected?", monitor_name);

    Ok(()) // I guess?
}
