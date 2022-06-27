#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

use clap::{App, Arg};
use failure::{err_msg, ResultExt};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use std::{env, path};
use tokio::net::TcpStream;

use crate::config::Config;
use ipfs_monitoring_plugin_client::tcp::{
    EventType, MonitoringClient, BLOCK_PRESENCE_TYPE_DONT_HAVE, BLOCK_PRESENCE_TYPE_HAVE,
};
use ipfs_resolver_common::{logging, wantlist, Result};

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

    run_with_config(cfg).await
}

fn read_geoip_database(cfg: Config) -> Result<maxminddb::Reader<Vec<u8>>> {
    let geoip_db_path = path::Path::new(&cfg.geoip_database_path);

    let country_db_path = geoip_db_path.join("GeoLite2-Country.mmdb");
    debug!(
        "attempting to read GeoLite2 Country database at {:?}...",
        country_db_path
    );
    let country_reader = maxminddb::Reader::open_readfile(country_db_path)
        .context("unable to open GeoLite2 Country database")?;
    debug!("successfully opened GeoLite2 Country database");

    let country_db_ts = chrono::DateTime::<chrono::Utc>::from(
        UNIX_EPOCH + Duration::from_secs(country_reader.metadata.build_epoch),
    );
    debug!(
        "loaded MaxMind country database \"{}\", created {}, with {} entries",
        country_reader.metadata.database_type,
        country_db_ts.format("%+"),
        country_reader.metadata.node_count
    );
    if (chrono::Utc::now() - country_db_ts)
        > chrono::Duration::from_std(Duration::from_secs(30 * 24 * 60 * 60)).unwrap()
    {
        warn!(
            "MaxMind GeoIP country database is older than 30 days (created {})",
            country_db_ts.format("%+")
        )
    }

    debug!("testing MaxMind database...");
    let google_country = country_reader
        .lookup::<maxminddb::geoip2::Country>("8.8.8.8".parse().unwrap())
        .context("unable to look up 8.8.8.8 in Country database")?;
    debug!("got country {:?} for IP 8.8.8.8", google_country);

    Ok(country_reader)
}

async fn run_with_config(cfg: Config) -> Result<()> {
    // Read GeoIP databases
    info!("reading MaxMind GeoLite2 database...");
    let country_db = read_geoip_database(cfg.clone()).context("unable to open GeoIP databases")?;
    let country_db = Arc::new(country_db);
    info!("successfully read MaxMind database");

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
        .monitors
        .into_iter()
        .map(|c| {
            let country_db = country_db.clone();

            tokio::spawn(async move {
                let name = c.name.clone();
                let addr = c.address;

                // Create metrics for a few popular countries ahead of time.
                let mut metrics_by_country =
                    prom::MetricsByMonitorAndCountry::create_basic_set(&name);

                loop {
                    let country_db = country_db.clone();
                    let res =
                        connect_and_receive(&mut metrics_by_country, &name, &addr, country_db)
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
    metrics_by_country: &mut HashMap<String, prom::MetricsByMonitorAndCountry>,
    monitor_name: &str,
    address: &str,
    country_db: Arc<maxminddb::Reader<Vec<u8>>>,
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

                // Extract a multiaddress to use for GeoIP queries.
                let origin_ma = match &event.inner {
                    EventType::BitswapMessage(msg) => {
                        // We filter out any p2p-circuit addresses, since we cannot correctly geolocate those anyway.
                        msg.connected_addresses.iter().find(|a|
                            // Test if any part of the multiaddress is p2p circuit.
                            // Negate that, s.t. we find addresses where no part is p2p circuit.
                            !a.iter().any(|p| if let multiaddr::Protocol::P2pCircuit = p { true } else { false }))
                    }
                    EventType::ConnectionEvent(conn_event) => {
                        if conn_event.remote.iter().any(|p| {
                            if let multiaddr::Protocol::P2pCircuit = p {
                                true
                            } else {
                                false
                            }
                        }) {
                            // This is a relayed connection, ignore it.
                            None
                        } else {
                            Some(&conn_event.remote)
                        }
                    }
                };
                debug!(
                    "{}: extracted origin multiaddress {:?} from event {:?}",
                    monitor_name, origin_ma, event
                );

                let origin_ip = origin_ma
                    .map(|ma| ma.iter().next())
                    .map(|p| match p {
                        None => None,
                        Some(p) => match p {
                            multiaddr::Protocol::Ip4(addr) => Some(IpAddr::V4(addr)),
                            multiaddr::Protocol::Ip6(addr) => Some(IpAddr::V6(addr)),
                            _ => None,
                        },
                    })
                    .flatten();
                debug!(
                    "{}: extracted IP {:?} from multiaddress {:?}",
                    monitor_name, origin_ip, origin_ma
                );

                let origin_country = match origin_ip {
                    None => prom::COUNTRY_NAME_UNKNOWN,
                    Some(ip) => match country_db.lookup::<maxminddb::geoip2::Country>(ip) {
                        Ok(country) => country
                            .country
                            .as_ref()
                            .map(|o| o.iso_code)
                            .flatten()
                            .or_else(|| {
                                debug!(
                                    "Country lookup for IP {} has no country: {:?}",
                                    ip, country
                                );
                                Some(prom::COUNTRY_NAME_UNKNOWN)
                            })
                            .unwrap(),
                        Err(err) => match err {
                            maxminddb::MaxMindDBError::AddressNotFoundError(e) => {
                                debug!(
                                    "{}: IP {:?} not found in MaxMind database: {}",
                                    monitor_name, ip, e
                                );
                                prom::COUNTRY_NAME_UNKNOWN
                            }
                            _ => {
                                error!("unable to lookup country for IP {} (from multiaddress {:?}): {:?}",ip,origin_ma,err);
                                prom::COUNTRY_NAME_ERROR
                            }
                        },
                    },
                };
                debug!(
                    "{}: determined origin of IP {:?} to be {}",
                    monitor_name, origin_ip, origin_country
                );

                let metrics = match metrics_by_country.get(origin_country) {
                    None => {
                        debug!(
                            "{}: country metric for {} missing, creating on the fly...",
                            monitor_name, origin_country
                        );
                        let new_metrics =
                            match prom::MetricsByMonitorAndCountry::new_from_iso3166_alpha2(
                                monitor_name,
                                origin_country,
                            ) {
                                Ok(new_metrics) => new_metrics,
                                Err(e) => {
                                    error!("unable to create country metric for country {} on the fly: {:?}",origin_country,e);
                                    continue;
                                }
                            };
                        // We know that the origin_country value is safe, since we were able to create metrics with it.
                        metrics_by_country.insert(origin_country.to_string(), new_metrics);
                        // We know this is safe since we just inserted it.
                        metrics_by_country.get(origin_country).unwrap()
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
                            ipfs_monitoring_plugin_client::tcp::CONN_EVENT_CONNECTED => {
                                metrics.num_connected.inc();
                                debug!("{} {:12}", ident, "CONNECTED")
                            }
                            ipfs_monitoring_plugin_client::tcp::CONN_EVENT_DISCONNECTED => {
                                metrics.num_disconnected.inc();
                                debug!("{} {:12}", ident, "DISCONNECTED")
                            }
                            _ => {}
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
                                } else if entry.want_type == wantlist::JSON_WANT_TYPE_BLOCK {
                                    if entry.send_dont_have {
                                        metrics.num_entries_want_block_send_dont_have.inc();
                                    } else {
                                        metrics.num_entries_want_block.inc();
                                    }
                                } else if entry.want_type == wantlist::JSON_WANT_TYPE_HAVE {
                                    if entry.send_dont_have {
                                        metrics.num_entries_want_have_send_dont_have.inc();
                                    } else {
                                        metrics.num_entries_want_have.inc();
                                    }
                                } else {
                                    error!("monitor {}: ignoring wantlist entry with invalid want type: {:?}",monitor_name,entry);
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
                                metrics.num_blocks.inc();
                                debug!("{} {:9} {}", ident, "BLOCK", entry.path)
                            }
                        }

                        if !msg.block_presences.is_empty() {
                            for entry in msg.block_presences.iter() {
                                match entry.block_presence_type {
                                    BLOCK_PRESENCE_TYPE_HAVE => {
                                        metrics.num_block_presence_have.inc()
                                    }
                                    BLOCK_PRESENCE_TYPE_DONT_HAVE => {
                                        metrics.num_block_presence_dont_have.inc()
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
