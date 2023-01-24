use crate::{Config, Geolocation};
use failure::ResultExt;
use ipfs_monitoring_plugin_client::monitoring::{EventType, PushedEvent};
use maxminddb::Reader;
use std::net::IpAddr;
use std::path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::Result;

pub(crate) fn read_geoip_database(cfg: Config) -> Result<maxminddb::Reader<Vec<u8>>> {
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

pub(crate) fn geolocate_event(
    country_db: &Arc<Reader<Vec<u8>>>,
    event: &PushedEvent,
) -> Geolocation {
    // Extract a multiaddress to use for GeoIP queries.
    let origin_ma = match &event.inner {
        EventType::BitswapMessage(msg) => {
            // We filter out any p2p-circuit addresses, since we cannot correctly geolocate those anyway.
            msg.connected_addresses.iter().find(|a|
                // Parse the multiaddress
                match multiaddr::Multiaddr::from_str(a) {
                    Ok(ma) => {
                        // Test if any part of the multiaddress is p2p circuit.
                        // Negate that, s.t. we find addresses where no part is p2p circuit.
                        !ma.iter().any(|p| if let multiaddr::Protocol::P2pCircuit = p { true } else { false })
                    }
                    Err(err) => {
                        // Probably a new protocol which we can't decode (yet)
                        debug!("unable to decode multiaddress {}: {:?}",a,err);
                        false
                    }
                }
            ).map(|a| multiaddr::Multiaddr::from_str(a).unwrap())
        }
        EventType::ConnectionEvent(conn_event) => {
            // Parse the multiaddress
            match multiaddr::Multiaddr::from_str(&conn_event.remote) {
                Ok(ma) => {
                    if ma.iter().any(|p| {
                        if let multiaddr::Protocol::P2pCircuit = p {
                            true
                        } else {
                            false
                        }
                    }) {
                        // This is a relayed connection, ignore it.
                        None
                    } else {
                        Some(ma)
                    }
                }
                Err(err) => {
                    // Probably a new protocol which we can't decode (yet)
                    debug!(
                        "unable to decode multiaddress {}: {:?}",
                        conn_event.remote, err
                    );
                    None
                }
            }
        }
    };
    debug!(
        "extracted origin multiaddress {:?} from event {:?}",
        origin_ma, event
    );

    let origin_ip = origin_ma
        .as_ref()
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
        "extracted IP {:?} from multiaddress {:?}",
        origin_ip, origin_ma
    );

    let geolocation: Geolocation = match origin_ip {
        None => Geolocation::Unknown,
        Some(ip) => match country_db.lookup::<maxminddb::geoip2::Country>(ip) {
            Ok(country) => country
                .country
                .as_ref()
                .map(|o| o.iso_code)
                .flatten()
                .map(|iso_code| Geolocation::Alpha2(iso_code.to_string()))
                .or_else(|| {
                    debug!("Country lookup for IP {} has no country: {:?}", ip, country);
                    Some(Geolocation::Unknown)
                })
                .unwrap(),
            Err(err) => match err {
                maxminddb::MaxMindDBError::AddressNotFoundError(e) => {
                    debug!("IP {:?} not found in MaxMind database: {}", ip, e);
                    Geolocation::Unknown
                }
                _ => {
                    error!(
                        "unable to lookup country for IP {} (from multiaddress {:?}): {:?}",
                        ip, origin_ma, err
                    );
                    Geolocation::Error
                }
            },
        },
    };
    debug!(
        " determined origin of IP {:?} to be {:?}",
        origin_ip, geolocation
    );

    geolocation
}
