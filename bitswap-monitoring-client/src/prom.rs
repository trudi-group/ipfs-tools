use celes::EmptyLookupTable;
use failure::{err_msg, ResultExt};
use ipfs_resolver_common::Result;
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::IntCounterVec;
use std::collections::HashMap;
use std::net::SocketAddr;

lazy_static! {
        pub static ref BITSWAP_MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_messages_received",
        "number of bitswap messages (both requests and responses) received by monitor and origin country",
        &["monitor","origin_country","origin_is_gateway"]
    )
    .unwrap();

    pub static ref BITSWAP_BLOCKS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_blocks_received",
        "number of blocks received via bitswap, by monitor and origin country",
        &["monitor","origin_country","origin_is_gateway"]
    )
    .unwrap();

    pub static ref BITSWAP_BLOCK_PRESENCES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_block_presences_received",
        "number of block presences received via bitswap, by monitor, presence type, and origin country",
        &["monitor","presence_type","origin_is_gateway","origin_country"]
    )
    .unwrap();

    pub static ref WANTLIST_ENTRIES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by monitor, entry type, send_dont_have, and origin country",
        &["monitor","entry_type","origin_is_gateway","send_dont_have","origin_country"]
    )
    .unwrap();

    pub static ref WANTLISTS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlists_received",
        "number of bitswap messages received for which the wantlist was not empty, by monitor, whether the wantlist was a full wantlist, and origin country",
        &["monitor","full","origin_is_gateway","origin_country"]
    )
    .unwrap();

    pub static ref CONNECTION_EVENTS_CONNECTED: IntCounterVec = register_int_counter_vec!(
        "connection_events_connected",
        "number of connect events by monitor and origin country",
        &["monitor","origin_country","origin_is_gateway"]
    )
    .unwrap();

    pub static ref CONNECTION_EVENTS_DISCONNECTED: IntCounterVec = register_int_counter_vec!(
        "connection_events_disconnected",
        "number of disconnect events by monitor and origin country",
        &["monitor","origin_country","origin_is_gateway"]
    )
    .unwrap();
}
pub(crate) static COUNTRY_NAME_UNKNOWN: &'static str = "Unknown";
pub(crate) static COUNTRY_NAME_ERROR: &'static str = "Error";
// /// Country constants for various error conditions.
// pub(crate) static COUNTRY_NAME_UNKNOWN: celes::Country = celes::Country {
//     code: "Unkown",
//     value: 0,
//     alpha2: "Unkown",
//     alpha3: "Unkown",
//     long_name: "Unkown",
//     aliases: celes::CountryTable::Empty(EmptyLookupTable([])),
// };
// pub(crate) static COUNTRY_NAME_ERROR: celes::Country = celes::Country {
//     code: "Error",
//     value: 0,
//     alpha2: "Error",
//     alpha3: "Error",
//     long_name: "Error",
//     aliases: celes::CountryTable::Empty(EmptyLookupTable([])),
// };

/// A set of metrics instantiated by monitor name and country.
pub(crate) struct Metrics {
    /// Counter for Bitswap messages.
    pub(crate) num_messages: GenericCounter<AtomicU64>,

    /// Counter for blocks received via Bitswap.
    pub(crate) num_blocks: GenericCounter<AtomicU64>,

    /// Counters for block presences received via Bitswap, by presence type.
    pub(crate) num_block_presence_have: GenericCounter<AtomicU64>,
    pub(crate) num_block_presence_dont_have: GenericCounter<AtomicU64>,

    /// Counters for Bitswap messages containing a wantlist.
    pub(crate) num_wantlists_incremental: GenericCounter<AtomicU64>,
    pub(crate) num_wantlists_full: GenericCounter<AtomicU64>,

    /// Counters for wantlist entries by type.
    pub(crate) num_entries_cancel: GenericCounter<AtomicU64>,
    pub(crate) num_entries_want_block: GenericCounter<AtomicU64>,
    pub(crate) num_entries_want_block_send_dont_have: GenericCounter<AtomicU64>,
    pub(crate) num_entries_want_have: GenericCounter<AtomicU64>,
    pub(crate) num_entries_want_have_send_dont_have: GenericCounter<AtomicU64>,

    /// Counters for connection events.
    pub(crate) num_connected: GenericCounter<AtomicU64>,
    pub(crate) num_disconnected: GenericCounter<AtomicU64>,
}

/// Distinguish traffic by the type of the origin
/// If the origin is a real IPFS-note the type should be Peer
/// If the origin is a HTTP to IPFS proxy the type should be Gateway
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum OriginType {
    Peer,
    Gateway,
}

/// Represents a country with its alpha2-code
/// If the country is not known the type should be Unkown
/// If the country could not be found because of an error the type should be Error
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum CountryIdenifier {
    Unkown,
    Error,
    Alpha2(String),
}

impl Metrics {
    /// Creates a set of metrics consisting of a few popular countries and the special
    /// error-condition countries, for the given monitor.
    pub(crate) fn create_basic_set(
        monitor_name: &str,
    ) -> HashMap<(CountryIdenifier, OriginType), Metrics> {
        let mut metrics_by_country = [
            celes::Country::germany(),
            celes::Country::the_united_states_of_america(),
            celes::Country::the_netherlands(),
        ]
        .into_iter()
        .map(|c| [(c, OriginType::Peer), (c, OriginType::Gateway)].into_iter())
        .flatten()
        .map(|ct| {
            (
                (CountryIdenifier::Alpha2(ct.0.alpha2.to_string()), ct.1),
                Self::new_for_country_name(monitor_name, ct.0.long_name),
            )
        })
        .chain(
            [CountryIdenifier::Unkown, CountryIdenifier::Error]
                .into_iter()
                .map(|c| [(c, OriginType::Peer), (c, OriginType::Gateway)].into_iter())
                .flatten()
                .map(|ct| {
                    (
                        ct,
                        Self::new_for_countryidentifier(monitor_name, ct.0).unwrap(),
                    ) //the unwrap is ok because the error can only happen if we the arg has type Alpha2
                }),
        )
        .collect::<HashMap<_, _>>();

        metrics_by_country
    }

    /// Creates a set of metrics with their `origin_country` set to "Unknown".
    /// This is used for events for which no origin can be determined.
    fn new_unknown(monitor_name: &str) -> Metrics {
        Self::new_for_country_name(monitor_name, COUNTRY_NAME_UNKNOWN)
    }

    /// Creates a set of metrics with their `origin_country` set to "Error".
    /// This is used for events for which determining the origin country failed.
    fn new_error(monitor_name: &str) -> Metrics {
        Self::new_for_country_name(monitor_name, COUNTRY_NAME_ERROR)
    }

    /// Creates a new set of metrics for the country identified by `country_code`.
    /// If `country_code` is not a valid 2-letter ISO3166-1 code, an error is returned.
    pub(crate) fn new_for_countryidentifier(
        monitor_name: &str,
        country_id: CountryIdenifier,
    ) -> Result<Metrics> {
        match country_id {
            CountryIdenifier::Alpha2(country_code) => {
                let country = celes::Country::from_alpha2(country_code)
                    .map_err(|e| err_msg(format!("{}", e)))
                    .context("invalid country code")?;
                Ok(Self::new_for_country_name(monitor_name, country.long_name))
            }
            CountryIdenifier::Unkown => Ok(Self::new_unknown(monitor_name)),
            CountryIdenifier::Error => Ok(Self::new_error(monitor_name)),
        }
    }

    /// Creates a new set of metrics for the country
    fn new_for_country_name(monitor_name: &str, country_name: &str) -> Metrics {
        Metrics {
            num_messages: BITSWAP_MESSAGES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, country_name])
                .unwrap(),

            num_entries_cancel: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "cancel", "false", country_name])
                .unwrap(),
            num_entries_want_block: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "want_block", "false", country_name])
                .unwrap(),
            num_entries_want_block_send_dont_have: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "want_block", "true", country_name])
                .unwrap(),
            num_entries_want_have: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "want_have", "false", country_name])
                .unwrap(),
            num_entries_want_have_send_dont_have: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "want_have", "true", country_name])
                .unwrap(),

            num_connected: CONNECTION_EVENTS_CONNECTED
                .get_metric_with_label_values(&[monitor_name, country_name])
                .unwrap(),
            num_disconnected: CONNECTION_EVENTS_DISCONNECTED
                .get_metric_with_label_values(&[monitor_name, country_name])
                .unwrap(),
            num_wantlists_incremental: WANTLISTS_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "false", country_name])
                .unwrap(),
            num_wantlists_full: WANTLISTS_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "true", country_name])
                .unwrap(),
            num_blocks: BITSWAP_BLOCKS_RECEIVED
                .get_metric_with_label_values(&[monitor_name, country_name])
                .unwrap(),
            num_block_presence_have: BITSWAP_BLOCK_PRESENCES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "HAVE", country_name])
                .unwrap(),
            num_block_presence_dont_have: BITSWAP_BLOCK_PRESENCES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "DONT_HAVE", country_name])
                .unwrap(),
        }
    }
}

pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    prometheus_exporter::start(addr).context("can not start exporter")?;

    Ok(())
}
