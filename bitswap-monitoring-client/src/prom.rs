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
        &["monitor","presence_type","origin_country","origin_is_gateway"]
    )
    .unwrap();

    pub static ref WANTLIST_ENTRIES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by monitor, entry type, send_dont_have, and origin country",
        &["monitor","entry_type","send_dont_have","origin_country","origin_is_gateway"]
    )
    .unwrap();

    pub static ref WANTLISTS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlists_received",
        "number of bitswap messages received for which the wantlist was not empty, by monitor, whether the wantlist was a full wantlist, and origin country",
        &["monitor","full","origin_country","origin_is_gateway"]
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

/// Country constants for various error conditions.
pub(crate) static COUNTRY_NAME_UNKNOWN: &'static str = "Unknown";
pub(crate) static COUNTRY_NAME_ERROR: &'static str = "Error";

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

/// Classification of IPFS nodes into gateways and non-gateways.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub(crate) enum PublicGatewayStatus {
    /// The node does not provide a public IPFS<->HTTP gateway.
    NonGateway,

    /// The node provides a public IPFS<->HTTP gateway.
    Gateway,
}

impl PublicGatewayStatus {
    fn is_gateway_str(&self) -> &'static str {
        match self {
            PublicGatewayStatus::NonGateway => "false",
            PublicGatewayStatus::Gateway => "true",
        }
    }
}

/// Represents geolocation of an IPFS node.
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub(crate) enum Geolocation {
    /// The location could not be determined, e.g., because geolocation data for the address is
    /// not available.
    Unknown,

    /// The location could not be determined because an error occurred.
    Error,

    /// ISO 3166-1 two-letter code, e.g. "US", "DE", "NL".
    Alpha2(String),
}

/// The key type for metrics.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct MetricsKey {
    pub geo_origin: Geolocation,
    pub overlay_origin: PublicGatewayStatus,
}

pub(crate) type MetricsMap = HashMap<MetricsKey, Metrics>;

impl Metrics {
    /// Creates a set of metrics consisting of a few popular countries and the special
    /// error-condition countries, for the given monitor.
    pub(crate) fn create_basic_set(monitor_name: &str) -> HashMap<MetricsKey, Metrics> {
        [
            // These are countries with high traffic, so we initialize them beforehand.
            celes::Country::germany(),
            celes::Country::the_united_states_of_america(),
            celes::Country::the_netherlands(),
        ]
        .into_iter()
        .map(|c| Geolocation::Alpha2(c.alpha2.to_string()))
        // Add the Unknown and Error geolocations.
        .chain([Geolocation::Unknown, Geolocation::Error].into_iter())
        // Map to MetricsKeys for both overlay origin types.
        .map(|g| {
            [
                MetricsKey {
                    geo_origin: g.clone(),
                    overlay_origin: PublicGatewayStatus::NonGateway,
                },
                MetricsKey {
                    geo_origin: g,
                    overlay_origin: PublicGatewayStatus::Gateway,
                },
            ]
        })
        .flatten()
        // Create metrics.
        .map(|k| {
            let metrics =
                Self::new_for_key(monitor_name, &k).expect("invalid metric initialization");
            (k, metrics)
        })
        .collect()
    }

    /// Creates a new set of metrics for the given metrics key, encoding geolocation and gateway
    /// status.
    /// If the geolocation is [`Geolocation::Alpha2`] and not a valid 2-letter ISO3166-1 code, an
    /// error is returned.
    pub(crate) fn new_for_key(monitor_name: &str, key: &MetricsKey) -> Result<Metrics> {
        match &key.geo_origin {
            Geolocation::Alpha2(country_code) => {
                let country = celes::Country::from_alpha2(country_code)
                    .map_err(|e| err_msg(format!("{}", e)))
                    .context("invalid country code")?;
                Ok(Self::new_for_country_name_and_gateway_status(
                    monitor_name,
                    country.long_name,
                    key.overlay_origin,
                ))
            }
            Geolocation::Unknown => Ok(Self::new_for_country_name_and_gateway_status(
                monitor_name,
                COUNTRY_NAME_UNKNOWN,
                key.overlay_origin,
            )),
            Geolocation::Error => Ok(Self::new_for_country_name_and_gateway_status(
                monitor_name,
                COUNTRY_NAME_ERROR,
                key.overlay_origin,
            )),
        }
    }

    /// Creates a new set of metrics for the country name and gateway status.
    fn new_for_country_name_and_gateway_status(
        monitor_name: &str,
        country_name: &str,
        gateway_status: PublicGatewayStatus,
    ) -> Metrics {
        Metrics {
            num_messages: BITSWAP_MESSAGES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),

            num_entries_cancel: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "cancel",
                    "false",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_entries_want_block: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "want_block",
                    "false",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_entries_want_block_send_dont_have: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "want_block",
                    "true",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_entries_want_have: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "want_have",
                    "false",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_entries_want_have_send_dont_have: WANTLIST_ENTRIES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "want_have",
                    "true",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),

            num_connected: CONNECTION_EVENTS_CONNECTED
                .get_metric_with_label_values(&[
                    monitor_name,
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_disconnected: CONNECTION_EVENTS_DISCONNECTED
                .get_metric_with_label_values(&[
                    monitor_name,
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_wantlists_incremental: WANTLISTS_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "false",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_wantlists_full: WANTLISTS_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "true",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_blocks: BITSWAP_BLOCKS_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_block_presence_have: BITSWAP_BLOCK_PRESENCES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "HAVE",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
            num_block_presence_dont_have: BITSWAP_BLOCK_PRESENCES_RECEIVED
                .get_metric_with_label_values(&[
                    monitor_name,
                    "DONT_HAVE",
                    country_name,
                    gateway_status.is_gateway_str(),
                ])
                .unwrap(),
        }
    }
}

/// Starts a thread to serve prometheus metrics.
pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    prometheus_exporter::start(addr).context("can not start exporter")?;

    Ok(())
}
