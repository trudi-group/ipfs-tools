use failure::{err_msg, ResultExt};
use ipfs_resolver_common::Result;
use prometheus::core::{AtomicI64, GenericCounter};
use prometheus::IntCounterVec;
use prometheus_exporter::PrometheusExporter;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;

lazy_static! {
    pub static ref WANTLIST_ENTRIES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_entries_received",
        "number of wantlist entries received by monitor, entry type, send_dont_have, and origin country",
        &["monitor", "entry_type", "send_dont_have","origin_country"]
    )
    .unwrap();
    pub static ref WANTLIST_MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "wantlist_messages_received",
        "number of bitswap messages received for which the wantlist was not empty, by monitor, whether the message was a full wantlist, and origin country",
        &["monitor","full","origin_country"]
    )
    .unwrap();

    pub static ref CONNECTION_EVENTS_CONNECTED: IntCounterVec = register_int_counter_vec!(
        "connection_events_connected",
        "number of connect events by monitor and origin country",
        &["monitor","origin_country"]
    )
    .unwrap();

    pub static ref CONNECTION_EVENTS_DISCONNECTED: IntCounterVec = register_int_counter_vec!(
        "connection_events_disconnected",
        "number of disconnect events by monitor and origin country",
        &["monitor","origin_country"]
    )
    .unwrap();

    pub static ref BITSWAP_MESSAGES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_messages_received",
        "number of bitswap messages (both requests and responses) received by monitor and origin country",
        &["monitor","origin_country"]
    )
    .unwrap();

    pub static ref BITSWAP_BLOCKS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_blocks_received",
        "number of blocks received via bitswap, by monitor and origin country",
        &["monitor","origin_country"]
    )
    .unwrap();

    pub static ref BITSWAP_BLOCK_PRESENCES_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bitswap_block_presences_received",
        "number of block presences received via bitswap, by monitor, presence type, and origin country",
        &["monitor","presence_type","origin_country"]
    )
    .unwrap();
}

/// Country name and code constants for various error conditions.
/// These are used as both the two-letter country code and the full country name.
pub(crate) static COUNTRY_NAME_UNKNOWN: &'static str = "Unknown";
pub(crate) static COUNTRY_NAME_ERROR: &'static str = "Error";

/// A set of metrics instantiated by monitor name and country.
pub(crate) struct MetricsByMonitorAndCountry {
    /// Counter for Bitswap messages.
    pub(crate) num_messages: GenericCounter<AtomicI64>,

    /// Counter for blocks received via Bitswap.
    pub(crate) num_blocks: GenericCounter<AtomicI64>,

    /// Counters for block presences received via Bitswap, by presence type.
    pub(crate) num_block_presence_have: GenericCounter<AtomicI64>,
    pub(crate) num_block_presence_dont_have: GenericCounter<AtomicI64>,

    /// Counters for Bitswap messages containing a wantlist.
    pub(crate) num_messages_with_wl_incremental: GenericCounter<AtomicI64>,
    pub(crate) num_messages_with_wl_full: GenericCounter<AtomicI64>,

    /// Counters for wantlist entries by type.
    pub(crate) num_entries_cancel: GenericCounter<AtomicI64>,
    pub(crate) num_entries_want_block: GenericCounter<AtomicI64>,
    pub(crate) num_entries_want_block_send_dont_have: GenericCounter<AtomicI64>,
    pub(crate) num_entries_want_have: GenericCounter<AtomicI64>,
    pub(crate) num_entries_want_have_send_dont_have: GenericCounter<AtomicI64>,

    /// Counters for connection events.
    pub(crate) num_connected: GenericCounter<AtomicI64>,
    pub(crate) num_disconnected: GenericCounter<AtomicI64>,
}

impl MetricsByMonitorAndCountry {
    /// Creates a set of metrics consisting of a few popular countries and the special
    /// error-condition countries, for the given monitor.
    pub(crate) fn create_basic_set(
        monitor_name: &str,
    ) -> HashMap<String, MetricsByMonitorAndCountry> {
        let mut metrics_by_country = IntoIterator::into_iter([
            celes::Country::germany(),
            celes::Country::the_united_states_of_america(),
            celes::Country::the_netherlands(),
        ])
        .map(|c| {
            (
                c.alpha2.to_string(),
                Self::new_from_iso3166_alpha2(monitor_name, c.alpha2).unwrap(),
            )
        })
        .collect::<HashMap<_, _>>();

        // Add special error-countries.
        metrics_by_country.insert(
            COUNTRY_NAME_UNKNOWN.to_string(),
            Self::new_unknown(monitor_name),
        );
        metrics_by_country.insert(
            COUNTRY_NAME_ERROR.to_string(),
            Self::new_error(monitor_name),
        );

        metrics_by_country
    }

    /// Creates a set of metrics with their `origin_country` set to "Unknown".
    /// This is used for events for which no origin can be determined.
    fn new_unknown(monitor_name: &str) -> MetricsByMonitorAndCountry {
        Self::new_from_country_name(monitor_name, COUNTRY_NAME_UNKNOWN)
    }

    /// Creates a set of metrics with their `origin_country` set to "Error".
    /// This is used for events for which determining the origin country failed.
    fn new_error(monitor_name: &str) -> MetricsByMonitorAndCountry {
        Self::new_from_country_name(monitor_name, COUNTRY_NAME_ERROR)
    }

    fn new_from_country_name(monitor_name: &str, country_name: &str) -> MetricsByMonitorAndCountry {
        MetricsByMonitorAndCountry {
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
            num_messages_with_wl_incremental: WANTLIST_MESSAGES_RECEIVED
                .get_metric_with_label_values(&[monitor_name, "false", country_name])
                .unwrap(),
            num_messages_with_wl_full: WANTLIST_MESSAGES_RECEIVED
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

    /// Creates a new set of metrics for the country identified by `country_code`.
    /// If `country_code` is not a valid 2-letter ISO3166-1 code, an error is returned.
    pub(crate) fn new_from_iso3166_alpha2(
        monitor_name: &str,
        country_code: &str,
    ) -> Result<MetricsByMonitorAndCountry> {
        let country = celes::Country::from_alpha2(country_code)
            .map_err(|e| err_msg(format!("{}", e)))
            .context("invalid country code")?;

        Ok(Self::new_from_country_name(monitor_name, country.long_name))
    }
}

pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    thread::Builder::new()
        .name("prom-exporter".to_string())
        .spawn(move || {
            PrometheusExporter::run(&addr).expect("unable to start server for prometheus");
        })?;

    Ok(())
}
