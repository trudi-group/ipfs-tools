#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

use crate::config::Config;
use clap::{App, Arg};
use failure::{bail, err_msg, format_err, ResultExt};
use futures_util::future::join_all;
use ipfs_monitoring_plugin_client::http::{
    APIClient, PeerMetadataConnectedness, PeerMetadataEntry,
};
use ipfs_resolver_common::logging;
use ipfs_resolver_common::Result;
use roots::SimpleConvergency;
use std::collections::HashSet;
use std::time;

mod config;
mod prom;

#[tokio::main]
async fn main() -> Result<()> {
    logging::set_up_logging()?;

    // Set up CLI
    let matches = App::new("IPFS Monitoring Size Estimator")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about(
            "estimates the size of the IPFS network based on connections of large monitoring nodes",
        )
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
    // Set up prometheus
    let prometheus_address = cfg
        .prometheus_address
        .parse()
        .expect("invalid prometheus_address");

    debug!("starting prometheus server");
    prom::run_prometheus(prometheus_address)?;
    info!("started prometheus server");

    // Connect to monitors
    let mut monitors = Vec::new();
    for monitor_cfg in cfg.monitors {
        monitors.push(
            Monitor::new(&monitor_cfg.name, &monitor_cfg.plugin_api_address)
                .await
                .context(format_err!(
                    "unable to connect to monitor {}",
                    monitor_cfg.name
                ))?,
        );
    }

    // We need at least two monitors
    if monitors.len() < 2 {
        bail!("not enough monitors to estimate network size")
    }

    // Estimate then sleep, forever.
    info!("starting estimation loop. Ctlr-C to quit");
    loop {
        match compute_estimates(&monitors).await {
            Ok(_) => {}
            Err(e) => {
                error!("unable to compute estimates: {:?}", e);
            }
        }

        tokio::time::sleep(time::Duration::from_secs(cfg.sample_interval_seconds)).await;
    }
}

async fn compute_estimates(monitors: &[Monitor]) -> Result<()> {
    let mut connected_peers_per_monitor = Vec::new();

    // Sample peer metadata on all monitors.
    let requests = monitors
        .iter()
        .map(|m| m.client.sample_peer_metadata(true))
        .collect::<Vec<_>>();
    join_all(requests)
        .await
        .into_iter()
        .zip(monitors.iter().map(|m| m.name.clone()))
        .for_each(|(res, name)| match res {
            Ok(res) => {
                // Filter for connected peers
                connected_peers_per_monitor.push((
                    name,
                    res.peer_metadata
                        .into_iter()
                        .filter(|p| match p.connectedness {
                            PeerMetadataConnectedness::Connected => true,
                            _ => false,
                        })
                        .collect::<Vec<_>>(),
                ))
            }
            Err(err) => {
                error!(
                    "unable to sample peer metadata for monitor {}: {:?}",
                    name, err
                )
            }
        });

    if connected_peers_per_monitor.len() < 2 {
        bail!("not enough monitors to estimate network size")
    }

    // Extract protocols offered by at least one peer.
    let protocols = connected_peers_per_monitor
        .iter()
        .map(|(_, metadata)| metadata)
        .map(|metadata| metadata.iter().map(|m| m.protocols.clone()))
        .flatten()
        .map(|protocols| {
            if let Some(protocols) = protocols {
                protocols
            } else {
                Vec::new()
            }
        })
        .fold(HashSet::new(), |mut protocols, new_protocols| {
            for protocol in new_protocols {
                protocols.insert(protocol);
            }
            protocols
        });
    debug!("derived protocols: {:?}", protocols);

    // Calculate global estimate
    estimate_for_protocol(connected_peers_per_monitor.clone(), None);

    // Calculate estimates for protocols
    for protocol in protocols {
        estimate_for_protocol(connected_peers_per_monitor.clone(), Some(protocol))
    }

    Ok(())
}

fn estimate_for_protocol(
    connected_peers_per_monitor: Vec<(String, Vec<PeerMetadataEntry>)>,
    protocol: Option<String>,
) {
    connected_peers_per_monitor
        .windows(2)
        .map(|pair| {
            let names = format!("{} with {}", pair[0].0, pair[1].0);
            let estimate = hypergeom_estimate(&pair[0].1, &pair[1].1, protocol.clone());
            (names, estimate)
        })
        .for_each(|(names, estimate)| {
            debug!(
                "hypergeom estimate for {}, protocol {:?}: {:?}",
                names, protocol, estimate
            );
            if let Ok(estimate) = estimate {
                prom::HYPERGEOM_SIZE_ESTIMATE
                    .get_metric_with_label_values(&[
                        protocol.as_ref().map(|s| s.as_str()).unwrap_or(""),
                        &names,
                    ])
                    .unwrap()
                    .set(estimate as i64)
            }
        });

    match coupon_estimate(
        connected_peers_per_monitor
            .iter()
            .map(|(_, metadata)| metadata)
            .collect(),
        protocol.clone(),
    ) {
        Ok(estimate) => {
            debug!("coupon estimate for protocol {:?}: {}", protocol, estimate);
            prom::COUPON_SIZE_ESTIMATE
                .get_metric_with_label_values(&[protocol
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or("")])
                .unwrap()
                .set(estimate as i64)
        }
        Err(e) => {
            error!(
                "unable to estimate with coupon collector estimator: {:?}",
                e
            )
        }
    }
}

fn extract_connected_ids_supporting_protocol(
    peers: &[PeerMetadataEntry],
    protocol: Option<String>,
) -> HashSet<String> {
    peers
        .iter()
        .filter(|p| match protocol.as_ref() {
            None => true,
            Some(protocol) => match p.protocols.as_ref() {
                None => false,
                Some(protocols) => protocols.contains(&protocol),
            },
        })
        .map(|p| p.peer_id.clone())
        .collect()
}

fn hypergeom_estimate(
    monitor_1_peers: &[PeerMetadataEntry],
    monitor_2_peers: &[PeerMetadataEntry],
    protocol: Option<String>,
) -> Result<u64> {
    let mon_1_peer_ids: HashSet<_> =
        extract_connected_ids_supporting_protocol(monitor_1_peers, protocol.clone());
    let mon_2_peer_ids: HashSet<_> =
        extract_connected_ids_supporting_protocol(monitor_2_peers, protocol.clone());

    Ok(hypergeom_estimate_inner(&mon_1_peer_ids, &mon_2_peer_ids))
}

fn hypergeom_estimate_inner(
    monitor_1_peers: &HashSet<String>,
    monitor_2_peers: &HashSet<String>,
) -> u64 {
    let n_monitor_1 = monitor_1_peers.len() as u64;
    let n_monitor_2 = monitor_2_peers.len() as u64;
    let union_size = monitor_1_peers.union(&monitor_2_peers).count() as u64;
    let intersection_size = monitor_1_peers.intersection(&monitor_2_peers).count() as u64;
    debug!(
        "monitor 1 peers: {}, monitor 2 peers: {}, union: {}, intersection: {}",
        n_monitor_1, n_monitor_2, union_size, intersection_size
    );

    if intersection_size == 0 {
        debug!("intersection is empty, returning union");
        return union_size;
    }

    let estimate = (n_monitor_1 * n_monitor_2) / intersection_size;
    debug!("hypergeom estimate: {}", estimate);

    if union_size > estimate {
        debug!("union is larger than estimate, using that");
        return union_size;
    }

    estimate
}

fn coupon_estimate(
    peer_metadata: Vec<&Vec<PeerMetadataEntry>>,
    protocol: Option<String>,
) -> Result<u64> {
    let peer_ids = peer_metadata
        .iter()
        .map(|m| extract_connected_ids_supporting_protocol(m, protocol.clone()))
        .filter(|h| !h.is_empty())
        .collect::<Vec<_>>();

    if peer_ids.len() == 0 {
        bail!("no peers supporting the protocol");
    } else if peer_ids.len() == 1 {
        Ok(peer_ids[0].len() as u64)
    } else {
        coupon_estimate_inner(peer_ids)
    }
}

fn coupon_estimate_inner(peers: Vec<HashSet<String>>) -> Result<u64> {
    /*
    ## Estimator based on the coupon collectors problem with group drawings. Intuitively, each monitor is
    ## a drawing from the population of cards.
    # N: Total number of nodes, our parameter to be estimated
    # r: Number of monitoring nodes ("drawings" in coupon collector problem)
    # w: size of each "drawing" = number of peers of each monitor.
    # We assume that each drawing has the same size (there are generalizations on this, though)
    # m: Union of all observed peers (= #distinct "cards" we got after drawing r times)

    # This formula is essentially a MLE estimate for N, based on the prob. density
    # derived by Mantel & Pasternack. Since it's intractable symbolically, we have to find its roots numerically

    couponCollector_func = function(N, w, m, r) {
      N - N*(1 - (m/N))^(1/r) - w
    }

    ## Numerically optimization of the above function. In our case r=2.
    couponCollector_est = function(n_peer_union, n_peers_per_monitor, n_monitors=2) {
      ## ToDo: "upper" interval is just a guess here
      peer_est = uniroot(couponCollector_func,
                         interval=c(min(n_peer_union, n_monitors*n_peers_per_monitor), 100*n_peer_union),
                         r=n_monitors, w=n_peers_per_monitor, m=n_peer_union)
      return(peer_est$root)
    }
         */

    let mean_peers_per_monitor =
        statistical::mean(&peers.iter().map(|h| h.len() as f64).collect::<Vec<_>>());
    let num_monitors = peers.len() as f64;
    let num_peers_union = peers
        .iter()
        .fold(HashSet::new(), |h1, h2| h1.union(&h2).cloned().collect())
        .len() as f64;
    debug!(
        "coupon: peer numbers: {:?}, mean peers: {}, num monitors: {}, union: {}",
        peers.iter().map(|p| p.len()).collect::<Vec<_>>(),
        mean_peers_per_monitor,
        num_monitors,
        num_peers_union
    );

    let f = |n: f64| {
        n - n * (1_f64 - (num_peers_union / n)).powf(1_f64 / num_monitors) - mean_peers_per_monitor
    };

    let root = roots::find_root_brent(
        num_peers_union,
        10_f64 * num_monitors * num_peers_union,
        f,
        &mut SimpleConvergency {
            eps: f64::EPSILON,
            max_iter: 1000,
        },
    );
    debug!("find_root returned {:?}", root);

    root.map(|r| r.round() as u64)
        .map_err(|e| format_err!("unable to find root: {:?}", e))
}

struct Monitor {
    name: String,
    client: APIClient,
}

impl Monitor {
    async fn new(name: &str, plugin_api_base: &str) -> Result<Monitor> {
        let client = APIClient::new(&plugin_api_base).context("unable to set up plugin client")?;

        client.ping().await.context("unable to ping monitor")?;

        Ok(Monitor {
            name: name.to_string(),
            client,
        })
    }
}
