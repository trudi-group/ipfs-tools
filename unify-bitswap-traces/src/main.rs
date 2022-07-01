#[macro_use]
extern crate log;

mod config;
mod matcher;
mod source;

use crate::config::Config;
use crate::matcher::InterMonitorMatcher;
use crate::source::{MultiSourceIngestResult, MultiSourceIngester};
use clap::{App, Arg};
use failure::{err_msg, ResultExt};
use flate2::write::GzEncoder;
use ipfs_resolver_common::{logging, Result};
use std::io;

fn main() -> Result<()> {
    logging::set_up_logging()?;

    let matches = App::new("IPFS bitswap trace unification tool")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("unifies two JSON bitswap traces")
        .arg(
            Arg::with_name("cfg")
                .long("config")
                .value_name("PATH")
                .default_value("config.yaml")
                .help("the config file to load")
                .required(true),
        )
        .get_matches();

    if !matches.is_present("cfg") {
        println!("{}", matches.usage());
        return Err(err_msg("missing config"));
    }
    let cfg = matches.value_of("cfg").unwrap();

    // Read config
    info!("attempting to load config file '{}'", cfg);
    let cfg = Config::open(cfg).context("unable to load config")?;
    debug!("read config {:?}", cfg);

    // Construct merged source
    let mut multi_source =
        MultiSourceIngester::from_config(&cfg).context("unable to set up sources")?;
    let source_names = multi_source.source_names();
    info!("unifying sources {:?}", source_names);

    // Construct the thing that keeps track of inter-monitor duplicates and matches entries
    let mut dup_marker = InterMonitorMatcher::new_from_config(&cfg.matching_config)
        .context("unable to construct inter-monitor duplicate marker and matcher")?;

    let mut num_messages_in_current_output_file = 0;
    let messages_per_file = 100_000;
    let mut current_output_file =
        create_output_writer_from_pattern(cfg.wantlist_output_file_pattern.clone(), 0)
            .context("unable to create output file")?;

    // Iterate through entries produced by the merged source iterator
    let before = std::time::Instant::now();
    while let Some(res) = multi_source.next() {
        match res {
            Err(e) => {
                return Err(e.context("unable to advance sources").into());
            }
            Ok(MultiSourceIngestResult {
                monitor_id,
                timestamp,
                peer_id,
                simulation_result,
            }) => {
                debug!(
                    "got entry {:?} from monitor {}",
                    simulation_result, monitor_id
                );

                // Feed that into the matching engine
                let output_entries = dup_marker
                    .handle_ingest_result(monitor_id, timestamp, peer_id, simulation_result)
                    .context("unable to handle ingest result")?;

                // Rotate output file if necessary
                if num_messages_in_current_output_file > messages_per_file {
                    current_output_file = create_output_writer_from_pattern(
                        cfg.wantlist_output_file_pattern.clone(),
                        multi_source.last_message_id() + 1,
                    )
                    .context("unable to create output file")?;
                    num_messages_in_current_output_file = 0;
                }

                // Write entries to output file
                output_entries
                    .into_iter()
                    .try_for_each(|e| current_output_file.serialize(e))
                    .context("unable to write output")?;
                num_messages_in_current_output_file += 1;
            }
        }
    }

    // TODO emit end-of-simulation synthetic cancels?
    // TODO keep track of missing ledgers, maybe per-source?
    // TODO also keep track of ledger count per-source and in sum?

    let time_diff = before.elapsed();

    let msg_id = multi_source.last_message_id();
    let _engine_states = multi_source.into_engine_states();
    let matching_stats = dup_marker.stats();

    info!(
        "processed {} messages in {:.1}s => {:.1}msg/s",
        msg_id,
        time_diff.as_secs_f32(),
        (msg_id as f64) / time_diff.as_secs_f64()
    );
    info!(
        "{} entries in total, of which {} were matched between monitors",
        matching_stats.total_entries, matching_stats.matched_entries
    );
    info!(
        "min/mean/max match diff (ms): {}/{}/{}",
        matching_stats.min_match_diff,
        (matching_stats.match_diff_sum / matching_stats.matched_entries as f64) * 1000.0,
        matching_stats.max_match_diff
    );

    Ok(())
}

fn create_output_writer_from_pattern(
    pattern: String,
    current_message_id: i64,
) -> Result<csv::Writer<GzEncoder<io::BufWriter<std::fs::File>>>> {
    Ok(csv::Writer::from_writer(GzEncoder::new(
        io::BufWriter::new(
            std::fs::File::create(pattern.replace("$id$", &format!("{:09}", current_message_id)))
                .context("unable to open output file for writing")?,
        ),
        flate2::Compression::default(),
    )))
}
