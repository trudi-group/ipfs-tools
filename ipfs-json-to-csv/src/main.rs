#[macro_use]
extern crate log;

mod config;
mod conntrack;

use crate::conntrack::ConnectionDurationTracker;
use clap::{App, Arg};
use failure::{err_msg, ResultExt};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use ipfs_resolver_common::{logging, wantlist, Result};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter};

fn main() -> Result<()> {
    let matches = App::new("IPFS wantlist JSON to CSV converter")
        .about("converts JSON files to CSV files and some other stuff")
        .help(
            "This will transform a JSON file into a CSV file.\n\
             Additionally, one line of CSV-formatted data will be written to Stdout,\
             containing timestamps of the first and last entry of the file.",
        )
        .arg(
            Arg::with_name("config")
                .default_value("config.yaml")
                .help("the config file to load"),
        )
        .get_matches();

    logging::set_up_logging(false)?;

    if !matches.is_present("config") {
        println!("{}", matches.usage());
        return Err(err_msg("missing config"));
    }

    info!(
        "attempting to load config from file '{}'",
        matches.value_of("config").unwrap()
    );
    let conf_file = matches.value_of("config").unwrap();
    let conf = config::Config::open(conf_file).context("unable to load config")?;

    info!(
        "output file for wantlist messages is {}",
        conf.wantlist_output_file_pattern
    );
    info!(
        "output file for connection events is {}",
        conf.connection_events_output_file
    );
    info!(
        "output file for connection durations is {}",
        conf.connection_duration_output_file
    );
    info!("simulation config is {:?}", conf.simulation_config);

    do_transform(conf).context("unable to do transformation")?;

    Ok(())
}

struct SingleFileTransformResult {
    timestamps: Option<(chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>,
    num_missing_ledgers: i32,
}

fn do_transform_single_file(
    mut infile: BufReader<GzDecoder<File>>,
    wl_writer: &mut csv::Writer<GzEncoder<BufWriter<File>>>,
    conn_writer: &mut csv::Writer<GzEncoder<BufWriter<File>>>,
    engine: &mut wantlist::EngineSimulation,
    current_message_id: &mut i64,
    conn_tracker: &mut ConnectionDurationTracker,
) -> Result<SingleFileTransformResult> {
    let mut buf = String::new();
    let mut first_message_ts = None;
    let mut last_message_ts = None;
    let mut missing_ledgers = 0;
    while let Ok(n) = infile.read_line(&mut buf) {
        if n == 0 {
            break;
        }
        *current_message_id += 1;

        let message: wantlist::JSONMessage = serde_json::from_str(&buf)?;
        debug!("decoded message {:?}", message);
        if first_message_ts.is_none() {
            first_message_ts = Some(message.timestamp);
        }
        last_message_ts.replace(message.timestamp);

        // Add to connection tracker.
        conn_tracker
            .push(&message)
            .context("unable to track connection duration")?;

        // Update simulated wantlists.
        let ingest_result = engine.ingest(&message, *current_message_id)?;
        debug!("ingest result: {:?}", ingest_result);

        if ingest_result.missing_ledger {
            missing_ledgers += 1;
        }
        if let Some(entries) = ingest_result.wantlist_entries.as_ref() {
            entries
                .iter()
                .try_for_each(|e| wl_writer.serialize(e))
                .context("unable to serialize wantlist entries")?;
        }
        if let Some(conn_event) = ingest_result.connection_event.as_ref() {
            conn_writer
                .serialize(conn_event)
                .context("unable to serialize connection event")?;
        }

        buf.clear();
    }

    Ok(SingleFileTransformResult {
        timestamps: if let Some(first) = first_message_ts {
            Some((first,last_message_ts.unwrap()))
        } else {None},
        num_missing_ledgers: missing_ledgers,
    })
}

fn do_transform(cfg: config::Config) -> Result<()> {
    let mut wl = wantlist::EngineSimulation::new(cfg.simulation_config.clone())
        .context("unable to set up engine simulation")?;
    let mut current_message_id: i64 = 0;
    let mut conn_tracker = ConnectionDurationTracker::new();
    let mut final_ts = None;

    let mut conn_events_output_writer = csv::Writer::from_writer(GzEncoder::new(
        io::BufWriter::new(
            std::fs::File::create(cfg.connection_events_output_file.clone())
                .context("unable to open connection events output file for writing")?,
        ),
        Compression::default(),
    ));
    let mut connection_durations_output_writer = csv::Writer::from_writer(GzEncoder::new(
        io::BufWriter::new(
            std::fs::File::create(cfg.connection_duration_output_file.clone())
                .context("unable to open connection duration output file for writing")?,
        ),
        Compression::default(),
    ));

    let input_files = cfg.glob_results().context("unable to glob")?;
    debug!("paths: {:?}", input_files);

    for path in input_files {
        info!("now working on {}", path.display());
        let input_file = BufReader::new(GzDecoder::new(
            std::fs::File::open(&path).context("unable to open input file for reading")?,
        ));

        let mut wl_output_writer = csv::Writer::from_writer(GzEncoder::new(
            io::BufWriter::new(
                std::fs::File::create(
                    cfg.wantlist_output_file_pattern
                        .clone()
                        .replace("$id$", &format!("{:09}", current_message_id)),
                )
                .context("unable to open wantlist output file for writing")?,
            ),
            Compression::default(),
        ));

        let id_before = current_message_id;
        let before = std::time::Instant::now();
        let transform_result = do_transform_single_file(
            input_file,
            &mut wl_output_writer,
            &mut conn_events_output_writer,
            &mut wl,
            &mut current_message_id,
            &mut conn_tracker,
        )
        .context(format!("unable to process file {}", path.display()))?;
        let num_messages = current_message_id - id_before;
        let time_diff = before.elapsed();

        info!("processed {} messages in {} s => {:.2} msg/s",num_messages,time_diff.as_secs(),(num_messages as f64) / time_diff.as_secs_f64());

        match transform_result.timestamps {
            Some((first,last)) => {
                info!("first ts: {}, last ts: {}", first, last);
                final_ts.replace(last);
            }
            None => info!("empty file?"),
        }
        info!("{} missing ledgers", transform_result.num_missing_ledgers);
    }

    info!("finalizing connection tracker...");
    if let Some(ts) = final_ts {
        let connections = conn_tracker.finalize(ts);

        info!("writing connections CSV...");
        for (peer_id, conns) in connections.into_iter() {
            for c in conns.into_iter() {
                let to_encode = c.to_csv(peer_id.clone());
                connection_durations_output_writer
                    .serialize(to_encode)
                    .context("unable to serialize connection metadata")?;
            }
        }
    } else {
        warn!("missing final timestamp, unable to finalize")
    }

    Ok(())
}
