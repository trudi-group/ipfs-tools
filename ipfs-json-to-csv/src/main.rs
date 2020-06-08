#[macro_use]
extern crate log;

mod conntrack;

use clap::{App, Arg, SubCommand};
use failure::{err_msg, ResultExt};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use ipfs_resolver_common::{logging, wantlist, Result};
use std::io;
use std::path::PathBuf;
use crate::conntrack::ConnectionDurationTracker;


fn main() -> Result<()> {
    let matches = App::new("IPFS wantlist JSON to CSV converter")
        .about("converts JSON files to CSV files and some other stuff")
        .subcommand(SubCommand::with_name("print-lookup")
            .help("prints current versions of lookup tables as CSV to Stdout"))
        .subcommand(SubCommand::with_name("transform")
            .about("transforms a JSON file to a CSV file")
            .help("This will transform a JSON file into a CSV file.\n\
             Additionally, one line of CSV-formatted data will be written to Stdout,\
             containing timestamps of the first and last entry of the file.")
            .arg(Arg::with_name("synth")
                .long("insert-cancels")
                .help("whether to insert synthetic CANCEL entries whenever a peer disconnected"))
            .arg(Arg::with_name("allowempty")
                .long("allow-empty-full-wantlist")
                .help("whether to allow messages with full_want_list"))
            .arg(Arg::with_name("inglob")
                .required(true)
                .takes_value(true)
                .index(1)
                .value_name("input file glob")
                .help("the glob for all input files, i.e. (gzipped) JSON files to read"))
            .arg(Arg::with_name("outdir")
                .required(true)
                .takes_value(true)
                .index(2)
                .value_name("output directory")
                .help("the directory for output files for wantlist messages, which will be gzipped CSV files"))
            .arg(Arg::with_name("conndir")
                .required(true)
                .takes_value(true)
                .index(3)
                .value_name("connection events directory")
                .help("the directory for output files for connection events, which will be a gzipped CSV files")))
        .get_matches();

    logging::set_up_logging(false)?;

    match matches.subcommand_name() {
        Some(name) => match name {
            "transform" => do_transform(
                matches
                    .subcommand_matches("transform")
                    .unwrap()
                    .value_of("inglob")
                    .unwrap(),
                matches
                    .subcommand_matches("transform")
                    .unwrap()
                    .value_of("outdir")
                    .unwrap(),
                matches
                    .subcommand_matches("transform")
                    .unwrap()
                    .value_of("conndir")
                    .unwrap(),
                matches
                    .subcommand_matches("transform")
                    .unwrap()
                    .is_present("synth"),
                matches
                    .subcommand_matches("transform")
                    .unwrap()
                    .is_present("allowempty"),
            ),
            "print-lookup" => {
                print_lookup_tables();
                Ok(())
            }
            _ => {
                println!("{}", matches.usage());
                Ok(())
            }
        },
        None => {
            println!("{}", matches.usage());
            Ok(())
        }
    }
}

fn do_transform_single_file(
    mut infile: Box<dyn io::BufRead>,
    outfile: Box<dyn io::Write>,
    conn_outfile: Box<dyn io::Write>,
    insert_cancels: bool,
    allow_empty_full_want_list: bool,
    wl: &mut wantlist::Wantlist,
    current_message_id: &mut i64,
    conn_tracker: &mut ConnectionDurationTracker,
) -> Result<(
    Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    i32,
)> {
    let mut wl_writer = csv::Writer::from_writer(outfile);
    let mut conn_writer = csv::Writer::from_writer(conn_outfile);

    let mut buf = String::new();
    let mut first_message_ts = None;
    let mut last_message_ts = None;
    let mut missing_leders = 0;
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
        let ingest_result = wl.ingest(&message, allow_empty_full_want_list, false)?;
        if ingest_result.missing_ledger {
            missing_leders += 1;
        }
        let produced_synthetic_cancels = {
            let connection_synthetic_cancels = match ingest_result
                .entries_canceled_because_new_connection
            {
                Some(cancels) => {
                    if insert_cancels {
                        debug!("got synthetic cancels because of a reconnect {:?} and will output them!", cancels);
                        let synthetic_entries = wantlist::CSVWantlistEntry::from_synthetic_cancels(
                            cancels,
                            &message,
                            *current_message_id,
                        )?;
                        synthetic_entries
                            .into_iter()
                            .try_for_each(|e| wl_writer.serialize(e))?;
                    } else {
                        debug!(
                            "got synthetic cancels from a reconnect {:?} but will not output them.",
                            cancels
                        );
                    }
                    true
                }
                None => false,
            };
            let full_want_list_synthetic_cancels = match ingest_result
                .entries_canceled_because_full_want_list
            {
                Some(cancels) => {
                    if insert_cancels {
                        debug!("got synthetic cancels because of a full wantlist {:?} and will output them!", cancels);
                        let synthetic_entries = wantlist::CSVWantlistEntry::from_synthetic_cancels(
                            cancels,
                            &message,
                            *current_message_id,
                        )?;
                        synthetic_entries
                            .into_iter()
                            .try_for_each(|e| wl_writer.serialize(e))?;
                    } else {
                        debug!(
                            "got synthetic cancels from a full wantlist {:?} but will not output them.",
                            cancels
                        );
                    }
                    true
                }
                None => false,
            };

            full_want_list_synthetic_cancels | connection_synthetic_cancels
        };

        // Serialize to CSV.
        match message.received_entries {
            Some(_) => {
                debug!("processing as wantlist entries");

                let csv_entries =
                    wantlist::CSVWantlistEntry::from_json_message(message, *current_message_id)?;
                csv_entries
                    .into_iter()
                    .try_for_each(|e| wl_writer.serialize(e))?;
            }
            None => {
                debug!("processing as connection event");
                let conn_entry =
                    wantlist::CSVConnectionEvent::from_json_message(message, *current_message_id)?;
                conn_writer.serialize(conn_entry)?;
            }
        }

        buf.clear();
    }

    match first_message_ts {
        Some(first_ts) => match last_message_ts {
            Some(last_ts) => Ok((Some((first_ts, last_ts)), missing_leders)),
            None => Ok((Some((first_ts, first_ts)), missing_leders)),
        },
        None => Ok((None, missing_leders)),
    }
}

fn do_transform(
    input_glob: &str,
    output_dir: &str,
    conn_out_dir: &str,
    insert_cancels: bool,
    allow_empty_full_want_list: bool,
) -> Result<()> {
    let mut wl = wantlist::Wantlist::new();
    let mut current_message_id: i64 = 0;
    let mut conn_tracker = ConnectionDurationTracker::new();
    let mut final_ts = None;

    let outdir = PathBuf::from(output_dir);
    let conn_outdir = PathBuf::from(conn_out_dir);

    std::fs::create_dir_all(&outdir).context("unable to create output directory")?;
    std::fs::create_dir_all(&conn_outdir)
        .context("unable to create connection event output direcotry")?;

    for entry in glob::glob(input_glob).context("invalid glob")? {
        match entry {
            Ok(path) => {
                info!("now working on {}", path.display());
                let filename = path
                    .file_stem()
                    .ok_or_else(|| err_msg(format!("file {} has no basename?", path.display())))?;

                let output_file_path = outdir
                    .join(PathBuf::from(filename))
                    .with_file_name(format!("{}-wl.csv.gz", filename.to_str().unwrap()));
                debug!("output file path is {}", output_file_path.display());
                let conn_output_file_path = conn_outdir
                    .join(PathBuf::from(filename))
                    .with_file_name(format!("{}-conn-events.csv.gz", filename.to_str().unwrap()));
                debug!(
                    "conn output file path is {}",
                    conn_output_file_path.display()
                );

                let input_file = io::BufReader::new(GzDecoder::new(
                    std::fs::File::open(&path).context("unable to open input file for reading")?,
                ));
                let output_file = GzEncoder::new(
                    io::BufWriter::new(
                        std::fs::File::create(output_file_path)
                            .context("unable to open wantlist output file for writing")?,
                    ),
                    Compression::default(),
                );
                let events_file = GzEncoder::new(
                    io::BufWriter::new(
                        std::fs::File::create(conn_output_file_path)
                            .context("unable to open connection output file for writing")?,
                    ),
                    Compression::default(),
                );

                let (timestamps, missing_ledgers) = do_transform_single_file(
                    Box::new(input_file),
                    Box::new(output_file),
                    Box::new(events_file),
                    insert_cancels,
                    allow_empty_full_want_list,
                    &mut wl,
                    &mut current_message_id,
                    &mut conn_tracker,
                )
                .context(format!("unable to process file {}", path.display()))?;

                match timestamps {
                    Some((first, last)) => {
                        info!("first ts: {}, last ts: {}", first, last);
                        final_ts.replace(last);
                    }
                    None => info!("empty file?"),
                }
                info!("{} missing ledgers", missing_ledgers);
            }

            // if the path matched but was unreadable,
            // thereby preventing its contents from matching
            Err(e) => return Err(err_msg(format!("unable to traverse glob: {:?}", e))),
        }
    }

    info!("finalizing connection tracker...");
    if let Some(ts) = final_ts {
        let connections = conn_tracker.finalize(ts);

        let conn_output_file_path = conn_outdir.with_file_name("connections.csv.gz");
        debug!(
            "connection output file path is {}",
            conn_output_file_path.display()
        );
        let f = GzEncoder::new(
            io::BufWriter::new(
                std::fs::File::create(conn_output_file_path)
                    .context("unable to open connection output file for writing")?,
            ),
            Compression::default(),
        );

        let mut w = csv::Writer::from_writer(f);

        info!("writing connections CSV...");
        for (peer_id, conns) in connections.into_iter() {
            for c in conns.into_iter() {
                let to_encode = c.to_csv(peer_id.clone());
                w.serialize(to_encode)
                    .context("unable to serialize connection metadata")?;
            }
        }
    } else {
        warn!("missing final timestamp, unable to finalize")
    }

    Ok(())
}

fn print_lookup_tables() {
    println!("Connection event types:");
    println!("{}", wantlist::connection_event_types_csv());

    println!("Message types:");
    println!("{}", wantlist::message_types_csv());

    println!("Entry types:");
    println!("{}", wantlist::entry_types_csv())
}

