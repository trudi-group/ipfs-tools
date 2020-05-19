#[macro_use]
extern crate log;

use clap::{App, Arg, SubCommand};
use failure::{err_msg, ResultExt};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use ipfs_resolver_common::{logging, wantlist, Result};
use std::io;
use std::path::PathBuf;

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
    wl: &mut wantlist::Wantlist,
    current_message_id: &mut i64,
) -> Result<Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>> {
    let mut wl_writer = csv::Writer::from_writer(outfile);
    let mut conn_writer = csv::Writer::from_writer(conn_outfile);

    let mut buf = String::new();
    let mut first_message_ts = None;
    let mut last_message_ts = None;
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

        let synthetic_cancels = wl.ingest(&message)?;
        let produced_synthetic_cancels = match synthetic_cancels {
            Some(cancels) => {
                if insert_cancels {
                    debug!("got synthetic cancels {:?} and will output them!", cancels);
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
                        "got synthetic cancels {:?} but will not output them.",
                        cancels
                    );
                }
                true
            }
            None => false,
        };

        match message.received_entries {
            Some(_) => {
                debug!("processing as wantlist entries");
                assert!(
                    !produced_synthetic_cancels,
                    "processing message as wantlist entries and generating synthetic cancels"
                );

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
            Some(last_ts) => Ok(Some((first_ts, last_ts))),
            None => Ok(Some((first_ts, first_ts))),
        },
        None => Ok(None),
    }
}

fn do_transform(
    input_glob: &str,
    output_dir: &str,
    conn_out_dir: &str,
    insert_cancels: bool,
) -> Result<()> {
    let mut wl = wantlist::Wantlist::new();
    let mut current_message_id: i64 = 0;

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

                let timestamps = do_transform_single_file(
                    Box::new(input_file),
                    Box::new(output_file),
                    Box::new(events_file),
                    insert_cancels,
                    &mut wl,
                    &mut current_message_id,
                )
                .context(format!("unable to process file {}", path.display()))?;

                match timestamps {
                    Some((first, last)) => info!("first ts: {}, last ts: {}", first, last),
                    None => info!("empty file?"),
                }
            }

            // if the path matched but was unreadable,
            // thereby preventing its contents from matching
            Err(e) => return Err(err_msg(format!("unable to traverse glob: {:?}", e))),
        }
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
