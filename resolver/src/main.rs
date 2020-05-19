#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

use ipfs_resolver_db::db;
use ipfs_resolver_db::model;

use ipfs_resolver_common::{logging, Result};

mod unixfs;
mod heuristics;
mod ipfs;

use crate::ipfs::ResolveError;
use chrono::Utc;
use cid::{Cid, Codec};
use clap::{App, Arg, SubCommand};
use diesel::{Connection, PgConnection};
use failure::{err_msg, Error, ResultExt};
use ipfs_resolver_db::model::*;
use reqwest::Url;
use std::convert::TryFrom;
use std::io;
use std::io::{BufRead, BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use std::{env, thread};

fn main() -> Result<()> {
    let matches = App::new("IPFS Resolver")
        .about(
            "resolves CIDs via an IPFS node's API and saves metadata about them to a postgres DB",
        )
        .subcommand(
            SubCommand::with_name("single")
                .about("performs lookup on a single CID, behaves like the \"old\" resolver")
                .arg(
                    Arg::with_name("cid")
                        .required(true)
                        .takes_value(true)
                        .help("the CID to resolve"),
                ),
        )
        .subcommand(
            SubCommand::with_name("batch")
                .about(
                    "processes STDIN of lines of CIDs and outputs lines of failed CIDs to stdout",
                )
                .arg(
                    Arg::with_name("nresolvers")
                        .long("num-resolvers")
                        .takes_value(true)
                        .required(true)
                        .help("the number of concurrent resolving operations to run"),
                )
                .arg(
                    Arg::with_name("errdir")
                        .long("error-directory")
                        .takes_value(true)
                        .required(true)
                        .help("the directory to use for error files"),
                )
                .arg(
                    Arg::with_name("leftovers")
                        .long("leftovers-file")
                        .takes_value(true)
                        .required(true)
                        .help("the file in which to save leftover hashes"),
                ),
        )
        .get_matches();

    dotenv::dotenv().ok();
    logging::set_up_logging(false)?;

    match matches.subcommand() {
        ("single", Some(arg_m)) => {
            let cid = arg_m.value_of("cid").expect("cid must be set");
            process_single(cid);
            Ok(())
        }
        ("batch", Some(arg_m)) => {
            let num_resolvers = arg_m
                .value_of("nresolvers")
                .expect("num-resolvers must be set")
                .parse()?;
            let err_dir = arg_m
                .value_of("errdir")
                .expect("error-directory must be set");
            let leftovers_file = arg_m
                .value_of("leftovers")
                .expect("leftovers-file must be set");
            process_batch(num_resolvers, err_dir, leftovers_file)
        }
        _ => Err(err_msg("invalid command line")),
    }
}

fn process_batch(num_resolvers: u32, err_dir_path: &str, leftovers_file_path: &str) -> Result<()> {
    let mut input = io::BufReader::new(io::stdin());

    info!("connecting to DB..");
    let conn_pool = ipfs_resolver_db::create_pool().context("unable to connect to DB")?;

    info!("creating directories/files...");
    let err_dir = PathBuf::from(err_dir_path);
    std::fs::create_dir_all(&err_dir).context("unable to create error directory")?;
    let mut leftovers_file = BufWriter::new(
        std::fs::File::create(leftovers_file_path).context("unable to create leftovers files")?,
    );

    let (results_tx, results_rx) = mpsc::sync_channel(num_resolvers as usize);

    info!("spawning thread to monitor IPFS status...");
    let monitor_chan_handle = results_tx.clone();
    thread::spawn(move || monitor_ipfs(monitor_chan_handle));

    info!("starting processing...");
    let mut processed_lines = 0;
    print!("{}", processed_lines);
    let mut free_tasks = num_resolvers;
    let mut buf = String::new();
    let mut ipfs_dead = false;
    loop {
        buf.clear();
        match input.read_line(&mut buf) {
            Ok(n) => {
                if n == 0 {
                    debug!("got EOF, cleaning up");
                    // EOF, do cleanup.
                    while free_tasks < num_resolvers {
                        let val = results_rx.recv().context("receive on channel failed, but we should always have a leftover sender")?;
                        match val {
                            BatchChannelItem::WorkerResult(cid, res) => {
                                handle_task_result(cid, res, &err_dir, &mut leftovers_file)
                                    .context("unable to process task result")?;
                                free_tasks += 1;
                                processed_lines += 1;
                                print!("\r{}", processed_lines);
                                io::stdout().flush().expect("unable to flush stdout");
                            }
                            BatchChannelItem::IPFSDead => {
                                error!("IPFS is dead, but will still try to clean up");
                                ipfs_dead = true;
                            }
                        }
                    }

                    break;
                }

                let cid = buf.trim_end().to_string();
                info!("working on {}", cid);

                if free_tasks == 0 {
                    debug!("resolver capacity reached, waiting...");
                    // We have to wait for a task to finish before starting a new one.
                    let val = results_rx.recv().context(
                        "receive on channel failed, but we should always have a leftover sender",
                    )?;
                    match val {
                        BatchChannelItem::WorkerResult(cid, res) => {
                            handle_task_result(cid, res, &err_dir, &mut leftovers_file)
                                .context("unable to process task result")?;
                            free_tasks += 1;
                            processed_lines += 1;
                            print!("\r{}", processed_lines);
                            io::stdout().flush().expect("unable to flush stdout");
                        }
                        BatchChannelItem::IPFSDead => {
                            panic!("IPFS is dead");
                        }
                    }
                }

                let results_chan = results_tx.clone();
                let pool = conn_pool.clone();

                thread::spawn(move || process_single_of_batch(cid, pool, results_chan));
                free_tasks -= 1;
            }
            Err(e) => {
                error!(
                    "unable to read from input, will process remaining tasks and exit: {:?}",
                    e
                );
                // Do cleanup.
                while free_tasks < num_resolvers {
                    let val = results_rx.recv().context(
                        "receive on channel failed, but we should always have a leftover sender",
                    )?;
                    match val {
                        BatchChannelItem::WorkerResult(cid, res) => {
                            handle_task_result(cid, res, &err_dir, &mut leftovers_file)
                                .context("unable to process task result")?;
                            free_tasks += 1;
                            processed_lines += 1;
                            print!("\r{}", processed_lines);
                            io::stdout().flush().expect("unable to flush stdout");
                        }
                        BatchChannelItem::IPFSDead => {
                            error!("IPFS is dead, but will still try to clean up");
                        }
                    }
                }

                // Exit.
                return Err(e.into());
            }
        }
    }

    if !ipfs_dead {
        Ok(())
    } else {
        Err(err_msg("IPFS is dead"))
    }
}

fn handle_task_result(
    cid: String,
    res: Result<Res>,
    err_dir: &PathBuf,
    leftovers: &mut BufWriter<std::fs::File>,
) -> Result<()> {
    match res {
        Ok(r) => match r {
            Res::Ok | Res::SkippedNotFSRelated | Res::SkippedExists => info!("{}: {:?}", cid, r),
            Res::SkippedCBOR
            | Res::SkippedHAMTShard
            | Res::SkippedMetadata
            | Res::SkippedSymlink => {
                info!("{}: {:?} (recording)", cid, r);
                writeln!(leftovers, "{}", cid).context("unable to write to leftovers file")?;
            }
        },
        Err(e) => {
            warn!("{}: {:?} (recording to file)", cid, e);
            writeln!(leftovers, "{}", cid).context("unable to write to leftovers file")?;

            // Write error to error file
            let output_file_path = err_dir.join(PathBuf::from(format!("{}.txt", cid)));
            debug!("writing error to {}", output_file_path.display());

            let mut out_file =
                std::fs::File::create(output_file_path).context("unable to create error file")?;
            writeln!(out_file, "{}: {:?}", cid, e).context("unable to write to error file")?;
        }
    }

    Ok(())
}

fn monitor_ipfs(done: mpsc::SyncSender<BatchChannelItem>) {
    let ipfs_resolver_api_url = env::var("IPFS_RESOLVER_API_URL")
        .or_else(|e| {
            done.send(BatchChannelItem::IPFSDead)
                .expect("unable to send on channel");
            Err(e)
        })
        .expect("IPFS_RESOLVER_API_URL must be set");
    let ipfs_api_base = Url::parse(&ipfs_resolver_api_url)
        .or_else(|e| {
            done.send(BatchChannelItem::IPFSDead)
                .expect("unable to send on channel");
            Err(e)
        })
        .expect("invalid IPFS API URL");
    debug!("using IPFS API base {}", ipfs_api_base);

    loop {
        let id = ipfs::query_ipfs_id(&ipfs_api_base);
        match id {
            Ok(id) => {
                debug!("IPFS monitor: got ID {}", id);
            }
            Err(e) => {
                error!("IPFS monitor: unable to get ID: {:?}", e);
                done.send(BatchChannelItem::IPFSDead)
                    .expect("unable to send on channel");
                return;
            }
        }
        thread::sleep(Duration::from_secs(10));
    }
}

enum BatchChannelItem {
    WorkerResult(String, Result<Res>),
    IPFSDead,
}

fn process_single_of_batch(
    cid: String,
    conn_pool: ipfs_resolver_db::PgPool,
    done: mpsc::SyncSender<BatchChannelItem>,
) {
    let res = process_single_of_batch_inner(&cid, conn_pool);
    debug!("{}: got {:?}", cid, res);
    done.send(BatchChannelItem::WorkerResult(cid, res))
        .expect("unable to send on task completion channel")
}

fn process_single_of_batch_inner(
    cid_str: &str,
    conn_pool: ipfs_resolver_db::PgPool,
) -> Result<Res> {
    let ipfs_resolver_api_url =
        env::var("IPFS_RESOLVER_API_URL").context("IPFS_RESOLVER_API_URL must be set")?;
    let ipfs_api_base = Url::parse(&ipfs_resolver_api_url).context("invalid IPFS API URL")?;
    debug!("using IPFS API base {}", ipfs_api_base);

    let resolve_timeout: u16 = env::var("IPFS_RESOLVER_TIMEOUT_SECS")
        .unwrap_or_else(|_| {
            debug!("IPFS_RESOLVER_TIMEOUT_SECS not provided, using 30");
            "30".to_string()
        })
        .parse::<u16>()
        .context("invalid timeout")?;
    debug!("using IPFS API timeout {}s", resolve_timeout);

    debug!("got CID {}", cid_str);
    let arg_cid = Cid::try_from(cid_str).context("invalid CID")?;

    // Extract the codec from the CID.
    // If it's fs-related, we need it for later, so remember which one it was.
    let codec = match arg_cid.codec() {
        Codec::Raw => &*CODEC_RAW,
        Codec::DagProtobuf => &*CODEC_DAG_PB,
        Codec::DagCBOR => {
            // Let's skip anything not-fs related for now.
            debug!(
                "codec is {:?}, skipping BUT recording for future...",
                arg_cid.codec()
            );
            return Ok(Res::SkippedCBOR);
        }
        _ => {
            // Let's skip anything not-fs related for now.
            debug!("codec is {:?}, skipping...", arg_cid.codec());
            return Ok(Res::SkippedNotFSRelated);
        }
    };

    debug!("getting connection from pool..");
    let conn = conn_pool
        .get()
        .context("unable to get DB connection from pool")?;

    process_single_cid(&ipfs_api_base, resolve_timeout, arg_cid, codec, &conn)
}

fn process_single(cid: &str) {
    let inner = do_stuff(cid).unwrap_or_else(|err| {
        eprintln!("{:?}", err);
        std::process::exit(1)
    });
    match inner {
        Res::Ok => {}
        Res::SkippedNotFSRelated => {}
        Res::SkippedExists => {}
        Res::SkippedCBOR => {
            std::process::exit(2);
        }
        Res::SkippedHAMTShard => {
            std::process::exit(3);
        }
        Res::SkippedMetadata => {
            std::process::exit(4);
        }
        Res::SkippedSymlink => {
            std::process::exit(5);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Res {
    Ok,
    SkippedNotFSRelated,
    SkippedCBOR,
    SkippedExists,
    SkippedHAMTShard,
    SkippedMetadata,
    SkippedSymlink,
}

fn do_stuff(cid_str: &str) -> Result<Res> {
    let ipfs_resolver_api_url =
        env::var("IPFS_RESOLVER_API_URL").context("IPFS_RESOLVER_API_URL must be set")?;
    let ipfs_api_base = Url::parse(&ipfs_resolver_api_url).context("invalid IPFS API URL")?;
    debug!("using IPFS API base {}", ipfs_api_base);

    let resolve_timeout: u16 = env::var("IPFS_RESOLVER_TIMEOUT_SECS")
        .unwrap_or_else(|_| {
            debug!("IPFS_RESOLVER_TIMEOUT_SECS not provided, using 30");
            "30".to_string()
        })
        .parse::<u16>()
        .context("invalid timeout")?;
    debug!("using IPFS API timeout {}s", resolve_timeout);

    debug!("got CID {}", cid_str);
    let arg_cid = Cid::try_from(cid_str).context("invalid CID")?;

    // Extract the codec from the CID.
    // If it's fs-related, we need it for later, so remember which one it was.
    let codec = match arg_cid.codec() {
        Codec::Raw => &*CODEC_RAW,
        Codec::DagProtobuf => &*CODEC_DAG_PB,
        Codec::DagCBOR => {
            // Let's skip anything not-fs related for now.
            debug!(
                "codec is {:?}, skipping BUT recording for future...",
                arg_cid.codec()
            );
            return Ok(Res::SkippedCBOR);
        }
        _ => {
            // Let's skip anything not-fs related for now.
            debug!("codec is {:?}, skipping...", arg_cid.codec());
            return Ok(Res::SkippedNotFSRelated);
        }
    };

    debug!("connecting to DB...");
    let conn = ipfs_resolver_db::establish_connection()?;

    process_single_cid(&ipfs_api_base, resolve_timeout, arg_cid, codec, &conn)
}

fn process_single_cid(
    ipfs_api_base: &Url,
    resolve_timeout: u16,
    arg_cid: Cid,
    codec: &model::Codec,
    conn: &PgConnection,
) -> Result<Res> {
    let cid_string = ipfs_resolver_db::canonicalize_cid(&arg_cid);
    debug!("canonicalized CID to {}", cid_string);

    debug!("checking if block already exists...");
    let exists = db::block_exists(&conn, &cid_string)?;
    if exists {
        debug!("already exists. Skipping");
        // Return Ok, so we don't record this as an error.
        return Ok(Res::SkippedExists);
    }

    debug!("querying IPFS for metadata...");
    let (block_stat, files_stat, object_stat, links) =
        match ipfs::query_ipfs_for_metadata(&ipfs_api_base, resolve_timeout, &cid_string) {
            Ok(s) => Ok(s),
            Err(e) => {
                debug!("unable to query IPFS: {:?}", e);
                match e {
                    ResolveError::ContextDeadlineExceeded => {
                        debug!("deadline exceeded, will record this");
                        return insert_timeout(&conn, &cid_string, codec).map(|_| Res::Ok);
                    }
                    _ => Err(e),
                }
            }
        }
        .context("unable to query IPFS")?;
    debug!(
        "block_stat: {:?}, files_stat: {:?}, object_stat: {:?}, refs: {:?}",
        block_stat, files_stat, object_stat, links
    );

    // If this block is dag-pb, we can parse the object data as UnixFS protobuf.
    // That will give us the actual UnixFS type.
    // If this block is raw, the UnixFS type is raw. (I hope.)

    debug!("determining UnixFSv1 type...");
    let typ: &UnixFSType = match arg_cid.codec() {
        Codec::DagProtobuf => {
            let object_data =
                ipfs::query_ipfs_for_object_data(&ipfs_api_base, resolve_timeout, &cid_string)?;
            let node: unixfs::Data =
                protobuf::parse_from_bytes(&object_data).context("unable to parse protobuf")?;
            match node.get_Type() {
                unixfs::Data_DataType::Directory => &*UNIXFS_TYPE_DIRECTORY,
                unixfs::Data_DataType::File => &*UNIXFS_TYPE_FILE,
                unixfs::Data_DataType::HAMTShard => {
                    // We skip these for now because we need to decode them properly to get actual link names.
                    debug!("skipping HAMTShard block");
                    return Ok(Res::SkippedHAMTShard);
                    //&*UNIXFS_TYPE_HAMT_SHARD
                }
                unixfs::Data_DataType::Metadata => {
                    // We skip these for now because I have no idea how to treat them.
                    debug!("skipping metadata block");
                    return Ok(Res::SkippedMetadata);
                    //&*UNIXFS_TYPE_METADATA
                }
                unixfs::Data_DataType::Symlink => {
                    // We skip these for now because, again, I have no idea how to treat them.
                    debug!("skipping symlink block");
                    return Ok(Res::SkippedSymlink);
                    //&*UNIXFS_TYPE_SYMLINK
                }
                unixfs::Data_DataType::Raw => &*UNIXFS_TYPE_RAW, // This is a bit strange and I think it never actually happens, but whatever.
            }
        }
        Codec::Raw => &*UNIXFS_TYPE_RAW,
        _ => unreachable!(),
    };
    debug!("determined type to be {}", &typ.name);

    // Get first 32 bytes to save for later.
    debug!("getting raw block data...");
    let mut raw_block = Vec::new();
    {
        let mut raw_block_i =
            ipfs::query_ipfs_for_block_get(&ipfs_api_base, resolve_timeout, &cid_string)?;
        raw_block.append(&mut raw_block_i);
        raw_block.truncate(32);
    }

    let heur = if typ.id != UNIXFS_TYPE_RAW.id && typ.id != UNIXFS_TYPE_FILE.id {
        debug!("skipping file heuristics because doesn't look like a file...");
        None
    } else {
        debug!("running file heuristics...");
        let h = heuristics::get_file_heuristics(&ipfs_api_base, resolve_timeout, &cid_string)?;
        debug!("got heuristics {:?}", h);
        Some(h)
    };

    debug!("inserting...");
    conn.transaction(|| {
        debug!("inserting block");
        let block = db::insert_successful_block_into_db(
            &conn,
            cid_string,
            codec.id,
            block_stat,
            raw_block,
            Utc::now().naive_utc(),
        )?;
        debug!("inserted block as {:?}", block);

        debug!("inserting UnixFS block...");
        let unixfs_block = db::insert_unixfs_block(&conn, &block, typ.id, files_stat, object_stat)?;
        debug!("inserted UnixFS block as {:?}", unixfs_block);

        match heur {
            Some(heuristics) => {
                debug!("inserting file heuristics...");
                let heuristics = db::insert_file_heuristics(&conn, &block, heuristics)?;
                debug!("inserted heuristics as {:?}", heuristics);
            }
            None => {}
        }

        debug!("inserting object links...");
        db::insert_object_links(&conn, &block, links)
    })
    .context("unable to insert")?;

    debug!("done.");
    Ok(Res::Ok)
}

fn insert_timeout(conn: &PgConnection, cid_string: &str, codec: &model::Codec) -> Result<()> {
    conn.transaction::<_, Error, _>(|| {
        debug!("inserting failed block");
        let block = db::insert_failed_block_into_db(
            &conn,
            cid_string,
            codec.id,
            &*BLOCK_ERROR_FAILED_TO_GET_BLOCK_DEADLINE_EXCEEDED,
            Utc::now().naive_utc(),
        )?;
        debug!("inserted block as {:?}", block);

        Ok(())
    })
    .context("unable to insert")?;

    debug!("done.");
    Ok(())
}
