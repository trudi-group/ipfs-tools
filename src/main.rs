#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate diesel;
extern crate dotenv;
#[macro_use]
extern crate lazy_static;

mod model;
mod schema;
mod unixfs;
mod db;
mod heuristics;
mod ipfs;
mod logging;

use crate::model::*;
use cid::{Cid, Codec};
use diesel::Connection;
use failure::{err_msg, Error, Fail, ResultExt};
use reqwest::Url;
use std::convert::TryFrom;
use std::env;

pub(crate) type Result<T> = std::result::Result<T, Error>;

enum ResolveError {
    FailedToGetBlockContextDeadlineExceeded,
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::set_up_logging(false)?;

    dotenv::dotenv().ok();
    let ipfs_resolver_api_url =
        env::var("IPFS_RESOLVER_API_URL").context("IPFS_RESOLVER_API_URL must be set")?;
    let ipfs_api_base = Url::parse(&ipfs_resolver_api_url).context("invalid IPFS API URL")?;
    debug!("using IPFS API base {}",ipfs_api_base);

    let resolve_timeout: u16 = env::var("IPFS_RESOLVER_TIMEOUT_SECS")
        .unwrap_or_else(|_| {
            debug!("IPFS_RESOLVER_TIMEOUT_SECS not provided, using 30");
            "30".to_string()
        })
        .parse::<u16>()
        .context("invalid timeout")?;
    debug!("using IPFS API timeout {}s",resolve_timeout);

    let arg_cid = get_v1_cid_from_args().context("unable to parse CID from args")?;

    // Extract the codec from the CID.
    // If it's fs-related, we need it for later, so remember which one it was.
    let codec = match arg_cid.codec() {
        Codec::Raw => &*CODEC_RAW,
        Codec::DagProtobuf => &*CODEC_DAG_PB,
        _ => {
            // Let's skip anything not-fs related for now.
            debug!("codec is {:?}, skipping...", arg_cid.codec());
            return Ok(());
        }
    };

    let cid_string = canonicalize_cid(&arg_cid);
    debug!("canonicalized CID to {}", cid_string);

    debug!("connecting to DB...");
    let conn = db::establish_connection()?;

    debug!("checking if block already exists...");
    let exists = db::block_exists(&conn, &cid_string)?;
    if exists {
        debug!("already exists. Skipping");
        // Return Ok, so we don't record this as an error.
        return Ok(());
    }

    debug!("querying IPFS for metadata...");
    let (block_stat, files_stat, object_stat, links) =
        ipfs::query_ipfs_for_metadata(&ipfs_api_base, resolve_timeout, &cid_string)
            .await
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
                ipfs::query_ipfs_for_object_data(&ipfs_api_base, resolve_timeout, &cid_string)
                    .await?;
            let node: unixfs::Data =
                protobuf::parse_from_bytes(&object_data).context("unable to parse protobuf")?;
            match node.get_Type() {
                unixfs::Data_DataType::Directory => &*UNIXFS_TYPE_DIRECTORY,
                unixfs::Data_DataType::File => &*UNIXFS_TYPE_FILE,
                unixfs::Data_DataType::HAMTShard => {
                    // We skip these for now because we need to decode them properly to get actual link names.
                    debug!("skipping HAMTShard block");
                    return Ok(());
                    //&*UNIXFS_TYPE_HAMT_SHARD
                }
                unixfs::Data_DataType::Metadata => {
                    // We skip these for now because I have no idea how to treat them.
                    debug!("skipping metadata block");
                    return Ok(());
                    //&*UNIXFS_TYPE_METADATA
                }
                unixfs::Data_DataType::Symlink => {
                    // We skip these for now because, again, I have no idea how to treat them.
                    debug!("skipping symlink block");
                    return Ok(());
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
    let mut raw_block =
        ipfs::query_ipfs_for_block_get(&ipfs_api_base, resolve_timeout, &cid_string).await?;
    raw_block.truncate(32);

    let heur = if typ.id != UNIXFS_TYPE_RAW.id && typ.id != UNIXFS_TYPE_FILE.id {
        debug!("skipping file heuristics because doesn't look like a file...");
        None
    } else {
        debug!("running file heuristics...");
        let h =
            heuristics::get_file_heuristics(&ipfs_api_base, resolve_timeout, &cid_string).await?;
        debug!("got heuristics {:?}", h);
        Some(h)
    };

    debug!("inserting...");
    conn.transaction(|| {
        debug!("inserting block");
        let block = db::insert_block_into_db(&conn, cid_string, block_stat, codec.id, raw_block)?;
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
    Ok(())
}

fn get_v1_cid_from_args() -> Result<Cid> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        error!("run as {} <CID>", args[0]);
        return Err(err_msg("need CID"));
    }
    debug!("got CID {}", args[1]);
    let cid = Cid::try_from(args[1].as_str()).context("invalid CID")?;
    Ok(cid)
}

pub(crate) fn canonicalize_cid_from_str(cid: &str) -> Result<String> {
    let provided_cid = Cid::try_from(cid).context("invalid CID")?;
    Ok(canonicalize_cid(&provided_cid))
}

fn canonicalize_cid(c: &Cid) -> String {
    let v1_cid = Cid::new_v1(c.codec(), c.hash().to_owned());
    multibase::encode(multibase::Base::Base32Lower, v1_cid.to_bytes())
}
