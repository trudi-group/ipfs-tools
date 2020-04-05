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

use chardet::charset2encoding;
use cid::{Cid, Codec};
use diesel::expression::exists::exists;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::select;
use dotenv::dotenv;
use failure::{err_msg, Error, Fail, ResultExt};
use flexi_logger::{DeferredNow, Duplicate, Logger, ReconfigurationHandle};
use ipfs_api::response;
use log::Record;
use model::*;
use reqwest::Url;
use std::convert::TryFrom;
use std::env;

type Result<T> = std::result::Result<T, Error>;

enum ResolveError {
    FailedToGetBlockContextDeadlineExceeded,
}

#[tokio::main]
async fn main() -> Result<()> {
    set_up_logging(false)?;

    let arg_cid = get_v1_cid_from_args().context("unable to parse CID from args")?;
    match arg_cid.codec() {
        Codec::Raw | Codec::DagProtobuf => {}
        _ => {
            // Let's skip anything not-fs related for now.
            debug!("codec is {:?}, skipping...", arg_cid.codec());
            return Ok(());
        }
    }

    let codec = match arg_cid.codec() {
        Codec::Raw => &*CODEC_RAW,
        Codec::DagProtobuf => &*CODEC_DAG_PB,
        _ => unreachable!(),
    };

    let cid_string = canonicalize_cid(&arg_cid);
    debug!("canonicalized CID to {}", cid_string);

    debug!("connecting to DB...");
    let conn = establish_connection()?;

    debug!("checking if block already exists...");
    let exists = block_exists(&conn, &cid_string)?;
    if exists {
        debug!("already exists. Skipping");
        // Return Ok, so we don't record this as an error.
        return Ok(());
    }

    debug!("querying IPFS for metadata...");
    let (block_stat, files_stat, object_stat, links) = query_ipfs_for_metadata(&cid_string)
        .await
        .context("unable to query IPFS")?;
    debug!(
        "block_stat: {:?}, files_stat: {:?}, object_stat: {:?}, refs: {:?}",
        block_stat, files_stat, object_stat, links
    );

    // If this block is dag-pb, we can parse the object data as UnixFS protobuf.
    // That will give us the actual UnixFS type.
    // If this block is raw, the UnixFS type is raw.

    debug!("determining UnixFSv1 type...");
    let typ: &UnixFSType = match arg_cid.codec() {
        Codec::DagProtobuf => {
            let object_data = query_ipfs_for_object_data(&cid_string).await?;
            let node: unixfs::Data =
                protobuf::parse_from_bytes(&object_data).context("unable to parse protobuf")?;
            match node.get_Type() {
                unixfs::Data_DataType::Directory => &*UNIXFS_TYPE_DIRECTORY,
                unixfs::Data_DataType::File => &*UNIXFS_TYPE_FILE,
                unixfs::Data_DataType::HAMTShard => &*UNIXFS_TYPE_HAMT_SHARD,
                unixfs::Data_DataType::Metadata => &*UNIXFS_TYPE_METADATA,
                unixfs::Data_DataType::Symlink => &*UNIXFS_TYPE_SYMLINK,
                unixfs::Data_DataType::Raw => &*UNIXFS_TYPE_RAW, // this is a bit strange
            }
        }
        Codec::Raw => &*UNIXFS_TYPE_RAW,
        _ => unreachable!(),
    };
    debug!("determined type to be {}", &typ.name);

    debug!("getting raw block data...");
    let mut raw_block = query_ipfs_for_block_get(&cid_string).await?;
    raw_block.truncate(32);

    let heuristics = if typ.id != UNIXFS_TYPE_RAW.id && typ.id != UNIXFS_TYPE_FILE.id {
        debug!("skipping file heuristics because doesn't look like a file...");
        None
    } else {
        debug!("running file heuristics...");
        let (mime_type, charset, language, confidence) = get_file_heuristics(&cid_string).await?;
        debug!(
            "got mime_type={}, charset={}, language={}, confidence={}",
            mime_type, charset, language, confidence
        );
        Some((mime_type, charset, language, confidence))
    };

    debug!("inserting...");
    conn.transaction(|| {
        debug!("inserting block");
        let block = insert_block_into_db(&conn, cid_string, block_stat, codec.id, raw_block)?;
        debug!("inserted block as {:?}", block);

        debug!("inserting UnixFS block...");
        let unixfs_block = insert_unixfs_block(&conn, &block, typ.id, files_stat, object_stat)?;
        debug!("inserted UnixFS block as {:?}", unixfs_block);

        if heuristics.is_some() {
            debug!("inserting file heuristics...");
            let (mime_type, charset, language, confidence) = heuristics.unwrap();
            let heuristics =
                insert_file_heuristics(&conn, &block, mime_type, charset, language, confidence)?;
            debug!("inserted heuristics as {:?}", heuristics);
        }

        debug!("inserting object links...");
        insert_object_links(&conn, &block, links)
    })
    .context("unable to insert")?;

    debug!("done.");
    Ok(())
}

fn insert_block_into_db(
    conn: &PgConnection,
    cid_string: String,
    block_stat: response::BlockStatResponse,
    codec_id: i32,
    first_bytes: Vec<u8>,
) -> Result<Block> {
    let block = create_block(
        conn,
        cid_string.as_str(),
        &codec_id,
        &(block_stat.size as i32),
        &first_bytes,
    )
    .context("unable to insert block")?;

    Ok(block)
}

fn insert_unixfs_block(
    conn: &PgConnection,
    block: &Block,
    unixfs_file_type_id: i32,
    files_stat: response::FilesStatResponse,
    object_stat: response::ObjectStatResponse,
) -> Result<UnixFSBlock> {
    let unixfs_block = create_unixfs_block(
        conn,
        &block.id,
        &unixfs_file_type_id,
        &(files_stat.size as i64),
        &(files_stat.cumulative_size as i64),
        &(files_stat.blocks as i32),
        &(object_stat.num_links as i32),
    )
    .context("unable to insert UnixFS block")?;

    Ok(unixfs_block)
}

fn insert_file_heuristics(
    conn: &PgConnection,
    block: &Block,
    mime_type: String,
    charset: String,
    language: String,
    chardet_confidence: f32,
) -> Result<UnixFSFileHeuristics> {
    let heuristics = create_unixfs_file_heuristics(
        conn,
        &block.id,
        Some(&mime_type),
        Some(charset2encoding(&charset)),
        Some(&language),
        Some(&chardet_confidence),
    )
    .context("unable to insert UnixFS file heuristics")?;

    Ok(heuristics)
}

fn insert_object_links(
    conn: &PgConnection,
    block: &Block,
    links: response::ObjectLinksResponse,
) -> Result<()> {
    for link in links.links {
        let canonicalized_cid =
            canonicalize_cid_from_str(&link.hash).context("unable to canonicalize link CID")?;
        debug!("canonicalized link CID to {}", canonicalized_cid);
        debug!(
            "inserting link (parent id={}, cid={}, name={}, size={})",
            block.id, canonicalized_cid, link.name, link.size
        );
        create_unixfs_link(
            conn,
            &block.id,
            &canonicalized_cid,
            &link.name,
            &(link.size as i64),
        )
        .context("unable to insert link")?;
    }

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

fn canonicalize_cid_from_str(cid: &str) -> Result<String> {
    let provided_cid = Cid::try_from(cid).context("invalid CID")?;
    Ok(canonicalize_cid(&provided_cid))
}

fn canonicalize_cid(c: &Cid) -> String {
    let v1_cid = Cid::new_v1(c.codec(), c.hash().to_owned());
    multibase::encode(multibase::Base::Base32Lower, v1_cid.to_bytes())
}

async fn get_file_heuristics(cid_string: &str) -> Result<(String, String, String, f32)> {
    let data = query_ipfs_for_cat(cid_string, 10 * 1024)
        .await
        .context("unable to /cat file")?;

    let (charset, confidence, language) = chardet::detect(&data);
    let mime_type = tree_magic::from_u8(&data);

    Ok((mime_type, charset, language, confidence))
}

async fn query_ipfs_for_cat(cid_string: &str, length: u64) -> Result<Vec<u8>> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let cat_url = match length {
        0 => Url::parse(&format!(
            "{}/cat?arg={}&timeout=30s",
            base, ipfs_prefixed_cid
        )),
        _ => Url::parse(&format!(
            "{}/cat?arg={}&timeout=30s&length={}",
            base, ipfs_prefixed_cid, length
        )),
    }
    .expect("invalid URL...");

    query_ipfs_api_raw(cat_url).await
}

async fn query_ipfs_for_block_get(cid_string: &str) -> Result<Vec<u8>> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let block_get_url = Url::parse(&format!(
        "{}/block/get?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");

    query_ipfs_api_raw(block_get_url).await
}

async fn query_ipfs_for_object_data(cid_string: &str) -> Result<Vec<u8>> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let object_data_url = Url::parse(&format!(
        "{}/object/data?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");

    query_ipfs_api_raw(object_data_url).await
}

async fn query_ipfs_for_metadata(
    cid_string: &str,
) -> Result<(
    response::BlockStatResponse,
    response::FilesStatResponse,
    response::ObjectStatResponse,
    response::ObjectLinksResponse,
)> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let block_stat_url = Url::parse(&format!(
        "{}/block/stat?arg={}&timeout=30s",
        base, cid_string
    ))
    .expect("invalid URL...");
    let files_stat_url = Url::parse(&format!(
        "{}/files/stat?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");
    let object_stat_url = Url::parse(&format!(
        "{}/object/stat?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");
    let object_links_url = Url::parse(&format!(
        "{}/object/links?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");

    let block_stat: response::BlockStatResponse = query_ipfs_api(block_stat_url)
        .await
        .context("unable to query IPFS API /block/stat")?;
    let files_stat: response::FilesStatResponse = query_ipfs_api(files_stat_url)
        .await
        .context("unable to query IPFS API /files/stat")?;
    let object_stat: response::ObjectStatResponse = query_ipfs_api(object_stat_url)
        .await
        .context("unable to query IPFS API /object/stat")?;

    // The IPFS HTTP API leaves out the "Links" field if there are no refs, which in turn causes
    // JSON parsing to fail. So if we have no links, just return a dummy response...
    if object_stat.num_links == 0 {
        return Ok((
            block_stat,
            files_stat,
            object_stat,
            response::ObjectLinksResponse {
                hash: cid_string.to_string(),
                links: vec![],
            },
        ));
    }
    let refs: response::ObjectLinksResponse = query_ipfs_api(object_links_url)
        .await
        .context("unable to query IPFS API /object/links")?;

    Ok((block_stat, files_stat, object_stat, refs))
}

async fn query_ipfs_api_raw(url: Url) -> Result<Vec<u8>> {
    let resp = reqwest::get(url)
        .await
        .context("unable to query IPFS API")?;

    match resp.status() {
        hyper::StatusCode::OK => {
            // parse as T
            let body = resp.bytes().await.context("unable to read body")?;
            Ok(body.to_vec())
        }
        _ => {
            // try to parse as IPFS error...
            let body = resp.bytes().await.context("unable to read body")?;
            let err = serde_json::from_slice::<ipfs_api::response::ApiError>(&body);
            match err {
                Ok(err) => Err(ipfs_api::response::Error::Api(err).into()),
                Err(_) => {
                    // just return the body I guess...
                    let err_text =
                        String::from_utf8(body.to_vec()).context("response is invalid UTF8")?;
                    Err(err_msg(format!(
                        "unable to parse IPFS API response: {}",
                        err_text
                    )))
                }
            }
        }
    }
}

async fn query_ipfs_api<Res>(url: Url) -> Result<Res>
where
    for<'de> Res: 'static + serde::Deserialize<'de>,
{
    let body = query_ipfs_api_raw(url).await?;

    let parsed = serde_json::from_slice::<Res>(&body);
    match parsed {
        Ok(parsed) => Ok(parsed),
        Err(_) => {
            // try to parse as IPFS error instead...
            let err = serde_json::from_slice::<ipfs_api::response::ApiError>(&body);
            match err {
                Ok(err) => Err(ipfs_api::response::Error::Api(err).into()),
                Err(_) => {
                    // just return the body I guess...
                    let err_text =
                        String::from_utf8(body.to_vec()).context("response is invalid UTF8")?;
                    Err(err_msg(format!(
                        "unable to parse IPFS API response: {}",
                        err_text
                    )))
                }
            }
        }
    }
}

fn block_exists(conn: &PgConnection, cid: &str) -> Result<bool> {
    use schema::blocks::dsl::*;

    let res = select(exists(blocks.filter(base32_cidv1.eq(cid))))
        .get_result(conn)
        .context("unable to query DB")?;

    Ok(res)
}

pub fn create_block<'a>(
    conn: &PgConnection,
    base32_cidv1: &'a str,
    codec_id: &'a i32,
    block_size: &'a i32,
    first_bytes: &'a Vec<u8>,
) -> Result<Block> {
    use schema::blocks;

    let new_block = NewBlock {
        base32_cidv1,
        codec_id,
        block_size,
        first_bytes,
    };

    let inserted_block = diesel::insert_into(blocks::table)
        .values(&new_block)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_block)
}

pub fn create_failed_block<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    error_id: &'a i32,
) -> Result<FailedBlock> {
    use schema::failed_blocks;

    let new_block = NewFailedBlock { block_id, error_id };

    let inserted_block = diesel::insert_into(failed_blocks::table)
        .values(&new_block)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_block)
}

pub fn create_unixfs_block<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    unixfs_type_id: &'a i32,
    size: &'a i64,
    cumulative_size: &'a i64,
    blocks: &'a i32,
    num_links: &'a i32,
) -> Result<UnixFSBlock> {
    use schema::unixfs_blocks;

    let new_block = NewUnixFSBlock {
        block_id,
        unixfs_type_id,
        size,
        cumulative_size,
        blocks,
        num_links,
    };

    let inserted_block = diesel::insert_into(unixfs_blocks::table)
        .values(&new_block)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_block)
}

pub fn create_unixfs_link<'a>(
    conn: &PgConnection,
    parent_block_id: &'a i32,
    referenced_base32_cidv1: &'a str,
    name: &'a str,
    size: &'a i64,
) -> Result<UnixFSLink> {
    use schema::unixfs_links;

    let new_link = NewUnixFSLink {
        parent_block_id,
        referenced_base32_cidv1,
        name,
        size,
    };

    let inserted_link = diesel::insert_into(unixfs_links::table)
        .values(&new_link)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_link)
}

pub fn create_unixfs_file_heuristics<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    mime_type: Option<&'a str>,
    encoding: Option<&'a str>,
    language: Option<&'a str>,
    chardet_confidence: Option<&'a f32>,
) -> Result<UnixFSFileHeuristics> {
    use schema::unixfs_file_heuristics;

    let new_heuristics = NewUnixFSFileHeuristics {
        block_id,
        mime_type,
        encoding,
        language,
        chardet_confidence,
    };

    let inserted_heuristics = diesel::insert_into(unixfs_file_heuristics::table)
        .values(&new_heuristics)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_heuristics)
}

pub fn establish_connection() -> Result<PgConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let conn = PgConnection::establish(&database_url)
        .context(format!("error connecting to {}", database_url))?;

    Ok(conn)
}

pub(crate) fn log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> std::result::Result<(), std::io::Error> {
    write!(
        w,
        "[{}] {} [{}] {}:{}: {}",
        now.now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
        record.level(),
        record.metadata().target(),
        //record.module_path().unwrap_or("<unnamed>"),
        record.file().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        &record.args()
    )
}

pub(crate) fn set_up_logging(log_to_file: bool) -> Result<ReconfigurationHandle> {
    let mut logger = Logger::with_env_or_str("debug").format(log_format);
    if log_to_file {
        logger = logger
            //.log_to_file()
            //.directory("logs")
            .duplicate_to_stderr(Duplicate::All)
            /*.rotate(
                Criterion::Size(100_000_000),
                Naming::Timestamps,
                Cleanup::KeepLogFiles(10),
            )*/;
    }

    let handle = logger.start()?;

    Ok(handle)
}
