#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate diesel;
extern crate dotenv;

mod model;
mod schema;

use cid::Cid;
use diesel::expression::exists::exists;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::select;
use dotenv::dotenv;
use failure::{err_msg, Error, ResultExt};
use flexi_logger::{DeferredNow, Duplicate, Logger, ReconfigurationHandle};
use ipfs_api::response;
use log::Record;
use model::*;
use reqwest::Url;
use std::convert::TryFrom;
use std::env;

type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() -> Result<()> {
    set_up_logging(false)?;

    let cid_string = get_canonicalized_cid_from_args().context("unable to parse CID from args")?;
    debug!("canonicalized CID to {}", cid_string);

    debug!("connecting to DB...");
    let conn = establish_connection()?;

    debug!("checking if CID already exists...");
    let exists = resolved_cid_exists(&conn, &cid_string)?;
    if exists {
        debug!("already exists. Skipping");
        // Return Ok, so we don't record this as an error.
        return Ok(());
    }

    debug!("querying IPFS...");
    let (files_stat, object_stat, refs) = query_ipfs_for_cid(&cid_string)
        .await
        .context("unable to query IPFS")?;
    debug!(
        "files_stat: {:?}, object_stat: {:?}, refs: {:?}",
        files_stat, object_stat, refs
    );

    let type_id = match files_stat.typ.to_lowercase().as_str() {
        "file" => Ok(TYPE_FILE),
        "directory" => Ok(TYPE_DIRECTORY),
        _ => Err(err_msg(format!("unknown object type {}", files_stat.typ))),
    }?;

    debug!("inserting...");
    conn.transaction(|| insert_into_db(cid_string, &conn, files_stat, object_stat, refs, type_id))
        .context("unable to insert")?;

    debug!("done.");
    Ok(())
}

fn insert_into_db(
    cid_string: String,
    conn: &PgConnection,
    files_stat: response::FilesStatResponse,
    object_stat: response::ObjectStatResponse,
    refs: response::ObjectLinksResponse,
    type_id: i32,
) -> Result<()> {
    let resolved_cid = create_resolved_cid(
        &conn,
        cid_string.as_str(),
        &type_id,
        &(files_stat.cumulative_size as i64),
        &(object_stat.block_size as i64),
        &(object_stat.links_size as i64),
        &(object_stat.data_size as i64),
        &(object_stat.num_links as i32),
        &(files_stat.blocks as i32),
    )
    .context("unable to insert resolved CID")?;
    debug!("inserted base CID as {:?}", resolved_cid);

    for reference in refs.links {
        let canonicalized_cid =
            canonicalize_cid(&reference.hash).context("unable to canonicalize reference CID")?;
        debug!("canonicalized reference CID to {}", canonicalized_cid);
        debug!(
            "inserting reference (parent id={}, cid={}, name={}, size={})",
            resolved_cid.id, canonicalized_cid, reference.name, reference.size
        );
        create_reference(
            &conn,
            &resolved_cid.id,
            &canonicalized_cid,
            &reference.name,
            &(reference.size as i64),
        )
        .context("unable to insert reference")?;
    }

    Ok(())
}

fn get_canonicalized_cid_from_args() -> Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        error!("run as {} <CID>", args[0]);
        return Err(err_msg("need CID"));
    }
    debug!("got CID {}", args[1]);
    canonicalize_cid(args[1].as_str())
}

fn canonicalize_cid(cid: &str) -> Result<String> {
    let provided_cid = Cid::try_from(cid).context("invalid CID")?;
    let v1_cid = Cid::new_v1(provided_cid.codec(), provided_cid.hash().to_owned());
    Ok(multibase::encode(
        multibase::Base::Base32Lower,
        v1_cid.to_bytes(),
    ))
}

async fn query_ipfs_for_cid(
    cid_string: &str,
) -> Result<(
    response::FilesStatResponse,
    response::ObjectStatResponse,
    response::ObjectLinksResponse,
)> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
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

    Ok((files_stat, object_stat, refs))
}

async fn query_ipfs_api<Res>(url: Url) -> Result<Res>
where
    for<'de> Res: 'static + serde::Deserialize<'de>,
{
    let resp = reqwest::get(url)
        .await
        .context("unable to query IPFS API")?;

    match resp.status() {
        hyper::StatusCode::OK => {
            // parse as T
            let body = resp.bytes().await.context("unable to read body")?;
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
                            let err_text = String::from_utf8(body.to_vec())
                                .context("response is invalid UTF8")?;
                            Err(err_msg(format!(
                                "unable to parse IPFS API response: {}",
                                err_text
                            )))
                        }
                    }
                }
            }
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

fn resolved_cid_exists(conn: &PgConnection, cid: &str) -> Result<bool> {
    use schema::resolved_cids::dsl::*;

    let res = select(exists(resolved_cids.filter(base32_cidv1.eq(cid))))
        .get_result(conn)
        .context("unable to query DB")?;

    Ok(res)
}

pub fn create_resolved_cid<'a>(
    conn: &PgConnection,
    base32_cidv1: &'a str,
    type_id: &'a i32,
    cumulative_size: &'a i64,
    block_size: &'a i64,
    links_size: &'a i64,
    data_size: &'a i64,
    num_links: &'a i32,
    blocks: &'a i32,
) -> Result<ResolvedCID> {
    use schema::resolved_cids;

    let new_resolved = NewResolvedCID {
        base32_cidv1,
        type_id,
        cumulative_size,
        block_size,
        links_size,
        data_size,
        num_links,
        blocks,
    };

    let inserted_cid = diesel::insert_into(resolved_cids::table)
        .values(&new_resolved)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to save to DB")?;

    Ok(inserted_cid)
}

pub fn create_reference<'a>(
    conn: &PgConnection,
    parent_cid_id: &'a i32,
    referenced_base32_cidv1: &'a str,
    name: &'a str,
    size: &'a i64,
) -> Result<Reference> {
    use schema::refs;

    let new_ref = NewReference {
        base_cid_id: parent_cid_id,
        referenced_base32_cidv1,
        name,
        size,
    };

    let inserted_ref = diesel::insert_into(refs::table)
        .values(&new_ref)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to save to DB")?;

    Ok(inserted_ref)
}

pub fn establish_connection() -> Result<PgConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let conn = PgConnection::establish(&database_url)
        .context(format!("Error connecting to {}", database_url))?;

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
