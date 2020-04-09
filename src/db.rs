use crate::heuristics::{ChardetHeuristics, FileHeuristics};
use crate::model::*;
use crate::Result;
use diesel::expression::exists::exists;
use diesel::prelude::*;
use diesel::select;
use diesel::PgConnection;
use failure::ResultExt;
use ipfs_api::response;
use std::env;

pub(crate) fn block_exists(conn: &PgConnection, cid: &str) -> Result<bool> {
    use crate::schema::blocks::dsl::*;

    let res = select(exists(blocks.filter(base32_cidv1.eq(cid))))
        .get_result(conn)
        .context("unable to query DB")?;

    Ok(res)
}

pub(crate) fn insert_object_links(
    conn: &PgConnection,
    block: &Block,
    links: response::ObjectLinksResponse,
) -> Result<()> {
    for link in links.links {
        let canonicalized_cid = crate::canonicalize_cid_from_str(&link.hash)
            .context("unable to canonicalize link CID")?;
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

pub(crate) fn insert_file_heuristics(
    conn: &PgConnection,
    block: &Block,
    heuristics: FileHeuristics,
) -> Result<UnixFSFileHeuristics> {
    let ChardetHeuristics {
        charset: chardet_charset,
        language: chardet_language,
        confidence: chardet_confidence,
    } = heuristics.chardet_heuristics.unwrap();
    let chardet_encoding = chardet::charset2encoding(&chardet_charset);

    let heuristics = create_unixfs_file_heuristics(
        conn,
        &block.id,
        heuristics.tree_mime_mime_type.as_deref(),
        Some(chardet_encoding),
        Some(&chardet_language),
        Some(&chardet_confidence),
        heuristics.chardetng_encoding.as_deref(),
        heuristics
            .whatlang_heuristics
            .clone()
            .map(|i| i.lang().eng_name()),
        heuristics
            .whatlang_heuristics
            .clone()
            .map(|i| i.script().name().to_string())
            .as_deref(),
        heuristics
            .whatlang_heuristics
            .map(|i| i.confidence())
            .as_ref(),
    )
    .context("unable to insert UnixFS file heuristics")?;

    Ok(heuristics)
}

pub(crate) fn insert_block_into_db(
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

pub(crate) fn insert_unixfs_block(
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

fn create_block<'a>(
    conn: &PgConnection,
    base32_cidv1: &'a str,
    codec_id: &'a i32,
    block_size: &'a i32,
    first_bytes: &'a Vec<u8>,
) -> Result<Block> {
    use crate::schema::blocks;

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

fn create_failed_block<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    error_id: &'a i32,
) -> Result<FailedBlock> {
    use crate::schema::failed_blocks;

    let new_block = NewFailedBlock { block_id, error_id };

    let inserted_block = diesel::insert_into(failed_blocks::table)
        .values(&new_block)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_block)
}

fn create_unixfs_block<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    unixfs_type_id: &'a i32,
    size: &'a i64,
    cumulative_size: &'a i64,
    blocks: &'a i32,
    num_links: &'a i32,
) -> Result<UnixFSBlock> {
    use crate::schema::unixfs_blocks;

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

fn create_unixfs_link<'a>(
    conn: &PgConnection,
    parent_block_id: &'a i32,
    referenced_base32_cidv1: &'a str,
    name: &'a str,
    size: &'a i64,
) -> Result<UnixFSLink> {
    use crate::schema::unixfs_links;

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

fn create_unixfs_file_heuristics<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    tree_mime_mime_type: Option<&'a str>,
    chardet_encoding: Option<&'a str>,
    chardet_language: Option<&'a str>,
    chardet_confidence: Option<&'a f32>,
    chardetng_encoding: Option<&'a str>,
    whatlang_language: Option<&'a str>,
    whatlang_script: Option<&'a str>,
    whatlang_confidence: Option<&'a f64>,
) -> Result<UnixFSFileHeuristics> {
    use crate::schema::unixfs_file_heuristics;

    let new_heuristics = NewUnixFSFileHeuristics {
        block_id,
        tree_mime_mime_type,
        chardet_encoding,
        chardet_language,
        chardet_confidence,
        chardetng_encoding,
        whatlang_language,
        whatlang_script,
        whatlang_confidence,
    };

    let inserted_heuristics = diesel::insert_into(unixfs_file_heuristics::table)
        .values(&new_heuristics)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_heuristics)
}

pub(crate) fn establish_connection() -> Result<PgConnection> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let conn = PgConnection::establish(&database_url)
        .context(format!("error connecting to {}", database_url))?;

    Ok(conn)
}
