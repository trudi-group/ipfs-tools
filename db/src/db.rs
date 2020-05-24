use crate::model::*;
use diesel::prelude::*;
use diesel::PgConnection;
use failure::ResultExt;
use ipfs_api::response;
use ipfs_resolver_common::Result;

/// Tracks the current status of a block in the database.
#[derive(Clone, Debug)]
pub enum BlockStatus {
    Missing,
    BlockExistsUnixFSMissing(Block),
    BlockExistsUnixFSExists(Block, UnixFSBlock),
    SuccessfulUnixFSMissing(Block, Vec<SuccessfulResolve>),
    SuccessfulUnixFSExists(Block, Vec<SuccessfulResolve>, UnixFSBlock),
    FailedUnixFSMissing(Block, Vec<FailedResolve>),
    FailedUnixFSExists(Block, Vec<FailedResolve>, UnixFSBlock),
    SuccessfulAndFailedUnixFSMissing(Block, Vec<SuccessfulResolve>, Vec<FailedResolve>),
    SuccessfulAndFailedUnixFSExists(
        Block,
        Vec<SuccessfulResolve>,
        Vec<FailedResolve>,
        UnixFSBlock,
    ),
}

pub fn block_exists(conn: &PgConnection, cid: &str) -> Result<BlockStatus> {
    use crate::schema::blocks::dsl::*;
    use crate::schema::failed_resolves::dsl::*;
    use crate::schema::successful_resolves::dsl::*;
    use crate::schema::unixfs_blocks::dsl::*;

    let c = crate::canonicalize_cid_from_str_to_cidv1(cid).context("unable to canonicalize CID")?;
    debug!("canonicalized CID to {:?} with bytes {:?}", c, c.to_bytes());

    let results: Vec<Block> = crate::schema::blocks::dsl::blocks
        .filter(cidv1.eq(c.to_bytes()))
        .load::<Block>(conn)
        .context("unable to query DB for blocks")?;
    if results.is_empty() {
        return Ok(BlockStatus::Missing);
    }
    let block = results[0].clone();

    let succs: Vec<SuccessfulResolve> = successful_resolves
        .filter(crate::schema::successful_resolves::dsl::block_id.eq(block.id))
        .load(conn)
        .context("unable to load successful resolves")?;
    let failed: Vec<FailedResolve> = failed_resolves
        .filter(crate::schema::failed_resolves::dsl::block_id.eq(block.id))
        .load(conn)
        .context("unable to load failed resolves")?;
    let unixfs_block: Option<UnixFSBlock> = unixfs_blocks
        .find(block.id)
        .first(conn)
        .optional()
        .context("unable to load block stat")?;

    if !succs.is_empty() {
        if !failed.is_empty() {
            if let Some(unixfs_block) = unixfs_block {
                return Ok(BlockStatus::SuccessfulAndFailedUnixFSExists(
                    block,
                    succs,
                    failed,
                    unixfs_block,
                ));
            }
            return Ok(BlockStatus::SuccessfulAndFailedUnixFSMissing(
                block, succs, failed,
            ));
        }

        if let Some(unixfs_block) = unixfs_block {
            return Ok(BlockStatus::SuccessfulUnixFSExists(
                block,
                succs,
                unixfs_block,
            ));
        }
        return Ok(BlockStatus::SuccessfulUnixFSMissing(block, succs));
    }
    if !failed.is_empty() {
        if let Some(unixfs_block) = unixfs_block {
            return Ok(BlockStatus::FailedUnixFSExists(block, failed, unixfs_block));
        }
        return Ok(BlockStatus::FailedUnixFSMissing(block, failed));
    }

    if let Some(unixfs_block) = unixfs_block {
        Ok(BlockStatus::BlockExistsUnixFSExists(block, unixfs_block))
    } else {
        Ok(BlockStatus::BlockExistsUnixFSMissing(block))
    }
}

/*
// These functions were only required during a DB migration to translate base32 CIDs into BYTEA
// versions.

pub fn get_blocks_without_cidv1(
    conn: &PgConnection,
    count: i64,
    offset: i64,
) -> Result<Vec<Block>> {
    use crate::schema::blocks::dsl::*;

    let b: Vec<Block> = blocks
        .filter(cidv1.is_null())
        .limit(count)
        .offset(offset)
        .get_results(conn)?;

    Ok(b)
}

pub fn update_blocks_with_cidv1(conn: &PgConnection, bs: Vec<Block>) -> Result<()> {
    use crate::schema::blocks::dsl::*;

    conn.transaction(|| {
        for b in bs.iter() {
            let res = diesel::update(b)
                .set(cidv1.eq(b.cidv1.clone()))
                .execute(conn)
                .context(format!("unable to update block {}", b.id))?;
            if res != 1 {
                return Err(err_msg(format!(
                    "update of block {} affected {} rows",
                    b.id, res
                )));
            }
        }

        Ok(())
    })
    .context("unable to perform transaction")?;

    Ok(())
}

pub fn get_unixfs_links_without_cidv1(conn: &PgConnection, count: i64) -> Result<Vec<UnixFSLink>> {
    use crate::schema::unixfs_links::dsl::*;

    let l: Vec<UnixFSLink> = unixfs_links
        .filter(referenced_cidv1.is_null())
        .limit(count)
        .get_results(conn)?;

    Ok(l)
}

pub fn update_unixfs_links_with_cidv1(conn: &PgConnection, ls: Vec<UnixFSLink>) -> Result<()> {
    use crate::schema::unixfs_links::dsl::*;

    conn.transaction(|| {
        for l in ls.iter() {
            let res = diesel::update(l)
                .set(referenced_cidv1.eq(l.referenced_cidv1.clone()))
                .execute(conn)
                .context(format!(
                    "unable to update unixfs link for block {} with base32_cidv1 {}",
                    l.parent_block_id, l.referenced_base32_cidv1
                ))?;
            if res != 1 {
                return Err(err_msg(format!(
                    "update of unixfs link for block {} with base32_cidv1 {} affected {} rows",
                    l.parent_block_id, l.referenced_base32_cidv1, res
                )));
            }
        }

        Ok(())
    })
    .context("unable to perform transaction")?;

    Ok(())
}
*/

pub fn count_blocks(conn: &PgConnection) -> Result<i64> {
    use crate::schema::blocks;

    let res = blocks::table.count().get_result(conn)?;

    Ok(res)
}

pub fn count_unixfs_blocks(conn: &PgConnection) -> Result<i64> {
    use crate::schema::unixfs_blocks;

    let res = unixfs_blocks::table.count().get_result(conn)?;

    Ok(res)
}

pub fn count_successful_resolves(conn: &PgConnection) -> Result<i64> {
    use crate::schema::successful_resolves;

    let res = successful_resolves::table.count().get_result(conn)?;

    Ok(res)
}

pub fn count_failed_resolves(conn: &PgConnection) -> Result<i64> {
    use crate::schema::failed_resolves;

    let res = failed_resolves::table.count().get_result(conn)?;

    Ok(res)
}

pub fn insert_object_links(
    conn: &PgConnection,
    block: &Block,
    links: response::ObjectLinksResponse,
) -> Result<()> {
    for link in links.links {
        let c = crate::canonicalize_cid_from_str_to_cidv1(&link.hash)
            .context("unable to canonicalize CID")?;
        debug!("canonicalized CID to {:?} with bytes {:?}", c, c.to_bytes());
        debug!(
            "inserting link (parent id={}, cid={:?}, name={}, size={})",
            block.id, c, link.name, link.size
        );
        create_unixfs_link(
            conn,
            &block.id,
            &c.to_bytes(),
            &link.name,
            &(link.size as i64),
        )
        .context("unable to insert link")?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct FileHeuristics {
    pub chardet_heuristics: Option<ChardetHeuristics>,
    pub tree_mime_mime_type: Option<String>,
    pub chardetng_encoding: Option<String>,
    pub whatlang_heuristics: Option<whatlang::Info>,
}

#[derive(Debug, Clone)]
pub struct ChardetHeuristics {
    pub charset: String,
    pub language: String,
    pub confidence: f32,
}

pub fn insert_file_heuristics(
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

pub fn insert_failed_block_into_db(
    conn: &PgConnection,
    cid_string: &str,
    codec_id: i32,
    err: &BlockError,
    ts: chrono::NaiveDateTime,
) -> Result<Block> {
    let c = crate::canonicalize_cid_from_str_to_cidv1(&cid_string)
        .context("unable to canonicalize CID")?;
    debug!("canonicalized CID to {:?} with bytes {:?}", c, c.to_bytes());

    let block = create_block(conn, &c.to_bytes(), &codec_id).context("unable to insert block")?;

    create_failed_resolve(conn, &block.id, &err.id, &ts)
        .context("unable to insert successful resolve")?;

    Ok(block)
}

pub fn insert_failed_resolve_into_db(
    conn: &PgConnection,
    block: &Block,
    err: &BlockError,
    ts: chrono::NaiveDateTime,
) -> Result<()> {
    create_failed_resolve(conn, &block.id, &err.id, &ts).context("unable to insert")?;

    Ok(())
}

pub fn insert_successful_block_into_db(
    conn: &PgConnection,
    cid_string: String,
    codec_id: i32,
    block_stat: response::BlockStatResponse,
    first_bytes: Vec<u8>,
    ts: chrono::NaiveDateTime,
) -> Result<Block> {
    let c = crate::canonicalize_cid_from_str_to_cidv1(&cid_string)
        .context("unable to canonicalize CID")?;
    debug!("canonicalized CID to {:?} with bytes {:?}", c, c.to_bytes());

    let block = create_block(conn, &c.to_bytes(), &codec_id).context("unable to insert block")?;

    create_block_stat(conn, &block.id, &(block_stat.size as i32), &first_bytes)
        .context("unable to insert block stat")?;

    create_successful_resolve(conn, &block.id, &ts)
        .context("unable to insert successful resolve")?;

    Ok(block)
}

pub fn insert_first_successful_resolve_into_db(
    conn: &PgConnection,
    block: &Block,
    block_stat: response::BlockStatResponse,
    first_bytes: Vec<u8>,
    ts: chrono::NaiveDateTime,
) -> Result<()> {
    create_block_stat(conn, &block.id, &(block_stat.size as i32), &first_bytes)
        .context("unable to insert block stat")?;

    create_successful_resolve(conn, &block.id, &ts)
        .context("unable to insert successful resolve")?;

    Ok(())
}

pub fn insert_additional_successful_resolve_into_db(
    conn: &PgConnection,
    block: &Block,
    ts: chrono::NaiveDateTime,
) -> Result<()> {
    create_successful_resolve(conn, &block.id, &ts)
        .context("unable to insert successful resolve")?;

    Ok(())
}

pub fn insert_unixfs_block(
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

fn create_block<'a>(conn: &PgConnection, cidv1: &'a Vec<u8>, codec_id: &'a i32) -> Result<Block> {
    use crate::schema::blocks;

    let new_block = NewBlock { cidv1, codec_id };

    let inserted_block = diesel::insert_into(blocks::table)
        .values(&new_block)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_block)
}

fn create_block_stat<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    block_size: &'a i32,
    first_bytes: &'a Vec<u8>,
) -> Result<BlockStat> {
    use crate::schema::block_stats;

    let new_stat = NewBlockStat {
        block_id,
        block_size,
        first_bytes,
    };

    let inserted_stat = diesel::insert_into(block_stats::table)
        .values(&new_stat)
        .on_conflict_do_nothing()
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_stat)
}

fn create_failed_resolve<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    error_id: &'a i32,
    ts: &'a chrono::NaiveDateTime,
) -> Result<FailedResolve> {
    use crate::schema::failed_resolves;

    let failed_resolve = NewFailedResolve {
        block_id,
        error_id,
        ts,
    };

    let inserted_resolve = diesel::insert_into(failed_resolves::table)
        .values(&failed_resolve)
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_resolve)
}

fn create_successful_resolve<'a>(
    conn: &PgConnection,
    block_id: &'a i32,
    ts: &'a chrono::NaiveDateTime,
) -> Result<SuccessfulResolve> {
    use crate::schema::successful_resolves;

    let successful_resolve = NewSuccessfulResolve { block_id, ts };

    let inserted_resolve = diesel::insert_into(successful_resolves::table)
        .values(&successful_resolve)
        .get_result(conn)
        .context("unable to insert")?;

    Ok(inserted_resolve)
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
    referenced_cidv1: &'a Vec<u8>,
    name: &'a str,
    size: &'a i64,
) -> Result<UnixFSLink> {
    use crate::schema::unixfs_links;

    let new_link = NewUnixFSLink {
        parent_block_id,
        referenced_cidv1,
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
