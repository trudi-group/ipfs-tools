#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;

use failure::{err_msg, ResultExt};
use ipfs_resolver_common::logging;
use ipfs_resolver_common::Result;
use ipfs_resolver_db::canonicalize_cid_from_str_to_cidv1;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Seek;

const HEADER: &str = "% asym unweighted";

#[derive(Debug, Deserialize)]
struct Block {
    id: i64,
    cidv1: String,
}

#[derive(Debug, Deserialize)]
struct Link {
    parent_block_id: i64,
    referenced_cidv1: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProcessedLink {
    parent_block_id: i64,
    referenced_cidv1: String,
    referenced_block_id: Option<i64>,
}

fn process_links_initial(
    mut input: csv::Reader<std::fs::File>,
    output: &mut csv::Writer<std::fs::File>,
    blocks: HashMap<Vec<u8>, i64>,
) -> Result<()> {
    let mut link_count = 0_i64;

    for result in input.deserialize() {
        let record: Link = result.context("unable to deserialize link")?;
        let mut link = ProcessedLink {
            parent_block_id: record.parent_block_id,
            referenced_cidv1: record.referenced_cidv1,
            referenced_block_id: None,
        };

        process_link(&mut link, &blocks)?;

        output
            .serialize(link)
            .context("unable to write output record")?;

        link_count += 1;
        if link_count % 1_000_000 == 0 {
            info!("processed {}M links...", link_count / 1_000_000);
        }
    }

    Ok(())
}

fn process_links_pass(
    mut input: csv::Reader<std::fs::File>,
    output: &mut csv::Writer<std::fs::File>,
    blocks: HashMap<Vec<u8>, i64>,
) -> Result<()> {
    let mut link_count = 0_i64;
    for result in input.deserialize() {
        let mut record: ProcessedLink = result.context("unable to deserialize processed link")?;

        process_link(&mut record, &blocks)?;

        output
            .serialize(record)
            .context("unable to write output record")?;

        link_count += 1;
        if link_count % 1_000_000 == 0 {
            info!("processed {}M links...", link_count / 1_000_000);
        }
    }

    Ok(())
}

fn process_link(link: &mut ProcessedLink, blocks: &HashMap<Vec<u8>, i64>) -> Result<()> {
    if link.referenced_block_id.is_some() {
        return Ok(());
    }

    let cid = canonicalize_cid_from_str_to_cidv1(&link.referenced_cidv1).context("invalid CID")?;
    let b = cid.to_bytes();

    link.referenced_block_id = blocks.get(&b).copied();

    Ok(())
}

fn process_links_final(mut input: csv::Reader<std::fs::File>, mut max_id: i64) -> Result<()> {
    let mut blocks = HashMap::new();
    let mut link_count = 0_i64;

    println!("{}", HEADER);
    for result in input.deserialize() {
        let mut record: ProcessedLink = result.context("unable to deserialize processed link")?;

        if let None = record.referenced_block_id {
            let cid = canonicalize_cid_from_str_to_cidv1(&record.referenced_cidv1)
                .context("invalid CID")?;
            let b = cid.to_bytes();

            let target_id = {
                if let Some(id) = blocks.get(&b) {
                    *id
                } else {
                    max_id += 1;
                    blocks.insert(b, max_id);
                    max_id
                }
            };

            record.referenced_block_id = Some(target_id);
        }

        println!(
            "{}\t{}",
            record.parent_block_id,
            record.referenced_block_id.unwrap()
        );

        link_count += 1;
        if link_count % 1_000_000 == 0 {
            info!("processed {}M links...", link_count / 1_000_000);
        }
    }

    Ok(())
}

fn read_blocks_chunk(
    input: &mut csv::Reader<std::fs::File>,
    max_id: &mut i64,
    block_count: &mut i64,
    chunk_size: i64,
) -> Result<HashMap<Vec<u8>, i64>> {
    info!(
        "reading chunk of {} nodes, currently read {}...",
        chunk_size, block_count
    );
    let mut nodes = HashMap::new();

    for result in input.deserialize() {
        let record: Block = result.context("unable to deserialize block")?;

        if record.id > *max_id {
            *max_id = record.id;
        }

        let cid = canonicalize_cid_from_str_to_cidv1(&record.cidv1).context("invalid CID")?;
        let b = cid.to_bytes();

        nodes.insert(b, record.id);

        *block_count += 1;
        if *block_count % chunk_size == 0 {
            break;
        }
        if *block_count % 100_000 == 0 {
            info!("read {}K blocks...", *block_count / 1_000);
        }
    }

    Ok(nodes)
}

fn main() -> Result<()> {
    logging::set_up_logging(false)?;

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        return Err(err_msg("missing arguments"));
    }

    let blocks_infile = args[1].clone();
    let links_infile = args[2].clone();

    let mut blocks_reader =
        csv::Reader::from_path(&blocks_infile).context("unable to open blocks file")?;

    // Read nodes
    info!("processing nodes...");
    let mut num_nodes_read = 0_i64;
    let mut max_id = 0_i64;
    let mut first = true;
    let mut tmp = None;

    loop {
        if blocks_reader.is_done() {
            break;
        }

        let nodes = read_blocks_chunk(
            &mut blocks_reader,
            &mut max_id,
            &mut num_nodes_read,
            20_000_000,
        )
        .context("unable to read nodes")?;

        if first {
            info!("performing initial pass...");
            first = false;
            let mut tmp_file = tempfile::tempfile().context("unable to create tempfile")?;
            let mut tmp_writer = csv::Writer::from_writer(tmp_file);

            let links_reader =
                csv::Reader::from_path(&links_infile).context("unable to open links file")?;
            process_links_initial(links_reader, &mut tmp_writer, nodes)
                .context("unable to process links initially")?;

            tmp_file = tmp_writer
                .into_inner()
                .context("unable to finalize temporary writer")?;
            tmp_file
                .seek(std::io::SeekFrom::Start(0))
                .context("unable to seek tempfile")?;
            tmp = Some(tmp_file);

            continue;
        }

        // This is now our input
        info!("performing pass...");
        let mut tmp_file = tmp.take().unwrap();
        let tmp_input = csv::ReaderBuilder::new().from_reader(tmp_file);

        let mut tmp_writer = csv::Writer::from_writer(
            tempfile::tempfile().context("unable to create temporary file")?,
        );

        process_links_pass(tmp_input, &mut tmp_writer, nodes).context("unable to perform pass")?;

        tmp_file = tmp_writer
            .into_inner()
            .context("unable to finalize temporary writer")?;
        tmp_file
            .seek(std::io::SeekFrom::Start(0))
            .context("unable to seek tempfile")?;
        tmp = Some(tmp_file);
    }
    info!(
        "done reading {} nodes, now doing final pass and output",
        num_nodes_read
    );

    let tmp_file = tmp.take().unwrap();
    let tmp_input = csv::ReaderBuilder::new().from_reader(tmp_file);
    process_links_final(tmp_input, max_id).context("unable to perform final pass")?;

    info!("done reading edges");

    Ok(())
}
