#[macro_use]
extern crate log;

use failure::ResultExt;
use ipfs_resolver_common::{logging, Result};
use ipfs_resolver_db::db;
use std::io;
use std::io::Write;

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    logging::set_up_logging(false)?;

    info!("creating connection pool...");
    let pool = ipfs_resolver_db::create_pool()?;
    let conn = pool.get()?;

    /*
    info!("working on blocks...");
    let mut i = 0;
    let batch_size = 100;
    print!("{}", i);
    loop {
        let mut blocks = db::get_blocks_without_cidv1(&conn, batch_size, 0)?;
        if blocks.len() == 0 {
            info!("got 0 blocks, quitting.");
            break;
        }
        debug!("got blocks: {:?}", blocks);

        for b in blocks.iter_mut() {
            debug!("working on block {:?}", b);

            let c = ipfs_resolver_db::canonicalize_cid_from_str_to_cidv1(&b.base32_cidv1).context("unable to canonicalize CID")?;
            debug!("canonicalized CID to {:?} with bytes {:?}",c,c.to_bytes());

            b.cidv1 = Some(c.to_bytes())
        }

        debug!("writing back..");
        db::update_blocks_with_cidv1(&conn, blocks).context("unable to update blocks")?;

        i += batch_size as i32;
        print!("\r{}", i);
        io::stdout().flush().expect("unable to flush stdout");
    }
     */

    /*
    info!("working on unixfs_links...");
    let mut i = 0;
    let batch_size = 1000;
    print!("{}", i);
    loop {
        let mut links = db::get_unixfs_links_without_cidv1(&conn, batch_size)?;
        if links.len() == 0 {
            info!("got 0 links, quitting...");
            break;
        }
        debug!("got links: {:?}", links);

        for l in links.iter_mut() {
            debug!("working on link {:?}", l);

            let c =
                ipfs_resolver_db::canonicalize_cid_from_str_to_cidv1(&l.referenced_base32_cidv1)
                    .context("unable to canonicalize CID")?;
            debug!("canonicalized CID to {:?} with bytes {:?}", c, c.to_bytes());

            l.referenced_cidv1 = Some(c.to_bytes())
        }

        debug!("writing back...");
        db::update_unixfs_links_with_cidv1(&conn, links)
            .context("unable to update UnixFS links")?;

        i += batch_size as i32;
        print!("\r{}", i);
        io::stdout().flush().expect("unable to flush stdout");
    }
     */

    Ok(())
}
