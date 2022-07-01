#[macro_use]
extern crate log;

use ipfs_resolver_common::{logging, Result};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{BufRead, BufReader};
use std::{io, panic};

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    logging::set_up_logging()?;

    let mut rdr = BufReader::new(io::stdin());
    let mut s = String::new();

    let mut results: HashMap<_, usize> = HashMap::new();

    let h = panic::take_hook();

    // Set panic hook to suppress warnings (bruh.)
    panic::set_hook(Box::new(|_info| {
        // do nothing
    }));
    while let Ok(n) = rdr.read_line(&mut s) {
        if n == 0 {
            // Restore panic hook.
            panic::set_hook(h);
            results
                .into_iter()
                .for_each(|(k, v)| println!("{},{}", k, v));
            return Ok(());
        }

        debug!("working on {}", s.trim());

        let res = match do_single(s.trim()) {
            Err(_) => "invalid".to_string(),
            Ok(m) => format!(
                "{:?}:{:?}:{:?}:{}:{}",
                m.base,
                m.version,
                m.codec,
                if let Some(h) = m.hash {
                    format!("{:?}", h)
                } else {
                    "invalid".to_string()
                },
                m.hash_len
            ),
        };

        let entry = results.entry(res.clone()).or_default();
        *entry += 1;

        s.clear();
    }
    // Restore panic hook.
    panic::set_hook(h);

    // We only get here if reading from Stdin fails...
    rdr.read_line(&mut s)?;

    Ok(())
}

#[derive(Debug, Clone)]
struct Metadata {
    base: multibase::Base,
    version: cid::Version,
    codec: cid::Codec,
    hash: Option<multihash::Code>,
    hash_len: usize,
}

fn do_single(s: &str) -> Result<Metadata> {
    let c = cid::Cid::try_from(s)?;
    if c.version() == cid::Version::V0 {
        return Ok(Metadata {
            base: multibase::Base::Base58Btc,
            version: c.version(),
            codec: c.codec(),
            hash: match std::panic::catch_unwind(|| c.hash().algorithm()) {
                Ok(h) => Some(h),
                Err(_) => None,
            },
            hash_len: c.hash().digest().len(),
        });
    }

    let (b, _) = multibase::decode(s)?;

    Ok(Metadata {
        base: b,
        version: c.version(),
        codec: c.codec(),
        hash: match std::panic::catch_unwind(|| c.hash().algorithm()) {
            Ok(h) => Some(h),
            Err(_) => None,
        },
        hash_len: c.hash().digest().len(),
    })
}
