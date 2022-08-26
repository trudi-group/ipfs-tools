#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

mod multicodecs;

use failure::err_msg;
use ipfs_resolver_common::{logging, Result};
use multicodecs::Tag;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{BufRead, BufReader};
use std::{io, panic};

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    logging::set_up_logging()?;

    group_and_count_cid_by_metadata(&mut io::stdin(), &mut io::stdout(), &mut io::stderr())
}

fn group_and_count_cid_by_metadata(
    input: &mut impl io::Read,
    mut output: &mut impl io::Write,
    mut error_output: &mut impl io::Write,
) -> Result<()> {
    let mut rdr = BufReader::new(input);
    let mut buffer = String::new();
    let mut results: HashMap<_, usize> = HashMap::new();

    let default_panic_hook = panic::take_hook();

    // Set panic hook to suppress warnings (bruh.)
    panic::set_hook(Box::new(|_info| {
        // do nothing
    }));
    let mut line_no = 0;
    while let Ok(n) = rdr.read_line(&mut buffer) {
        if n == 0 {
            //EOF
            // Restore panic hook.
            panic::set_hook(default_panic_hook);
            results
                .into_iter()
                .try_for_each(|(k, v)| writeln!(&mut output, "{},{}", k, v))?;
            return Ok(());
        }
        line_no += 1;
        debug!("working on row {}: ({})", line_no, buffer.trim());

        let res = match do_single(buffer.trim()) {
            Ok(m) => format!(
                "{:?}:{:?}:{}:{}:{}",
                m.base, m.version, m.codec, m.hash, m.hash_len
            ),
            Err(err) => {
                // If we get an error while parsing the input we log a warning to stderr and ignore the corresponding line
                writeln!(
                    &mut error_output,
                    "Ignoring line {}: {:?} (Line was {})",
                    line_no,
                    err,
                    buffer.trim()
                )?;
                buffer.clear();
                continue;
            }
        };

        let entry = results.entry(res.clone()).or_default();
        *entry += 1;

        buffer.clear();
    }
    // Restore panic hook.
    panic::set_hook(default_panic_hook);

    // We only get here if reading from Stdin fails...
    rdr.read_line(&mut buffer)?;

    Ok(())
}

#[derive(Debug, Clone)]
struct Metadata {
    base: cid::multibase::Base,
    version: cid::Version,
    codec: &'static str,
    hash: &'static str,
    hash_len: usize,
}

fn do_single(line: &str) -> Result<Metadata> {
    let c = cid::Cid::try_from(line)?;

    //TODO: If we can remove the panic catch this can go inside the
    //returnblock and the match matchstatement can be deleted.
    let hash = match std::panic::catch_unwind(|| c.hash().code()) {
        //code can give any u64
        Ok(h) => multicodecs::MULTICODEC_TABLE
            .get_name_with_checked_tag(h, Tag::Multihash)
            .ok_or(err_msg("unknown multicodec"))??,
        Err(_) => return Err(err_msg("Could not parse hash")),
    };

    return Ok(Metadata {
        base: if c.version() == cid::Version::V0 {
            cid::multibase::Base::Base58Btc
        } else {
            cid::multibase::decode(line)?.0
        },
        version: c.version(),
        codec: multicodecs::MULTICODEC_TABLE
            .get_name_with_checked_tag(c.codec(), Tag::Ipld)
            .ok_or(err_msg("unknown multicodec"))??,
        hash,
        hash_len: c.hash().digest().len(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_example_metadata() -> Metadata {
        let example_cid = "zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA";
        do_single(example_cid).unwrap()
    }

    #[test]
    fn decode_cid_base() {
        let m = get_example_metadata();
        assert_eq!(format!("{:?}", m.base), "Base58Btc")
    }

    #[test]
    fn decode_cid_version() {
        let m = get_example_metadata();
        assert_eq!(format!("{:?}", m.version), "V1")
    }

    #[test]
    fn decode_cid_codec() {
        let m = get_example_metadata();
        assert_eq!(m.codec, "raw")
    }

    #[test]
    fn decode_cid_hashtype() {
        let m = get_example_metadata();
        assert_eq!(m.hash, "sha2-256")
    }

    #[test]
    fn decode_cid_hash_len() {
        let m = get_example_metadata();
        assert_eq!(format!("{}", m.hash_len), "32")
    }

    #[test]
    fn test_count_1_one_cid() {
        let mut output: Vec<u8> = Vec::new();

        group_and_count_cid_by_metadata(
            &mut "zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA\n".as_bytes(),
            &mut output,
            &mut io::stderr(),
        )
        .unwrap();

        let output_counted = output[output.len() - 2];

        assert_eq!(output_counted, b'1');
    }

    #[test]
    fn test_count_2_one_cid() {
        let mut output: Vec<u8> = Vec::new();

        group_and_count_cid_by_metadata(
            &mut "zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA\nzb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA".as_bytes(),
            &mut output,
            &mut io::stderr()
        )
        .unwrap();

        let output_counted = output[output.len() - 2];

        assert_eq!(output_counted, b'2');
    }

    #[test]
    fn test_full_one_cid() {
        let mut output: Vec<u8> = Vec::new();

        group_and_count_cid_by_metadata(
            &mut "zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA\n".as_bytes(),
            &mut output,
            &mut io::stderr(),
        )
        .unwrap();

        assert_eq!(&output, b"Base58Btc:V1:raw:sha2-256:32,1\n")
    }

    #[test]
    fn test_full_one_cid_2() {
        let mut output: Vec<u8> = Vec::new();

        group_and_count_cid_by_metadata(
            &mut "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR".as_bytes(),
            &mut output,
            &mut io::stderr(),
        )
        .unwrap();

        assert_eq!(&output, b"Base58Btc:V0:dag-pb:sha2-256:32,1\n")
    }
}
