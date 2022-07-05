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

    group_and_count_cid_by_metadata(&mut io::stdin(), &mut io::stdout())
}

fn group_and_count_cid_by_metadata(
    input: &mut impl io::Read,
    mut output: &mut impl io::Write,
) -> Result<()> {
    let mut rdr = BufReader::new(input);
    let mut buffer = String::new();
    let mut results: HashMap<_, usize> = HashMap::new();

    let default_panic_hook = panic::take_hook();

    // Set panic hook to suppress warnings (bruh.)
    panic::set_hook(Box::new(|_info| {
        // do nothing
    }));
    while let Ok(n) = rdr.read_line(&mut buffer) {
        if n == 0 {
            // Restore panic hook.
            panic::set_hook(default_panic_hook);
            results
                .into_iter()
                .try_for_each(|(k, v)| writeln!(&mut output, "{},{}", k, v))?;
            return Ok(());
        }

        debug!("working on {}", buffer.trim());

        let res = match do_single(buffer.trim()) {
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
    base: multibase::Base,
    version: cid::Version,
    codec: cid::Codec,
    hash: Option<multihash::Code>,
    hash_len: usize,
}

fn do_single(line: &str) -> Result<Metadata> {
    let c = cid::Cid::try_from(line)?;
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

    let (b, _) = cid::multibase::decode(line)?;

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
        assert_eq!(format!("{:?}", m.codec), "Raw")
    }

    #[test]
    fn decode_cid_hashtype() {
        let m = get_example_metadata();
        assert_eq!(format!("{:?}", m.hash.unwrap()), "Sha2_256")
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
        )
        .unwrap();

        assert_eq!(&output, b"Base58Btc:V1:Raw:Sha2_256:32,1\n")
    }

    #[test]
    fn test_full_one_cid_2() {
        let mut output: Vec<u8> = Vec::new();

        group_and_count_cid_by_metadata(
            &mut "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR".as_bytes(),
            &mut output,
        )
        .unwrap();

        assert_eq!(&output, b"Base58Btc:V0:DagProtobuf:Sha2_256:32,1\n")
    }
}
