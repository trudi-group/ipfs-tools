#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;

use failure::{Error, ResultExt};
use std::path::PathBuf;

pub mod logging;
pub mod wantlist;

pub type Result<T> = std::result::Result<T, Error>;

pub fn expand_globs(input_globs: &Vec<String>) -> Result<Vec<PathBuf>> {
    let mut entries = Vec::new();

    for pattern in input_globs {
        let ent: std::result::Result<Vec<PathBuf>, _> =
            glob::glob(pattern).context("invalid glob")?.collect();
        entries.extend(ent.context("unable to traverse path")?.into_iter());
    }

    Ok(entries)
}
