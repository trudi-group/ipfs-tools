#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;

use failure::Error;

pub mod ipfs;
pub mod logging;
pub mod wantlist;

pub type Result<T> = std::result::Result<T, Error>;
