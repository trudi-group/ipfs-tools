#[macro_use]
extern crate log;

use failure::Error;

pub mod logging;
pub mod wantlist;

pub type Result<T> = std::result::Result<T, Error>;
