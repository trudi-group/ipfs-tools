use failure::Error;

pub mod logging;

pub type Result<T> = std::result::Result<T, Error>;
