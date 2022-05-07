pub extern crate ipdis_common as common;

pub mod server;

#[cfg(feature = "s3")]
pub use ipdis_api_persistent_s3 as client;
