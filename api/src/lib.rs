pub extern crate ipsis_common as common;

pub mod server;

#[cfg(feature = "s3")]
pub use ipsis_api_persistent_s3 as client;
#[cfg(feature = "temp")]
pub use ipsis_api_persistent_temp as client;
