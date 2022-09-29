pub extern crate ipsis_common as common;

pub mod server;

#[cfg(feature = "ipfs")]
pub use ipsis_api_persistent_ipfs as client;
#[cfg(feature = "local")]
pub use ipsis_api_persistent_local as client;
#[cfg(feature = "s3")]
pub use ipsis_api_persistent_s3 as client;
