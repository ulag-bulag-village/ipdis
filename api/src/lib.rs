pub extern crate ipsis_common as common;

pub mod client {
    pub use ::ipsis_api_common::{client::IpsisClientInner, config::IpsisClientConfig};

    pub type IpsisClient =
        IpsisClientInner<::ipiis_api::client::IpiisClient, super::IpsisPersistentStorageImpl>;
}

pub mod server;

#[cfg(feature = "ipfs")]
use ipsis_api_persistent_ipfs::IpsisPersistentStorageImpl;
#[cfg(feature = "local")]
use ipsis_api_persistent_local::IpsisPersistentStorageImpl;
#[cfg(feature = "s3")]
use ipsis_api_persistent_s3::IpsisPersistentStorageImpl;
