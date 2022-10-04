use std::{ops::Range, sync::Arc};

use ipis::{async_trait::async_trait, core::anyhow::Result, path::Path};

#[cfg(feature = "ipfs")]
pub mod ipfs;
#[cfg(feature = "ipiis")]
pub mod ipiis;
#[cfg(feature = "local")]
pub mod local;
#[cfg(feature = "s3")]
pub mod s3;

#[async_trait]
pub trait Protocol {
    async fn to_string(&self) -> Result<String>;

    async fn read(&self, ctx: self::BenchmarkCtx) -> Result<()>;

    async fn write(&self, ctx: self::BenchmarkCtx) -> Result<()>;

    async fn cleanup(&self, ctx: self::BenchmarkCtx) -> Result<()>;
}

pub async fn select(args: &crate::io::Args) -> Result<Box<dyn Protocol>> {
    match args.inputs.protocol {
        #[cfg(feature = "ipiis")]
        crate::io::ArgsProtocol::Ipiis => self::ipiis::ProtocolImpl::try_new(&args.ipiis)
            .await
            .map(|protocol| Box::new(protocol) as Box<dyn Protocol>),
        #[cfg(feature = "ipfs")]
        crate::io::ArgsProtocol::Ipfs => self::ipfs::ProtocolImpl::try_new()
            .await
            .map(|protocol| Box::new(protocol) as Box<dyn Protocol>),
        #[cfg(feature = "local")]
        crate::io::ArgsProtocol::Local => self::local::ProtocolImpl::try_new()
            .await
            .map(|protocol| Box::new(protocol) as Box<dyn Protocol>),
        #[cfg(feature = "s3")]
        crate::io::ArgsProtocol::S3 => self::s3::ProtocolImpl::try_new()
            .await
            .map(|protocol| Box::new(protocol) as Box<dyn Protocol>),
    }
}

pub struct BenchmarkCtx {
    pub num_threads: usize,
    pub size_bytes: usize,

    pub offset: u32,
    pub dataset: Arc<[(Path, Range<usize>)]>,
    pub data: Arc<[u8]>,
}
