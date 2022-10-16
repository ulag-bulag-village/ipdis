use std::{ops::Range, sync::Arc, time::Duration};

use ipis::{
    async_trait::async_trait,
    core::anyhow::Result,
    path::Path,
    tokio::{self, io::AsyncReadExt},
};
use ipsis_common::Ipsis;

#[cfg(feature = "ipfs")]
mod ipfs;
#[cfg(feature = "ipiis")]
mod ipiis;
#[cfg(feature = "local")]
mod local;
#[cfg(feature = "s3")]
mod s3;

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

pub(super) async fn read<T>(client: &T, ctx: self::BenchmarkCtx) -> Result<()>
where
    T: Ipsis,
{
    for (path, _) in ctx
        .dataset
        .iter()
        .skip(ctx.offset as usize)
        .step_by(ctx.num_threads)
    {
        // compose simulation environment
        if let Some(delay) = ctx.simulation.delay_ms.map(Duration::from_millis) {
            tokio::time::sleep(delay).await;
        }

        let mut recv = client.get_raw(path).await?;

        let len = recv.read_u64().await?;
        assert_eq!(len as usize, ctx.size_bytes);

        tokio::io::copy(&mut recv, &mut tokio::io::sink()).await?;
    }
    Ok(())
}

pub(super) async fn write<T>(client: &T, ctx: self::BenchmarkCtx) -> Result<()>
where
    T: Ipsis,
{
    for (path, range) in ctx
        .dataset
        .iter()
        .skip(ctx.offset as usize)
        .step_by(ctx.num_threads)
    {
        // compose simulation environment
        if let Some(delay) = ctx.simulation.delay_ms.map(Duration::from_millis) {
            tokio::time::sleep(delay).await;
        }

        let data = unsafe {
            ::core::slice::from_raw_parts(ctx.data.as_ptr().add(range.start), ctx.size_bytes)
        };
        client.put_raw(path, data).await?;
    }
    Ok(())
}

pub(super) async fn cleanup<T>(client: &T, ctx: self::BenchmarkCtx) -> Result<()>
where
    T: Ipsis,
{
    for (path, _) in ctx
        .dataset
        .iter()
        .skip(ctx.offset as usize)
        .step_by(ctx.num_threads)
    {
        // compose simulation environment
        if let Some(delay) = ctx.simulation.delay_ms.map(Duration::from_millis) {
            tokio::time::sleep(delay).await;
        }

        client.delete(path).await?;
    }
    Ok(())
}

pub struct BenchmarkCtx {
    pub num_threads: usize,
    pub size_bytes: usize,
    pub simulation: crate::io::ArgsSimulation,

    pub offset: u32,
    pub dataset: Arc<[(Path, Range<usize>)]>,
    pub data: Arc<[u8]>,
}
