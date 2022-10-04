use ipis::{
    async_trait::async_trait,
    core::anyhow::{Ok, Result},
    env::Infer,
    tokio,
};
use ipsis_common::Ipsis;
use tokio::io::AsyncReadExt;

pub struct ProtocolImpl {
    client: ::ipsis_api_persistent_local::IpsisClient,
}

impl ProtocolImpl {
    pub async fn try_new() -> Result<Self> {
        // init client
        let client = ::ipsis_api_persistent_local::IpsisClient::try_infer().await?;

        Ok(Self { client })
    }
}

#[async_trait]
impl super::Protocol for ProtocolImpl {
    async fn to_string(&self) -> Result<String> {
        Ok("ipfs".into())
    }

    async fn read(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        for (path, _) in ctx
            .dataset
            .iter()
            .skip(ctx.offset as usize)
            .step_by(ctx.num_threads)
        {
            let mut recv = self.client.get_raw(path).await?;

            let len = recv.read_u64().await?;
            assert_eq!(len as usize, ctx.size_bytes);

            tokio::io::copy(&mut recv, &mut tokio::io::sink()).await?;
        }
        Ok(())
    }

    async fn write(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        for (path, range) in ctx
            .dataset
            .iter()
            .skip(ctx.offset as usize)
            .step_by(ctx.num_threads)
        {
            let data = unsafe {
                ::core::slice::from_raw_parts(ctx.data.as_ptr().add(range.start), ctx.size_bytes)
            };
            self.client.put_raw(path, data).await?;
        }
        Ok(())
    }

    async fn cleanup(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        for (path, _) in ctx
            .dataset
            .iter()
            .skip(ctx.offset as usize)
            .step_by(ctx.num_threads)
        {
            self.client.delete(path).await?;
        }
        Ok(())
    }
}
