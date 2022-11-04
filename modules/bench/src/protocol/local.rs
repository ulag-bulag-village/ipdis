use ipiis_api::client::IpiisClient;
use ipis::{
    async_trait::async_trait,
    core::anyhow::{Ok, Result},
    env::Infer,
};
use ipsis_api_common::client::IpsisClientInner;
use ipsis_api_persistent_local::IpsisPersistentStorageImpl;

pub struct ProtocolImpl {
    client: IpsisClientInner<IpiisClient, IpsisPersistentStorageImpl>,
}

impl ProtocolImpl {
    pub async fn try_new() -> Result<Self> {
        // init client
        let client = IpsisClientInner::try_infer().await?;

        Ok(Self { client })
    }
}

#[async_trait]
impl super::Protocol for ProtocolImpl {
    async fn to_string(&self) -> Result<String> {
        Ok("local".into())
    }

    async fn read(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        super::read(&self.client, ctx).await
    }

    async fn write(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        super::write(&self.client, ctx).await
    }

    async fn cleanup(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        super::cleanup(&self.client, ctx).await
    }
}
