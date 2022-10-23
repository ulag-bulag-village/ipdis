use ipiis_api::{client::IpiisClient, common::Ipiis};
use ipis::{
    async_trait::async_trait,
    core::anyhow::{Ok, Result},
    env::Infer,
};
use ipsis_common::KIND;

use crate::io::ArgsIpiis;

pub struct ProtocolImpl {
    client: IpiisClient,
}

impl ProtocolImpl {
    pub async fn try_new(ipiis: &ArgsIpiis) -> Result<Self> {
        // init client
        let client = IpiisClient::try_infer().await?;

        // register the server account as primary
        client
            .set_account_primary(KIND.as_ref(), &ipiis.account)
            .await?;
        client
            .set_address(KIND.as_ref(), &ipiis.account, &ipiis.address)
            .await?;

        Ok(Self { client })
    }
}

#[async_trait]
impl super::Protocol for ProtocolImpl {
    async fn to_string(&self) -> Result<String> {
        Ok(format!(
            "ipiis_{}_{}",
            ::ipiis_api::common::Ipiis::protocol(&self.client)?,
            ::ipsis_common::Ipsis::protocol(&self.client).await?,
        ))
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
