use std::env;

use ipiis_api::{client::IpiisClient, common::Ipiis};
use ipis::{
    async_trait::async_trait,
    core::anyhow::{Ok, Result},
    env::Infer,
};
use ipsis_common::KIND;

use crate::io::ArgsIpiis;

pub struct ProtocolImpl {
    client_read: IpiisClient,
    client_write: IpiisClient,
}

impl ProtocolImpl {
    pub async fn try_new(ipiis: &ArgsIpiis) -> Result<Self> {
        // init client - write
        let client_write = {
            env::set_var("ipiis_router_db", "/tmp/ipiis-rarp-db-writer");
            let client = IpiisClient::try_infer().await?;

            // register the server account as primary
            client
                .set_account_primary(KIND.as_ref(), &ipiis.account)
                .await?;
            client
                .set_address(KIND.as_ref(), &ipiis.account, &ipiis.address_write)
                .await?;

            client
        };

        // init client - read
        let client_read = {
            env::set_var("ipiis_router_db", "/tmp/ipiis-rarp-db-reader");
            let client = IpiisClient::try_infer().await?;

            // register the server account as primary
            client
                .set_account_primary(KIND.as_ref(), &ipiis.account)
                .await?;
            client
                .set_address(KIND.as_ref(), &ipiis.account, &ipiis.address_read)
                .await?;

            client
        };

        Ok(Self {
            client_write,
            client_read,
        })
    }
}

#[async_trait]
impl super::Protocol for ProtocolImpl {
    async fn to_string(&self) -> Result<String> {
        Ok(format!(
            "ipiis_{}_{}_{}_{}",
            ::ipiis_api::common::Ipiis::protocol(&self.client_write)?,
            ::ipsis_common::Ipsis::protocol(&self.client_write).await?,
            ::ipiis_api::common::Ipiis::protocol(&self.client_read)?,
            ::ipsis_common::Ipsis::protocol(&self.client_read).await?,
        ))
    }

    async fn read(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        super::read(&self.client_read, ctx).await
    }

    async fn write(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        super::write(&self.client_write, ctx).await
    }

    async fn cleanup(&self, ctx: super::BenchmarkCtx) -> Result<()> {
        super::cleanup(&self.client_write, ctx).await
    }
}
