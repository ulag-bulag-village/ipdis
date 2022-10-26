use std::env;

use ipis::{
    async_trait::async_trait,
    core::anyhow::{Ok, Result},
    env::Infer,
};
use ipsis_api_persistent_ipfs::IpsisClient;

pub struct ProtocolImpl {
    client_read: IpsisClient,
    client_write: IpsisClient,
}

impl ProtocolImpl {
    pub async fn try_new() -> Result<Self> {
        // init client - write
        if let Some(host) = env::var_os("ipsis_client_ipfs_host_write") {
            env::set_var("ipsis_client_ipfs_host", host);
        };
        if let Some(port) = env::var_os("ipsis_client_ipfs_port_write") {
            env::set_var("ipsis_client_ipfs_port", port);
        };
        let client_write = IpsisClient::try_infer().await?;

        // init client - read
        if let Some(host) = env::var_os("ipsis_client_ipfs_host_read") {
            env::set_var("ipsis_client_ipfs_host", host);
        };
        if let Some(port) = env::var_os("ipsis_client_ipfs_port_read") {
            env::set_var("ipsis_client_ipfs_port", port);
        };
        let client_read = IpsisClient::try_infer().await?;

        Ok(Self {
            client_read,
            client_write,
        })
    }
}

#[async_trait]
impl super::Protocol for ProtocolImpl {
    async fn to_string(&self) -> Result<String> {
        Ok("ipfs".into())
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
