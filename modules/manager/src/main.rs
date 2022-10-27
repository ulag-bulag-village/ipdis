use ipiis_api::client::IpiisClient;
use ipis::{async_trait::async_trait, core::anyhow::Result, env::Infer, path::Path, tokio};
use ipsis_api::{client::IpsisClient, common::Ipsis};

#[async_trait]
trait IpsisExt {
    async fn send(&self, path: &Path) -> Result<()>;

    async fn sync(&self, path: &Path) -> Result<()>;
}

#[async_trait]
impl<IpsisClient> IpsisExt for IpsisClient
where
    IpsisClient: AsRef<IpiisClient> + Ipsis + Send + Sync,
{
    async fn send(&self, path: &Path) -> Result<()> {
        // get the primary storage
        let primary = self.as_ref();

        // get data from the current storage
        let data = self.get_raw(path).await?;

        // put data to the primary storage
        primary.put_raw(path, data).await?;

        // remove the data from current storage
        self.delete(path).await
    }

    async fn sync(&self, path: &Path) -> Result<()> {
        // get the primary storage
        let primary = self.as_ref();

        // get data from the primary storage
        let data = primary.get_raw(path).await?;

        // put data to the current storage
        self.put_raw(path, data).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize client
    let client = IpsisClient::try_infer().await?;

    todo!()
}
