use ipis::{core::anyhow::Result, env::Infer, path::Path, tokio};
use ipsis_api::{client::IpsisClient, common::Ipsis};
use ipsis_modules_gdown::IpsisGdown;

#[tokio::main]
async fn main() -> Result<()> {
    // create a client
    let client = IpsisClient::try_infer().await?;

    // we know the file's static path
    let id = "1gICu4NshBMQyUNgWsc2kydLBPpasIMNF";
    let path = Path {
        value: "bafybeie52ly6uafpr4h3ih24mqa4twtojppo6366kyi74ejtd4sxv2fezm".parse()?,
        len: 496_300_196,
    };

    // download from gdrive
    client.gdown_static(id, &path).await?;
    assert!(client.contains(&path).await?);
    Ok(())
}
