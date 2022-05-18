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
        value: "FjL3dTmyrudvLxFcezJ7b3oGq7Q48ZUS8HH5e4wajVL7".parse()?,
        len: 496_300_196,
    };

    // download from gdrive
    client.gdown_static(id, &path).await?;
    assert!(client.contains(&path).await?);
    Ok(())
}
