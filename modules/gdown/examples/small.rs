use ipis::{core::anyhow::Result, env::Infer, path::Path, tokio};
use ipsis_api::{client::IpsisClient, common::Ipsis};
use ipsis_modules_gdown::IpsisGdown;

#[tokio::main]
async fn main() -> Result<()> {
    // create a client
    let client = IpsisClient::try_infer().await?;

    // we know the file's static path
    let id = "1l-OwuECuYRgSk3JIOz0xgFqiqomKi1ct";
    let path = Path {
        value: "bafkreihm64a7oj6z4llxysvetldpxpgjs4tyvsqbbpo65olbyegpktkdli".parse()?,
        len: 13,
    };

    // download from gdrive
    client.gdown_static(id, &path).await?;
    assert!(client.contains(&path).await?);
    Ok(())
}
