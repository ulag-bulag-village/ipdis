use ipis::{
    core::{anyhow::Result, value::hash::Hash},
    env::Infer,
    path::Path,
    tokio,
};
use ipsis_api::{client::IpsisClient, common::Ipsis};
use ipsis_modules_web::IpsisWeb;

#[tokio::main]
async fn main() -> Result<()> {
    // create a client
    let client = IpsisClient::try_infer().await?;

    // we know the file's static path
    let url = "https://upload.wikimedia.org/wikipedia/commons/7/7a/Huskiesatrest.jpg";
    let path = Path {
        value: "67JwwcZ5HHMP26GoVMtSh1SVS3u3wbr6GB5snKHPLfGP".parse()?,
        len: 4_854_901,
    };

    // download from web
    let local_path = client.download_web_static_on_local(url, &path).await?;
    assert!(client.contains(&path).await?);
    assert_eq!(local_path.metadata()?.len(), path.len);

    // valify the downloaded local file
    let data = tokio::fs::read(&local_path).await?;
    let data_hash = Hash::with_bytes(&data);
    assert_eq!(data_hash, path.value);

    Ok(())
}
