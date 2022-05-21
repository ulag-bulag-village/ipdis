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
    let url = "https://huggingface.co/cross-encoder/nli-distilroberta-base/raw/main/vocab.json";
    let path = Path {
        value: "TBNdeMd2zDstNeqDheuzvkKBDdsPxwV8uZrCfeg1mDt".parse()?,
        len: 898_822,
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
