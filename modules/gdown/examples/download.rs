use ipdis_api::client::IpdisClient;
use ipdis_modules_gdrive::IpdisGdown;
use ipis::{core::anyhow::Result, env::Infer, tokio};

#[tokio::main]
async fn main() -> Result<()> {
    // create a client
    let client = IpdisClient::try_infer()?;

    // download from gdrive
    let id = "1gICu4NshBMQyUNgWsc2kydLBPpasIMNF";
    let path = client.download_from_gdrive(id).await?;

    // verify
    assert_eq!(path.len, 496_300_196);

    // notify
    println!("Hash = {}", path.value.to_string());
    println!("Length = {}", path.len);
    Ok(())
}
