use ipdis_api::client::IpdisClient;
use ipdis_modules_gdown::IpdisGdown;
use ipis::{core::anyhow::Result, env::Infer, path::Path, tokio};

#[tokio::main]
async fn main() -> Result<()> {
    // create a client
    let client = IpdisClient::try_infer()?;

    // we know the file's static path
    let id = "1gICu4NshBMQyUNgWsc2kydLBPpasIMNF";
    let path = Path {
        value: "FjL3dTmyrudvLxFcezJ7b3oGq7Q48ZUS8HH5e4wajVL7".parse()?,
        len: 496_300_196,
    };

    // download from gdrive
    client.gdown_static(id, &path).await?;
    Ok(())
}
