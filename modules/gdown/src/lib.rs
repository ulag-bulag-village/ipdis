use ipdis_common::Ipdis;
use ipis::{
    async_trait::async_trait,
    core::anyhow::{anyhow, Result},
    path::Path,
};
use reqwest::Client;

#[async_trait]
pub trait IpdisGdown: Ipdis {
    async fn download_from_gdrive(&self, id: &str) -> Result<Path> {
        const URL: &str = "https://docs.google.com/uc?export=download";

        // create a session
        let session = Client::builder().cookie_store(true).build()?;

        // try to acquire data
        let mut response = session.get(URL).query(&[("id", id)]).send().await?;

        // size > 100MiB?
        if response
            .headers()
            .get("content-type")
            .and_then(|e| e.to_str().ok())
            .map(|e| e.contains("text/html"))
            .unwrap_or_default()
        {
            let token = response.text().await?;
            let token = token
                .split("confirm=")
                .nth(1)
                .and_then(|e| e.split('\"').next())
                .ok_or_else(|| anyhow!("failed to get gdrive token: {id}"))?;

            // acquire data
            response = session
                .get(URL)
                .query(&[("id", id), ("confirm", token)])
                .send()
                .await?;
        }

        let data = response.bytes().await?.to_vec();
        self.put_raw(data, None).await
    }
}

impl<T: Ipdis> IpdisGdown for T {}
