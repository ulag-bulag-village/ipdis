use ipdis_common::Ipdis;
use ipis::{
    async_trait::async_trait,
    core::anyhow::{anyhow, Result},
    path::Path,
};
use reqwest::Client;

#[async_trait]
pub trait IpdisGdown: Ipdis {
    /// NOTE: referred from: https://stackoverflow.com/a/39225039
    async fn download_from_gdrive(&self, id: &str) -> Result<Path> {
        const URL: &str = "https://docs.google.com/uc?export=download";

        // create a session
        let session = Client::builder().cookie_store(true).build()?;

        // try to acquire data
        let mut response = session.get(URL).query(&[("id", id)]).send().await?;

        // size > 100MiB?
        if response.cookies().next().is_some() {
            let token = response
                .cookies()
                .find(|cookie| cookie.name() == "download_warning")
                .ok_or_else(|| anyhow!("failed to get gdrive token: {id}"))?;

            // acquire data
            response = session
                .get(URL)
                .query(&[("id", id), ("confirm", token.value())])
                .send()
                .await?;
        }

        let data = response.bytes().await?.to_vec();
        self.put_raw(data, None).await
    }
}

impl<T: Ipdis> IpdisGdown for T {}
