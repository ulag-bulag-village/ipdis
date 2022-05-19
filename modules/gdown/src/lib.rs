use std::io::Cursor;

use ipis::{
    async_trait::async_trait,
    core::{
        anyhow::{anyhow, bail, Result},
        value::hash::Hash,
    },
    path::Path,
};
use ipsis_common::Ipsis;
use reqwest::Client;

#[async_trait]
pub trait IpsisGdown: Ipsis {
    async fn gdown(&self, id: &str) -> Result<Path> {
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

        // digest a hash
        let data = response.bytes().await?;
        let path = Path {
            value: Hash::with_bytes(&data),
            len: data.len().try_into()?,
        };

        // store data
        self.put_raw(&path, Cursor::new(data)).await?;
        Ok(path)
    }

    async fn gdown_static(&self, id: &str, path: &Path) -> Result<()> {
        if self.contains(path).await? {
            Ok(())
        } else {
            let downloaded = self.gdown(id).await?;

            if &downloaded == path {
                Ok(())
            } else {
                let expected = path.value.to_string();
                let downloaded = downloaded.value.to_string();
                bail!("gdown path mismatched: expected {expected}, but given {downloaded}")
            }
        }
    }
}

impl<T: Ipsis> IpsisGdown for T {}
