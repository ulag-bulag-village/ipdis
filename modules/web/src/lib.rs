use std::io::Cursor;

use ipis::{
    async_trait::async_trait,
    core::{
        anyhow::{bail, Result},
        value::hash::Hash,
    },
    path::Path,
};
use ipsis_common::Ipsis;
use ipsis_modules_local::IpsisLocal;
use reqwest::{header::USER_AGENT, Client};

#[async_trait]
pub trait IpsisWeb: Ipsis {
    async fn download_web(&self, url: &str) -> Result<Path> {
        // create a session
        let session = Client::builder().build()?;

        // try to acquire data
        let response = session
            .get(url)
            .header(
                USER_AGENT,
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0",
            )
            .send()
            .await?;

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

    async fn download_web_static(&self, url: &str, path: &Path) -> Result<()> {
        if self.contains(path).await? {
            Ok(())
        } else {
            let downloaded = self.download_web(url).await?;

            if &downloaded == path {
                Ok(())
            } else {
                let expected = path.value.to_string();
                let downloaded = downloaded.value.to_string();
                bail!("download path mismatched: expected {expected}, but given {downloaded}")
            }
        }
    }

    async fn download_web_static_on_local(
        &self,
        url: &str,
        path: &Path,
    ) -> Result<::std::path::PathBuf> {
        // download a file
        self.download_web_static(url, path).await?;

        // resolve the local file path
        let filename = url
            .split('.')
            .last()
            .filter(|ext| ext.len() <= 16)
            .map(|ext| {
                let hash = path.value.to_string();
                format!("{hash}.{ext}")
            });
        self.download_on_local(path, filename).await
    }
}

impl<T: Ipsis + ?Sized> IpsisWeb for T {}
