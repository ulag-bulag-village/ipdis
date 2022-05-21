use std::io::Cursor;

use ipis::{
    async_trait::async_trait,
    core::{
        anyhow::{bail, Result},
        value::hash::Hash,
    },
    path::Path,
    tokio::{self, io::AsyncReadExt},
};
use ipsis_common::Ipsis;
use reqwest::Client;

#[async_trait]
pub trait IpsisWeb: Ipsis {
    async fn download_web(&self, url: &str) -> Result<Path> {
        // create a session
        let session = Client::builder().build()?;

        // try to acquire data
        let response = session.get(url).send().await?;

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
        let () = self.download_web_static(url, path).await?;

        // resolve the local file path
        let local_path = ::std::env::temp_dir().join(path.value.to_string());
        if local_path.exists() && local_path.metadata()?.len() == path.len {
            return Ok(local_path);
        }

        // get data
        let mut file = tokio::fs::File::create(&local_path).await?;
        let mut recv = self.get_raw(path).await?;

        let len = recv.read_u64().await?;
        if len != path.len {
            bail!("failed to validate the length");
        }

        tokio::io::copy(&mut recv, &mut file).await?;
        Ok(local_path)
    }
}

impl<T: Ipsis> IpsisWeb for T {}
