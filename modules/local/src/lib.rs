use ipis::{
    async_trait::async_trait,
    core::anyhow::{bail, Result},
    path::Path,
    tokio::{self, io::AsyncReadExt},
};
use ipsis_common::Ipsis;
use tokio_tar::Archive;

struct Context<T>
where
    T: Ipsis + ?Sized,
{
    local_path: ::std::path::PathBuf,
    recv: Option<<T as Ipsis>::Reader>,
}

impl<T> Context<T>
where
    T: Ipsis + ?Sized,
{
    async fn load(ipsis: &T, path: &Path, name: Option<String>) -> Result<Self> {
        // resolve the local file path
        let name = name.unwrap_or_else(|| path.value.to_string());
        let local_path = ::std::env::temp_dir().join(name);
        if local_path.exists() && local_path.metadata()?.len() == path.len {
            return Ok(Self {
                local_path,
                recv: None,
            });
        }

        // get data
        let mut recv = ipsis.get_raw(path).await?;

        let len = recv.read_u64().await?;
        if len != path.len {
            bail!("failed to validate the length");
        }

        Ok(Self {
            local_path,
            recv: Some(recv),
        })
    }
}

#[async_trait]
pub trait IpsisLocal: Ipsis {
    async fn download_on_local(
        &self,
        path: &Path,
        filename: Option<String>,
    ) -> Result<::std::path::PathBuf> {
        // get context
        let ctx = Context::load(self, path, filename).await?;
        let mut recv = match ctx.recv {
            Some(recv) => recv,
            None => return Ok(ctx.local_path),
        };

        // get data
        let mut file = tokio::fs::File::create(&ctx.local_path).await?;

        // copy to local file
        tokio::io::copy(&mut recv, &mut file).await?;
        Ok(ctx.local_path)
    }

    async fn download_on_local_tar(
        &self,
        path: &Path,
        dirname: Option<String>,
    ) -> Result<::std::path::PathBuf>
    where
        <Self as Ipsis>::Reader: Sync,
    {
        // get context
        let ctx = Context::load(self, path, dirname).await?;
        let recv = match ctx.recv {
            Some(recv) => recv,
            None => return Ok(ctx.local_path),
        };

        // get data
        let mut ar = Archive::new(recv);

        // unpack to target directory
        ar.unpack(&ctx.local_path).await?;
        Ok(ctx.local_path)
    }
}

impl<T: Ipsis + ?Sized> IpsisLocal for T {}
