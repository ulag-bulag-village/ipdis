use std::path::PathBuf;

use ipis::{
    async_trait::async_trait,
    core::{
        account::AccountRef,
        anyhow::{Error, Result},
    },
    env::{infer, Infer},
    path::Path,
    tokio::{
        self,
        io::{AsyncRead, AsyncWrite},
    },
};
use ipsis_api_persistent_common::IpsisPersistentStorage;

pub struct IpsisPersistentStorageImpl {
    dir: PathBuf,
}

#[async_trait]
impl<'a> Infer<'a> for IpsisPersistentStorageImpl {
    type GenesisArgs = PathBuf;
    type GenesisResult = Self;

    async fn try_infer() -> Result<Self> {
        Ok(Self {
            dir: infer("ipsis_client_local_dir").or_else(|e| {
                let mut dir = ::dirs::home_dir().ok_or(e)?;
                dir.push(".ipsis");
                Result::<_, Error>::Ok(dir)
            })?,
        })
    }

    async fn genesis(
        dir: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Ok(Self { dir })
    }
}

impl IpsisPersistentStorageImpl {
    pub fn to_path_canonical(&self, account: &AccountRef, path: &Path) -> PathBuf {
        let mut buf = self.dir.clone();
        buf.push(account.to_string());
        buf.push(path.value.to_string());
        buf
    }
}

#[async_trait]
impl IpsisPersistentStorage for IpsisPersistentStorageImpl {
    const PROTOCOL: &'static str = "local";
    const USE_HASH_AS_NATIVE: bool = false;

    async fn get_raw<W>(&self, account: &AccountRef, path: &Path, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(account, &path);

        // external call
        let mut file = tokio::fs::File::open(path_canonical).await?;
        tokio::io::copy(&mut file, writer)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn put_raw<R>(
        &self,
        account: &AccountRef,
        path: &Path,
        reader: &mut R,
    ) -> Result<Result<(), Path>>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(account, &path);

        // create a directory
        tokio::fs::create_dir_all(path_canonical.ancestors().nth(1).unwrap()).await?;

        // external call
        let mut file = tokio::fs::File::create(path_canonical).await?;
        tokio::io::copy(reader, &mut file)
            .await
            .map(|_| Ok(()))
            .map_err(Into::into)
    }

    async fn contains(&self, account: &AccountRef, path: &Path) -> Result<bool> {
        // get canonical path
        let path = self.to_path_canonical(account, path);

        // external call
        Ok(tokio::fs::metadata(path).await.is_ok())
    }

    async fn delete(&self, account: &AccountRef, path: &Path) -> Result<()> {
        // get canonical path
        let path = self.to_path_canonical(account, path);

        // external call
        tokio::fs::remove_file(path).await.map_err(Into::into)
    }
}
