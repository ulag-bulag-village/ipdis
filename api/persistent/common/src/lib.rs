use ipis::{
    async_trait::async_trait,
    core::{account::AccountRef, anyhow::Result},
    path::Path,
    tokio::io::{AsyncRead, AsyncWrite},
};

#[async_trait]
pub trait IpsisPersistentStorage {
    const PROTOCOL: &'static str;
    const USE_HASH_AS_NATIVE: bool;

    async fn get_raw<W>(&self, account: &AccountRef, path: &Path, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Send + Unpin + 'static;

    async fn put_raw<R>(&self, account: &AccountRef, path: &Path, reader: &mut R) -> Result<Result<(), Path>>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static;

    async fn contains(&self, account: &AccountRef, path: &Path) -> Result<bool>;

    async fn delete(&self, account: &AccountRef, path: &Path) -> Result<()>;
}
