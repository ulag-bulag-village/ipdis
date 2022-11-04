use async_compat::{Compat, CompatExt};
use http::uri::Scheme;
use ipfs_api::{IpfsApi, IpfsClient, TryFromUri};
use ipis::{
    async_trait::async_trait,
    core::{account::AccountRef, anyhow::Result},
    env::{infer, Infer},
    futures::TryStreamExt,
    path::Path,
    tokio::{
        self,
        io::{AsyncRead, AsyncReadExt, AsyncWrite},
    },
};
use ipsis_api_persistent_common::IpsisPersistentStorage;

pub struct IpsisPersistentStorageImpl {
    ipfs: IpfsClient,
}

#[async_trait]
impl<'a> Infer<'a> for IpsisPersistentStorageImpl
where
    Self: Send,
{
    type GenesisArgs = ();
    type GenesisResult = Self;

    async fn try_infer() -> Result<Self> {
        Self::try_new()
    }

    async fn genesis(
        (): <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Self::try_new()
    }
}

impl IpsisPersistentStorageImpl {
    pub fn try_new() -> Result<Self> {
        let host: String = infer("ipsis_client_ipfs_host").unwrap_or_else(|_| "localhost".into());
        let port = infer("ipsis_client_ipfs_port").unwrap_or(5001);

        Ok(Self {
            ipfs: IpfsClient::from_host_and_port(Scheme::HTTP, &host, port)?,
        })
    }

    pub fn ipfs(&self) -> &IpfsClient {
        &self.ipfs
    }
}

#[async_trait]
impl IpsisPersistentStorage for IpsisPersistentStorageImpl {
    const PROTOCOL: &'static str = "ipfs";
    const USE_HASH_AS_NATIVE: bool = true;

    async fn get_raw<W>(&self, _account: &AccountRef, path: &Path, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        // TODO: verify account

        // get canonical path
        let path = *path;

        // external call
        let mut stream = self
            .ipfs
            .get(&path.value.to_string())
            .map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::Other, e))
            .into_async_read();
        let mut stream = stream.compat_mut();

        // drop header packets
        {
            let mut buf = vec![0u8; 512];
            stream.read_exact(&mut buf).await?;
        }

        // execute data transfer
        tokio::io::copy(&mut stream, writer)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn put_raw<R>(
        &self,
        _account: &AccountRef,
        path: &Path,
        reader: &mut R,
    ) -> Result<Result<(), Path>>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // TODO: verify account

        // get canonical path
        let path = *path;

        // SAFETY: the reader should be **comsumed** in the external API call
        let mut reader: Compat<&mut R> = unsafe { ::core::mem::transmute(reader.compat_mut()) };
        let reader: &mut Compat<&mut R> = unsafe { ::core::mem::transmute(&mut reader) };

        // IPFS PUT options
        let options = ipfs_api::request::Add::builder()
            .cid_version(1)
            .pin(true)
            .build();

        // external call
        let response = self.ipfs.add_async_with_options(reader, options).await?;

        // poll hash
        let path_from_data = Path {
            value: response.hash.parse()?,
            len: path.len,
        };

        // validate hash
        if path == path_from_data {
            Ok(Ok(()))
        } else {
            Ok(Err(path_from_data))
        }
    }

    async fn contains(&self, _account: &AccountRef, path: &Path) -> Result<bool> {
        // TODO: verify account

        // get canonical path
        let path = *path;

        // external call
        let result = self.ipfs.pin_ls(Some(&path.value.to_string()), None).await;

        // pack data
        Ok(result.is_ok())
    }

    async fn delete(&self, _account: &AccountRef, path: &Path) -> Result<()> {
        // TODO: verify account

        // get canonical path
        let path = *path;

        // external call
        self.ipfs.pin_rm(&path.value.to_string(), true).await?;

        // pack data
        Ok(())
    }
}
