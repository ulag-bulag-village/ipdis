use std::{path::PathBuf, sync::Arc};

use ipiis_api::common::Ipiis;
use ipis::{
    async_trait::async_trait,
    core::{
        account::AccountRef,
        anyhow::{bail, Result},
        value::hash::Hasher,
    },
    env::{infer, Infer},
    path::Path,
    tokio::{
        self,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    },
};
use ipsis_common::Ipsis;

pub type IpsisClient = IpsisClientInner<::ipiis_api::client::IpiisClient>;

pub struct IpsisClientInner<IpiisClient> {
    pub ipiis: IpiisClient,
    dir: Arc<PathBuf>,
}

impl<IpiisClient> AsRef<::ipiis_api::client::IpiisClient> for IpsisClientInner<IpiisClient>
where
    IpiisClient: AsRef<::ipiis_api::client::IpiisClient>,
{
    fn as_ref(&self) -> &::ipiis_api::client::IpiisClient {
        self.ipiis.as_ref()
    }
}

impl<IpiisClient> AsRef<::ipiis_api::server::IpiisServer> for IpsisClientInner<IpiisClient>
where
    IpiisClient: AsRef<::ipiis_api::server::IpiisServer>,
{
    fn as_ref(&self) -> &::ipiis_api::server::IpiisServer {
        self.ipiis.as_ref()
    }
}

#[async_trait]
impl<'a, IpiisClient> Infer<'a> for IpsisClientInner<IpiisClient>
where
    Self: Send,
    IpiisClient: Infer<'a, GenesisResult = IpiisClient>,
    <IpiisClient as Infer<'a>>::GenesisArgs: Sized,
{
    type GenesisArgs = <IpiisClient as Infer<'a>>::GenesisArgs;
    type GenesisResult = Self;

    async fn try_infer() -> Result<Self> {
        IpiisClient::try_infer()
            .await
            .and_then(Self::with_ipiis_client)
    }

    async fn genesis(
        args: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        IpiisClient::genesis(args)
            .await
            .and_then(Self::with_ipiis_client)
    }
}

impl<IpiisClient> IpsisClientInner<IpiisClient> {
    pub fn with_ipiis_client(ipiis: IpiisClient) -> Result<Self> {
        Ok(Self {
            ipiis,
            dir: Self::new_dir()?,
        })
    }

    pub fn new_dir() -> Result<Arc<PathBuf>> {
        infer("ipsis_client_local_dir")
            .or_else(|e| {
                let mut dir = ::dirs::home_dir().ok_or(e)?;
                dir.push(".ipsis");
                Ok(dir)
            })
            .map(Into::into)
    }
}

#[async_trait]
impl<IpiisClient> Ipsis for IpsisClientInner<IpiisClient>
where
    IpiisClient: Ipiis + Send + Sync,
{
    type Reader = tokio::io::DuplexStream;

    async fn protocol(&self) -> Result<String> {
        Ok("local".into())
    }

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader> {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(self.ipiis.account_ref(), &path);

        // create a channel
        let (mut tx, rx) = tokio::io::duplex(CHUNK_SIZE.min(path.len.try_into()?));

        // external call
        tokio::spawn({
            let mut file = tokio::fs::File::open(path_canonical).await?;
            async move {
                tx.write_u64(path.len).await?;
                tokio::io::copy(&mut file, &mut tx).await
            }
        });

        // pack data
        Ok(rx)
    }

    async fn put_raw<R>(&self, path: &Path, mut data: R) -> Result<()>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(self.ipiis.account_ref(), &path);

        // create a directory
        tokio::fs::create_dir_all(path_canonical.ancestors().nth(1).unwrap()).await?;

        // create a channel
        let (mut tx, mut rx) = tokio::io::duplex(CHUNK_SIZE);

        // begin digesting a hash
        let handle_hash = tokio::spawn(async move {
            let mut chunk = Vec::with_capacity(CHUNK_SIZE.min(path.len.try_into()?));
            let mut hasher = Hasher::default();

            'pipe: loop {
                // clean up buffer
                chunk.clear();

                // read to buffer
                let chunk_size = CHUNK_SIZE as u64;
                let chunk_size = chunk_size.min(path.len - hasher.len() as u64);
                let mut take = (&mut data).take(chunk_size);
                take.read_to_end(&mut chunk).await?;

                let chunk_len = chunk.len();
                if chunk_len > 0 {
                    let ((), tx_result) =
                        tokio::join!(async { hasher.update(&chunk) }, tx.write_all(&chunk));
                    tx_result?;
                }

                let len = hasher.len() as u64;
                if len >= path.len || chunk_len == 0 {
                    break 'pipe Result::<_, ::ipis::core::anyhow::Error>::Ok(Path {
                        value: hasher.finalize(),
                        len,
                    });
                }
            }
        });

        // external call
        let mut file = tokio::fs::File::create(path_canonical).await?;
        tokio::io::copy(&mut rx, &mut file).await?;

        // poll hash
        let path_from_data = handle_hash.await??;

        // validate hash
        if path == path_from_data {
            Ok(())
        } else {
            // revert the request
            self.delete(&path_from_data).await?;

            // raise an error
            bail!("failed to validate the path")
        }
    }

    async fn contains(&self, path: &Path) -> Result<bool> {
        // get canonical path
        let path = self.to_path_canonical(self.ipiis.account_ref(), path);

        // external call
        Ok(tokio::fs::metadata(path).await.is_ok())
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // get canonical path
        let path = self.to_path_canonical(self.ipiis.account_ref(), path);

        // external call
        tokio::fs::remove_file(path).await.map_err(Into::into)
    }
}

impl<IpiisClient> IpsisClientInner<IpiisClient> {
    pub fn to_path_canonical(&self, account: &AccountRef, path: &Path) -> PathBuf {
        let mut buf = (*self.dir).clone();
        buf.push(account.to_string());
        buf.push(path.value.to_string());
        buf
    }
}

const CHUNK_SIZE: usize = 524_288;
