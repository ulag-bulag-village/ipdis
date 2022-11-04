use std::sync::Arc;

use ipiis_api::common::Ipiis;
use ipis::{
    async_trait::async_trait,
    core::{
        anyhow::{bail, Error, Result},
        value::hash::Hasher,
    },
    env::Infer,
    path::Path,
    tokio::{
        self,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    },
};
use ipsis_api_persistent_common::IpsisPersistentStorage;
use ipsis_common::Ipsis;

use crate::config::IpsisClientConfig;

pub type IpsisClient<PersistentStorage> =
    IpsisClientInner<::ipiis_api::client::IpiisClient, PersistentStorage>;

pub struct IpsisClientInner<IpiisClient, PersistentStorage> {
    pub ipiis: IpiisClient,
    config: IpsisClientConfig,
    persistent_storage: Arc<PersistentStorage>,
}

impl<IpiisClient, PersistentStorage> AsRef<::ipiis_api::client::IpiisClient>
    for IpsisClientInner<IpiisClient, PersistentStorage>
where
    IpiisClient: AsRef<::ipiis_api::client::IpiisClient>,
{
    fn as_ref(&self) -> &::ipiis_api::client::IpiisClient {
        self.ipiis.as_ref()
    }
}

impl<IpiisClient, PersistentStorage> AsRef<::ipiis_api::server::IpiisServer>
    for IpsisClientInner<IpiisClient, PersistentStorage>
where
    IpiisClient: AsRef<::ipiis_api::server::IpiisServer>,
{
    fn as_ref(&self) -> &::ipiis_api::server::IpiisServer {
        self.ipiis.as_ref()
    }
}

#[async_trait]
impl<'a, IpiisClient, PersistentStorage> Infer<'a>
    for IpsisClientInner<IpiisClient, PersistentStorage>
where
    Self: Send,
    IpiisClient: Infer<'a, GenesisResult = IpiisClient> + Send,
    <IpiisClient as Infer<'a>>::GenesisArgs: Sized,
    PersistentStorage: Infer<'a, GenesisResult = PersistentStorage>,
{
    type GenesisArgs = <IpiisClient as Infer<'a>>::GenesisArgs;
    type GenesisResult = Self;

    async fn try_infer() -> Result<Self> {
        Ok(Self {
            ipiis: IpiisClient::try_infer().await?,
            config: Default::default(),
            persistent_storage: PersistentStorage::try_infer().await?.into(),
        })
    }

    async fn genesis(
        args: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Ok(Self {
            ipiis: IpiisClient::genesis(args).await?,
            config: Default::default(),
            persistent_storage: PersistentStorage::try_infer().await?.into(),
        })
    }
}

#[async_trait]
impl<IpiisClient, PersistentStorage> Ipsis for IpsisClientInner<IpiisClient, PersistentStorage>
where
    IpiisClient: Ipiis + Send + Sync,
    PersistentStorage: IpsisPersistentStorage + Send + Sync + 'static,
{
    type Reader = tokio::io::DuplexStream;

    async fn protocol(&self) -> Result<String> {
        Ok(<PersistentStorage as IpsisPersistentStorage>::PROTOCOL.into())
    }

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader> {
        // create a channel
        let (mut tx, rx) = tokio::io::duplex(CHUNK_SIZE.min(path.len.try_into()?));

        // external call
        if !self.config.enable_get_next_hop || self.contains(&path).await? {
            // clone the arguments to send over the thread
            let account_ref = *self.ipiis.account_ref();
            let path = path.clone();
            let persistent_storage = self.persistent_storage.clone();

            tokio::spawn(async move {
                tx.write_u64(path.len).await?;
                persistent_storage
                    .get_raw(&account_ref, &path, &mut tx)
                    .await
            });
        } else {
            // traverse to next-hop
            let mut rx = self.ipiis.get_raw(&path).await?;
            tokio::spawn(async move { tokio::io::copy(&mut rx, &mut tx).await });
        }

        // pack data
        Ok(rx)
    }

    async fn put_raw<R>(&self, path: &Path, mut data: R) -> Result<()>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        let result = if <PersistentStorage as IpsisPersistentStorage>::USE_HASH_AS_NATIVE {
            // external call
            self.persistent_storage
                .put_raw(self.ipiis.account_ref(), path, &mut data.take(path.len))
                .await?
        } else {
            // create a channel
            let (mut tx, mut rx) = tokio::io::duplex(CHUNK_SIZE);

            // clone the arguments to send over the thread
            let total_len = path.len;

            // begin digesting a hash
            let handle_hash = tokio::spawn(async move {
                let mut chunk = Vec::with_capacity(CHUNK_SIZE.min(total_len.try_into()?));
                let mut hasher = Hasher::default();

                'pipe: loop {
                    // clean up buffer
                    chunk.clear();

                    // read to buffer
                    let chunk_size = CHUNK_SIZE as u64;
                    let chunk_size = chunk_size.min(total_len - hasher.len() as u64);
                    let mut take = (&mut data).take(chunk_size);
                    take.read_to_end(&mut chunk).await?;

                    let chunk_len = chunk.len();
                    if chunk_len > 0 {
                        let ((), tx_result) =
                            tokio::join!(async { hasher.update(&chunk) }, tx.write_all(&chunk));
                        tx_result?;
                    }

                    let len = hasher.len() as u64;
                    if len >= total_len || chunk_len == 0 {
                        break 'pipe Result::<_, Error>::Ok(Path {
                            value: hasher.finalize(),
                            len,
                        });
                    }
                }
            });

            // external call
            match self
                .persistent_storage
                .put_raw(self.ipiis.account_ref(), path, &mut rx)
                .await?
            {
                Ok(()) => {
                    // poll hash
                    let path_from_data = handle_hash.await??;

                    if path == &path_from_data {
                        Ok(())
                    } else {
                        Err(path_from_data)
                    }
                }
                Err(path_from_data) => Err(path_from_data),
            }
        };

        // validate hash
        match result {
            Ok(()) => Ok(()),
            Err(path_from_data) => {
                // revert the request
                self.delete(&path_from_data).await?;

                // raise an error
                bail!("failed to validate the path")
            }
        }
    }

    async fn contains(&self, path: &Path) -> Result<bool> {
        // external call
        self.persistent_storage
            .contains(self.ipiis.account_ref(), path)
            .await
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // external call
        self.persistent_storage
            .delete(self.ipiis.account_ref(), path)
            .await
    }
}

const CHUNK_SIZE: usize = 4_096;
