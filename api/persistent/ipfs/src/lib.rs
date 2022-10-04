use std::sync::Arc;

use async_compat::{Compat, CompatExt};
use http::uri::Scheme;
use ipfs_api::{IpfsApi, IpfsClient, TryFromUri};
use ipiis_api::common::Ipiis;
use ipis::{
    async_trait::async_trait,
    core::anyhow::{bail, Error, Result},
    env::{infer, Infer},
    futures::TryStreamExt,
    path::Path,
    tokio::{
        self,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream},
    },
};
use ipsis_common::Ipsis;

pub type IpsisClient = IpsisClientInner<::ipiis_api::client::IpiisClient>;

pub struct IpsisClientInner<IpiisClient> {
    pub ipiis: IpiisClient,
    ipfs: Arc<IpfsClient>,
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
            ipfs: Self::new_ipfs_entrypoint()?,
        })
    }

    pub fn new_ipfs_entrypoint() -> Result<Arc<IpfsClient>> {
        let host: String = infer("ipsis_client_ipfs_host").unwrap_or_else(|_| "localhost".into());
        let port = infer("ipsis_client_ipfs_port").unwrap_or(5001);

        IpfsClient::from_host_and_port(Scheme::HTTP, &host, port)
            .map(Into::into)
            .map_err(Into::into)
    }

    pub fn ipfs(&self) -> &Arc<IpfsClient> {
        &self.ipfs
    }
}

#[async_trait]
impl<IpiisClient> Ipsis for IpsisClientInner<IpiisClient>
where
    IpiisClient: Ipiis + Send + Sync,
{
    type Reader = tokio::io::DuplexStream;

    async fn protocol(&self) -> Result<String> {
        Ok("ipfs".into())
    }

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader> {
        // get canonical path
        let path = *path;

        // create a channel
        let (mut tx, rx) = tokio::io::duplex(CHUNK_SIZE.min(path.len.try_into()?));

        // external call
        tokio::spawn({
            let ipfs = self.ipfs.clone();
            async move {
                tx.write_u64(path.len).await?;

                let mut stream = ipfs.get(&path.value.to_string());
                let mut has_header = true;
                let mut len = 0;
                loop {
                    match stream.try_next().await {
                        Ok(Some(bytes)) => {
                            let mut bytes = if has_header {
                                has_header = false;

                                const HEADER_SIZE: usize = 512;
                                &bytes[HEADER_SIZE..]
                            } else {
                                &bytes
                            };

                            let num_bytes = bytes.len() as u64;
                            len += num_bytes;

                            if len > path.len {
                                let nullbytes = len - path.len;
                                bytes = &bytes[..(num_bytes - nullbytes) as usize];
                                len -= nullbytes;
                            }
                            tx.write_all_buf(&mut bytes).await?;

                            if len == path.len {
                                break Ok(());
                            }
                        }
                        Ok(None) => {
                            break Ok(());
                        }
                        Err(e) => break Err(Error::from(e)),
                    }
                }
            }
        });

        // pack data
        Ok(rx)
    }

    async fn put_raw<R>(&self, path: &Path, data: R) -> Result<()>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        // get canonical path
        let path = *path;

        // create a channel
        let (mut tx, mut rx) = tokio::io::duplex(CHUNK_SIZE);

        // impl Sync for R
        tokio::spawn(async move { tokio::io::copy(&mut data.take(path.len), &mut tx).await });

        // external call
        let mut rx: Compat<&mut DuplexStream> = unsafe { ::core::mem::transmute(rx.compat_mut()) };
        let rx: &mut Compat<&mut DuplexStream> = unsafe { ::core::mem::transmute(&mut rx) };
        let options = ipfs_api::request::Add::builder()
            .cid_version(1)
            .pin(true)
            .build();
        let response = self.ipfs.add_async_with_options(rx, options).await?;

        // poll hash
        let path_from_data = Path {
            value: response.hash.parse()?,
            len: path.len,
        };

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
        let path = *path;

        // external call
        let result = self.ipfs.pin_ls(Some(&path.value.to_string()), None).await;

        // pack data
        Ok(result.is_ok())
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // get canonical path
        let path = *path;

        // external call
        self.ipfs.pin_rm(&path.value.to_string(), true).await?;

        // pack data
        Ok(())
    }
}

const CHUNK_SIZE: usize = 524_288;
