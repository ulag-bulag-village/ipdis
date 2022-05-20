use std::sync::Arc;

use ipiis_api::common::Ipiis;
use ipis::{
    async_trait::async_trait,
    core::{
        account::AccountRef,
        anyhow::{bail, Result},
        sha2::{Digest, Sha256},
        value::hash::Hash,
    },
    env::{infer, Infer},
    path::Path,
    tokio::{
        self,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    },
};
use ipsis_common::Ipsis;
use s3::Bucket;

pub type IpsisClient = IpsisClientInner<::ipiis_api::client::IpiisClient>;

pub struct IpsisClientInner<IpiisClient> {
    pub ipiis: IpiisClient,
    storage: Arc<Bucket>,
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
            storage: {
                let bucket_name: String = infer("ipsis_client_s3_bucket_name")?;
                let region_name = infer("ipsis_client_s3_region_name")?;
                let region = match infer::<_, String>("ipsis_client_s3_region") {
                    Ok(endpoint) => s3::Region::Custom {
                        region: region_name,
                        endpoint: match endpoint.find("://") {
                            Some(_) => endpoint,
                            None => format!("http://{}", endpoint),
                        },
                    },
                    Err(_) => region_name.parse()?,
                };
                let credentials = s3::creds::Credentials::from_env_specific(
                    Some("ipsis_client_s3_access_key"),
                    Some("ipsis_client_s3_secret_key"),
                    None,
                    None,
                )?;

                Bucket::new(&bucket_name, region, credentials)?
                    .with_path_style()
                    .into()
            },
        })
    }
}

#[async_trait]
impl<IpiisClient> Ipsis for IpsisClientInner<IpiisClient>
where
    IpiisClient: Ipiis + Send + Sync,
{
    type Reader = tokio::io::DuplexStream;

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader> {
        // get canonical path
        let path = *path;
        let path_canonical = to_path_canonical(self.ipiis.account_me().account_ref(), &path);

        // create a channel
        let (mut tx, rx) = tokio::io::duplex(CHUNK_SIZE);

        // external call
        tokio::spawn({
            let storage = self.storage.clone();
            async move {
                tx.write_u64(path.len).await?;
                storage.get_object_stream(path_canonical, &mut tx).await
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
        let path_canonical = to_path_canonical(self.ipiis.account_me().account_ref(), &path);

        // create a channel
        let (mut tx, mut rx) = tokio::io::duplex(CHUNK_SIZE);

        // begin digesting a hash
        let handle_hash = tokio::spawn(async move {
            let mut chunk = Vec::with_capacity(CHUNK_SIZE);
            let mut hasher = Sha256::new();
            let mut len: u64 = 0;

            'pipe: loop {
                let chunk_size = CHUNK_SIZE as u64;
                let chunk_size = chunk_size.min(path.len - len);
                let mut take = (&mut data).take(chunk_size);
                take.read_to_end(&mut chunk).await?;

                let chunk_len = chunk.len();
                if chunk_len > 0 {
                    len += chunk_size as u64;

                    let ((), tx_result) =
                        tokio::join!(async { hasher.update(&chunk) }, tx.write_all(&chunk));
                    tx_result?;
                }

                if len >= path.len || chunk_len == 0 {
                    break 'pipe Result::<_, ::ipis::core::anyhow::Error>::Ok(Path {
                        value: Hash(hasher.finalize()),
                        len,
                    });
                }
            }
        });

        // external call
        let status_code = self
            .storage
            .put_object_stream(&mut rx, &path_canonical)
            .await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

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
        let path = to_path_canonical(self.ipiis.account_me().account_ref(), path);

        // external call
        let (_, status_code) = self.storage.head_object(path).await?;

        // validate response
        if status_code != 404 {
            let () = validate_http_status_code(status_code)?;
        }

        // pack data
        Ok(status_code == 200)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // get canonical path
        let path = to_path_canonical(self.ipiis.account_me().account_ref(), path);

        // external call
        let (_, status_code) = self.storage.delete_object(path).await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

        // pack data
        Ok(())
    }
}

fn to_path_canonical(account: AccountRef, path: &Path) -> String {
    format!("{}/{}", account.to_string(), path.value.to_string())
}

fn validate_http_status_code(status_code: u16) -> Result<()> {
    let status_code = ::http::StatusCode::from_u16(status_code)?;

    if status_code.is_success() {
        Ok(())
    } else {
        bail!("HTTP response was not successful: \"{status_code}\"")
    }
}

const CHUNK_SIZE: usize = 524_288;
