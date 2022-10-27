use std::sync::Arc;

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
use s3::Bucket;

pub type IpsisClient = IpsisClientInner<::ipiis_api::client::IpiisClient>;

pub struct IpsisClientInner<IpiisClient> {
    pub ipiis: IpiisClient,
    bucket: Arc<Bucket>,
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
            bucket: Self::new_bucket()?,
        })
    }

    pub fn new_bucket() -> Result<Arc<Bucket>> {
        let bucket_name: String = infer("ipsis_client_s3_bucket_name")?;
        let region_name = infer("ipsis_client_s3_region_name")?;
        let region = match infer::<_, String>("ipsis_client_s3_region") {
            Ok(endpoint) => s3::Region::Custom {
                region: region_name,
                endpoint: match endpoint.find("://") {
                    Some(_) => endpoint,
                    None => {
                        let port = infer::<_, u16>("ipsis_client_s3_region_port").unwrap_or(80);
                        format!("http://{endpoint}:{port}")
                    }
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

        Ok(Bucket::new(&bucket_name, region, credentials)?
            .with_path_style()
            .into())
    }

    pub fn bucket(&self) -> &Arc<Bucket> {
        &self.bucket
    }
}

#[async_trait]
impl<IpiisClient> Ipsis for IpsisClientInner<IpiisClient>
where
    IpiisClient: Ipiis + Send + Sync,
{
    type Reader = tokio::io::DuplexStream;

    async fn protocol(&self) -> Result<String> {
        Ok("s3".into())
    }

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader> {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(self.ipiis.account_ref(), &path);

        // create a channel
        let (mut tx, rx) = tokio::io::duplex(CHUNK_SIZE.min(path.len.try_into()?));

        // external call
        tokio::spawn({
            let bucket = self.bucket.clone();
            async move {
                tx.write_u64(path.len).await?;
                bucket.get_object_stream(path_canonical, &mut tx).await
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
        let status_code = self
            .bucket
            .put_object_stream_parallel(&mut rx, &path_canonical)
            .await?;

        // validate response
        validate_http_status_code(status_code)?;

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
        let (_, status_code) = self.bucket.head_object(path).await?;

        // validate response
        if status_code != 404 {
            validate_http_status_code(status_code)?;
        }

        // pack data
        Ok(status_code == 200)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // get canonical path
        let path = self.to_path_canonical(self.ipiis.account_ref(), path);

        // // external call
        // let result = self.bucket.delete_object(path).await?;

        // // validate response
        // validate_http_status_code(result.status_code())?;

        // external call
        let (_, status_code) = self.bucket.delete_object(path).await?;

        // validate response
        validate_http_status_code(status_code)?;

        // pack data
        Ok(())
    }
}

impl<IpiisClient> IpsisClientInner<IpiisClient> {
    pub fn to_path_canonical(&self, account: &AccountRef, path: &Path) -> String {
        format!("{}/{}", account.to_string(), path.value.to_string())
    }
}

fn validate_http_status_code(status_code: u16) -> Result<()> {
    let status_code = ::http::StatusCode::from_u16(status_code)?;

    if status_code.is_success() {
        Ok(())
    } else {
        bail!("HTTP response was not successful: \"{status_code}\"")
    }
}

const CHUNK_SIZE: usize = 4_096;
